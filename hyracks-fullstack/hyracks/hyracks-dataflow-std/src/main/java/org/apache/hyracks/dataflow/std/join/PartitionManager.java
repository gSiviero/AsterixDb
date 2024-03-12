/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.std.join;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;

public class PartitionManager {
    private final List<Partition> partitions = new ArrayList<Partition>();
    IFrame reloadBuffer;
    public BitSet spilledStatus;
    private IHyracksJobletContext context;

    /**
     * Buffer manager, responsable for allocating frames from a memory pool to partition buffers.<br>
     * It is shared among Build Partitions, Probe Partitions and Hash Table.
     */
    private static IPartitionedTupleBufferManager bufferManager;
    /**
     * Partition Couputer, often called the Split Function.<br>
     * This class hashes a tuple to a partition based on the number of partitions being used.
     */
    private final ITuplePartitionComputer partitionComputer;
    /**
     * Frame accessor, provides access to tuples in a byte buffer. It must be created with a record descriptor.
     */
    private final IFrameTupleAccessor tupleAccessor;

    public PartitionManager(int numberOfPartitions, IHyracksJobletContext context,
            IPartitionedTupleBufferManager bufferManager, ITuplePartitionComputer partitionComputer,
            IFrameTupleAccessor frameTupleAccessor, IFrameTupleAppender frameTupleAppender, BitSet spilledStatus,
            String relationName) throws HyracksDataException {
        this.context = context;
        this.bufferManager = bufferManager;
        this.tupleAccessor = frameTupleAccessor;
        this.partitionComputer = partitionComputer;
        reloadBuffer = new VSizeFrame(context);
        this.spilledStatus = spilledStatus;
        for (int i = 0; i < numberOfPartitions; i++) {
            partitions.add(new Partition(i, this.bufferManager, context, frameTupleAccessor, frameTupleAppender,
                    reloadBuffer, relationName,1));
        }
        if (numberOfPartitions*context.getInitialFrameSize() > bufferManager.getBufferPoolSize()) {
            throw new HyracksDataException(
                    "Number of Partitions can't be used. The memory budget in frames is smaller than the number of partitions");
        }
    }

    /**
     * Get the total number of tuples in memory at the time.
     *
     * @return Number of tuples
     */
    public int getTuplesInMemory() {
        return partitions.stream().mapToInt(Partition::getTuplesInMemory).sum();
    }

    /**
     * Get the total number of tuples processed that are either in memory or spilled
     * @return
     */
    public int getTuplesProcessed() {
        return partitions.stream().mapToInt(Partition::getTuplesProcessed).sum();
    }

    /**
     * Get total tuples spilled to Disk
     * @return
     */
    public int getTuplesSpilled() {
        return partitions.stream().mapToInt(Partition::getTuplesSpilled).sum();
    }
    
    /**
     * Get total number of bytes spilled,
     * @return
     */
    public long getBytesSpilled() {
        return partitions.stream().mapToLong(Partition::getBytesSpilled).sum();
    }
    /**
     * Get total number of bytes reloaded,
     * @return
     */
    public int getBytesReloaded() {
        return partitions.stream().mapToInt(Partition::getBytesReloaded).sum();
    }

    /**
     * Get Partition by Id
     * @param id Partition's id
     * @return IPartition
     */
    public IPartition getPartition(int id) {
        return getPartitionById(id);
    }
    public Partition getPartitionById(int id) {
        return partitions.stream().filter(p -> p.getId() == id).findFirst().get();
    }

    /**
     * Get number of tuples in memory from a specific partition
     *
     * @param partitionId
     * @return
     */
    public int getTuplesInMemory(int partitionId) {
        return partitions.get(partitionId).getTuplesInMemory();
    }

    /**
     * Get total memory used in <b>BYTES</b> by all partitions.
     *
     * @return
     */
    public double getTotalMemory() {
        return partitions.stream().mapToLong(Partition::getMemoryUsed).sum();
    }

    public long getTotalFrames() {
        return partitions.stream().mapToLong(Partition::getFramesUsed).sum();
    }

    /**
     * Insert tuple into a Partition, the partition that will hold this tuple is obtained using {@code partitionComputer}.
     *
     * @param tupleId Tuple id
     * @return <b>TRUE</b>True if was successfully inserted or <b>FALSE</b> if there is no more frames available in the Memory Pool.
     * @throws HyracksDataException Exception
     */
    public boolean insertTuple(int tupleId) throws HyracksDataException {
        int partitionId = partitionComputer.partition(tupleAccessor, tupleId, getNumberOfPartitions());
        return partitions.get(partitionId).insertTuple(tupleId);
    }

    /**
     * <p>This method insert a tuple into a partition managed by this class, selecting the partition based on the {@code partitionComputer}.</p>
     * <p>If this partition's buffer is full, and there are no buffers available in the {@code bufferManager}'s Pool than a partition will be selected to be spilled to disk.</p>
     * <p>This partition will be seleceted based on the {@code spillPolicy}</p>
     *
     * @param tupleId     tuple id to be inserted.
     * @param spillPolicy spill policy that will determine witch partition will be spilled in the case that are no buffers available.
     * @throws HyracksDataException Exception
     */
    public void insertTupleWithSpillPolicy(int tupleId, PreferToSpillFullyOccupiedFramePolicy spillPolicy)
            throws HyracksDataException {
        int partitionId = partitionComputer.partition(tupleAccessor, tupleId, getNumberOfPartitions());

        while (!partitions.get(partitionId).insertTuple(tupleId)) {
            spillPartition(spillPolicy.selectVictimPartition(partitionId));
        }
    }

    /**
     * Get number of partitions in the partition manager.
     *
     * @return Number of Partitions
     */
    public int getNumberOfPartitions() {
        return partitions.size();
    }

    /**
     * Internal method that filter the spilled partitions.
     *
     * @return List of Partitions
     */
    public List<Partition> getSpilledPartitions() {
        return partitions.stream().filter(Partition::getSpilledStatus).collect(Collectors.toList());
    }
    public List<Partition> getSpilledOrInconsistentPartitions() {
        return partitions.stream().filter(p -> p.getReloadedStatus() || p.getSpilledStatus())
                .collect(Collectors.toList());
    }

    /**
     * Internal method that filter the spilled partitions.
     *
     * @return List of Partitions
     */
    public List<Partition> getMemoryResidentPartitions() {
        return partitions.stream().filter(p -> !p.getSpilledStatus()).collect(Collectors.toList());
    }

    /**
     * Spill a partition to Disk
     *
     * @param id id of partition to be spilled
     * @return Number of Frames Released
     * @throws HyracksDataException Exception
     */
    public int spillPartition(int id) throws HyracksDataException {
        Partition partition = getPartitionById(id);
        int oldMemoryUsage = partition.getMemoryUsed();
        partition.spill();
//        spilledStatus.set(id);
        return (oldMemoryUsage - partition.getMemoryUsed()) / context.getInitialFrameSize();
    }

    //region [FIND PARTITION]

    /**
     * Returns a list of partitions that are candidates to be spilled. <br>
     * A Partition is a candidate if:
     * <ul>
     *     <li>Its buffer is larger than one FRAME</li>
     * </ul>
     * Candidate partitions are returned in order of:
     * <ul>
     *     <li>Spilled first</li>
     *     <li>Than Larger Buffer first</li>
     *     <li>Than larger number of tuples first</li>
     * </ul>
     * @return
     */
    private List<Partition> getSpillCandidatePartitions() {
        List<Partition> candidates =
                partitions.stream().filter(p -> p.getFramesUsed() > 1).collect(Collectors.toList());
        PartitionComparatorBuilder comparator = new PartitionComparatorBuilder();
        comparator.addStatusComparator(true);
        comparator.addBufferSizeComparator(true);
        comparator.addInMemoryTupleComparator(true);
        candidates.sort(comparator.build());
        return candidates;
    }

    /**
     * Spill Partitions until release {@code numberOfFrames}
     * <p>This method stops spilling partitions if the number of frames is reached. Otherwise keeps spilling candidates.</p>
     * @param numberOfFrames Frames to release
     * @return Number of frames Released
     * @throws HyracksDataException
     */
    public int spillToReleaseFrames(int numberOfFrames) throws HyracksDataException {
        int framesReleased = 0;
        List<Partition> candidates = getSpillCandidatePartitions();
        for (Partition p : candidates) {
            framesReleased += spillPartition(p.getId());
            if (framesReleased >= numberOfFrames) {
                break;
            }
        }
        return framesReleased;
    }

    /**
     * Get id of Spilled Partition with Larger Buffer:
     * <ul>
     *     <li>If two or mor partitions have the same buffer size (frames) than return the one with more tuples in memory</li>
     *     <li>If there is a tie return the lowest id</li>
     * </ul>
     *
     * @return Partition id
     */
    public int getNumberOfTuplesOfLargestPartition() {
        Comparator<Partition> comparator = Comparator.comparing(Partition::getTuplesProcessed).reversed();
        return Collections.max(partitions, comparator).getTuplesProcessed();
    }
    //endregion

    /**
     * Close all spilled partitions, spilling all tuples that are spill in memory.s
     *
     * @throws HyracksDataException Exception
     */
    public void closeSpilledPartitions() throws HyracksDataException {
        List<Partition> spilledPartitions = getSpilledPartitions();
        for (Partition p : spilledPartitions) {
            if (p.getTuplesProcessed() > 0)
                p.close();
        }
    }

    /**
     * Close all spilled partitions, spilling all tuples that are spill in memory.s
     *
     * @throws HyracksDataException Exception
     */
    public void flushSpilledPartitions() throws HyracksDataException {
        List<Partition> spilledPartitions = getSpilledPartitions();
        for (Partition p : spilledPartitions) {
            if (p.getTuplesProcessed() > 0)
                p.spill();
        }
    }

    /**
     * Reload partition from disk into memory
     *
     * @param id id of Partition to be reloaded.
     * @return <b>TRUE</b> if reload was successfull.
     * @throws HyracksDataException Exception
     */
    public boolean reloadPartition(int id, boolean deleteAfterReload, int framesAvailable) throws HyracksDataException {
        if (getPartitionById(id).reload(deleteAfterReload,framesAvailable)) {
            spilledStatus.clear(id);
            return true;
        }
        return false;
    }

    /**
     * Check if all partitions are memory resident.
     *
     * @return <b>TRUE</b> if no partitions were spilled and <b>FALSE</b> if one or more are spilled.
     */
    public boolean areAllPartitionsMemoryResident() {
        return getSpilledStatus().cardinality() == 0;
    }

    /**
     * Return a Bit Set containing the spilled status.<br>
     * This bit set is generated by the Partition's status, only updatable by the Partition itself. <br>
     * This is a single source of truth.
     *
     * @return
     */
    public BitSet getSpilledStatus() {
        BitSet status = new BitSet(getNumberOfPartitions());
        status.clear();
        for (Partition p : partitions) {
            status.set(p.getId(), p.getSpilledStatus());
        }
        return status;
    }

    /**
     * Return a Bit Set containing the spilled status.<br>
     * This bit set is generated by the Partition's status, only updatable by the Partition itself. <br>
     * This is a single source of truth.
     *
     * @return
     */
    public BitSet getInconsistentStatus() {
        BitSet inconsistent = new BitSet(getNumberOfPartitions());
        for (Partition p : partitions) {
            inconsistent.set(p.getId(), p.getSpilledStatus() || p.getReloadedStatus());
        }
        return inconsistent;
    }

    /**
     * Close all Partitions and their file accessors.
     *
     * @throws HyracksDataException Exception
     */
    public void cleanUp() throws HyracksDataException {
        for (Partition partition : partitions) {
            partition.cleanUp();
        }
    }
}
