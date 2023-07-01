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

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class mainly applies one level of HHJ on a pair of
 * relations. It is always called by the descriptor.
 */
public class HybridHashJoin implements IHybridHashJoin {

    private static final Logger LOGGER = LogManager.getLogger();
    // Used for special probe BigObject which can not be held into the Join memory
    private FrameTupleAppender bigFrameAppender;
    protected boolean dynamicMemory = false;
    protected final IHyracksJobletContext jobletCtx;
    private final String buildRelName;
    private final String probeRelName;
    protected final ITuplePartitionComputer buildHpc;
    protected final ITuplePartitionComputer probeHpc;
    protected final RecordDescriptor buildRd;
    protected final RecordDescriptor probeRd;
    private final IPredicateEvaluator buildPredEval;
    private final IPredicateEvaluator probePredEval;
    protected final boolean isLeftOuter;
    protected final IMissingWriter[] nonMatchWriters;
    private final BitSet spilledStatus; //0=resident, 1=spilled
    private final BitSet probeSpilledStatus; //0=resident, 1=spilled
    protected final int numOfPartitions;
    protected int memSizeInFrames;
    protected InMemoryHashJoin inMemJoiner; //Used for joining resident partitions
    protected IPartitionedTupleBufferManager bufferManager;
    protected IPartitionedTupleBufferManager bufferManagerProbe;
    private PreferToSpillFullyOccupiedFramePolicy spillPolicy;
    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;
    protected ISimpleFrameBufferManager bufferManagerForHashTable;
    // Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal.
    protected boolean isReversed;
    protected PartitionManager buildPartitionManager;
    protected PartitionManager probePartitionManager;
    IDeallocatableFramePool framePool;
    // this is a reusable object to store the pointer,which is not used anywhere. we mainly use it to match the
    // corresponding function signature.
    private IOperatorStats stats = null;
    ISerializableTable table;
    protected boolean deleteAfterReload = true;

    public HybridHashJoin(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions,
                          String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd,
                          ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval,
                          IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
        this.jobletCtx = jobletCtx;
        this.memSizeInFrames = memSizeInFrames;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;
        this.buildRelName = buildRelName;
        this.probeRelName = probeRelName;
        this.numOfPartitions = numOfPartitions;
        this.accessorBuild = new FrameTupleAccessor(buildRd);
        this.accessorProbe = new FrameTupleAccessor(probeRd);
        this.isLeftOuter = isLeftOuter;
        if (isLeftOuter && probePredEval != null) {
            throw new IllegalStateException();
        }
        this.buildPredEval = buildPredEval;
        this.probePredEval = probePredEval;
        this.isReversed = false;
        this.spilledStatus = new BitSet(numOfPartitions);
        this.probeSpilledStatus = new BitSet(numOfPartitions);
        this.nonMatchWriters = isLeftOuter ? new IMissingWriter[nullWriterFactories1.length] : null;
        if (isLeftOuter) {
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nonMatchWriters[i] = nullWriterFactories1[i].createMissingWriter();
            }
        }
    }

    @Override
    public void initBuild() throws HyracksDataException {
        framePool =
                new DeallocatableFramePool(jobletCtx, memSizeInFrames * jobletCtx.getInitialFrameSize(), dynamicMemory);
        bufferManagerForHashTable = new FramePoolBackedFrameBufferManager(framePool);
        bufferManager = new VPartitionTupleBufferManager(
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus),
                numOfPartitions, framePool);

        spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(bufferManager, spilledStatus);
        buildPartitionManager = new PartitionManager(numOfPartitions, jobletCtx, bufferManager, buildHpc, accessorBuild,
                bigFrameAppender, spilledStatus, buildRelName);
    }

    @Override
    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tupleCount = accessorBuild.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            if (buildPredEval == null || buildPredEval.evaluate(accessorBuild, i)) {
                buildPartitionManager.insertTupleWithSpillPolicy(i, spillPolicy);
            }
        }
    }

    @Override
    public void closeBuild() throws HyracksDataException {
        try {
            // Flushes the remaining chunks of the all spilled partitions to the disk.
            buildPartitionManager.closeSpilledPartitions();
            // Makes the space for the in-memory hash table (some partitions may need to be spilled to the disk
            // during this step in order to make the space.)
            // and tries to bring back as many spilled partitions as possible if there is free space.
            spillAndReloadPartitions();
            buildHashTable();
        } catch (Exception ex) {
            throw new HyracksDataException("Erro aqui FDP");
        }
    }

    @Override
    public void clearBuildTempFiles() throws HyracksDataException {
        buildPartitionManager.cleanUp();
    }

    @Override
    public void clearProbeTempFiles() throws HyracksDataException {
        probePartitionManager.cleanUp();
    }

    @Override
    public void fail() throws HyracksDataException {
        buildPartitionManager.cleanUp();
        if (probePartitionManager != null)
            probePartitionManager.cleanUp();
    }

    /**
     * Return the amount of Free Bytes.
     *
     * @return
     */
    protected long calculateFreeSpace() {
        long freeSpace = this.memSizeInFrames;
        if (buildPartitionManager != null) {
            freeSpace -= buildPartitionManager.getTotalFrames();
        }
        if (probePartitionManager != null) {
            freeSpace -= probePartitionManager.getTotalFrames();
        }
        if (table != null) {
            freeSpace -= SerializableHashTable.getExpectedTableFrameCount(buildPartitionManager.getTuplesInMemory(),jobletCtx.getInitialFrameSize());
        }
        return freeSpace;
    }

    /**
     * Return the number of bytes that will be available in the framePool if a certain partition {@code p} is spilled.
     *
     * @param p
     * @return Number of Bytes
     */
    private long calculateSpaceAfterSpillBuildPartition(Partition p) {
        int frameSize = jobletCtx.getInitialFrameSize();
        long spaceAfterSpill = calculateFreeSpace() + p.getMemoryUsed();
        spaceAfterSpill -= SerializableHashTable.calculateByteSizeDeltaForTableSizeChange(
                buildPartitionManager.getTuplesInMemory(), -p.getTuplesInMemory(), frameSize);
        return spaceAfterSpill;
    }

    protected void spillAndReloadPartitions() throws HyracksDataException {
        // Spill some partitions if there is no free space.
        long tableSize =SerializableHashTable.getExpectedTableByteSize(buildPartitionManager.getTuplesInMemory(),
                jobletCtx.getInitialFrameSize());
        while (calculateFreeSpace() < tableSize) {
            IPartition p = selectSinglePartitionToSpill();
            if (p == null) {
                break;
            }
            buildPartitionManager.spillPartition(p.getId());
            tableSize =SerializableHashTable.getExpectedTableByteSize(buildPartitionManager.getTuplesInMemory(),
                    jobletCtx.getInitialFrameSize());
        }
        bringPartitionsBack(false);
        // Bring some partitions back in if there is enough space.
    }

    /**
     * Brings back some partitions if there is free memory and partitions that fit in that space.
     *
     * @return number of in memory tuples after bringing some (or none) partitions in memory.
     * @throws HyracksDataException
     */
    protected void bringPartitionsBack(boolean print) throws HyracksDataException {
        int frameSize = jobletCtx.getInitialFrameSize();
        int framesAvailable = (int) calculateFreeSpace()/frameSize;
        // Add one frame to freeSpace to consider the one frame reserved for the spilled partition
        if (framesAvailable > 0) {
            for (Partition p : buildPartitionManager.getSpilledPartitions()) {
                // Expected hash table size increase after reloading this partition
                long expectedHashTableByteSizeIncrease = SerializableHashTable.calculateByteSizeDeltaForTableSizeChange(
                        buildPartitionManager.getTuplesInMemory(), p.getTuplesProcessed(), frameSize);
                if (framesAvailable >= p.getFileSize() + expectedHashTableByteSizeIncrease) {
                    if (print) {
                        LOGGER.info(String.format("Reloading Partition %d", p.getId()));
                    }
                    buildPartitionManager.reloadPartition(p.getId(), this.deleteAfterReload,framesAvailable);
                    buildHashTable();
                }
            }
        }
    }

    /**
     * Finds a best-fit partition that will be spilled to the disk to make enough space to accommodate the hash table.
     *
     * @return the partition id that will be spilled to the disk. Returns -1 if there is no single suitable partition.
     */
    private IPartition selectSinglePartitionToSpill() {
        int frameSize = jobletCtx.getInitialFrameSize();
        long minSpaceAfterSpill = (long) memSizeInFrames * frameSize;
        Partition minSpaceAfterSpillPartID = null, nextAvailablePidToSpill = null;
        for (Partition partition : buildPartitionManager.getMemoryResidentPartitions()) {
            if (partition.getTuplesProcessed() == 0 || partition.getMemoryUsed() == 0) {
                continue;
            } else if (nextAvailablePidToSpill == null) {
                nextAvailablePidToSpill = partition;
            }
            long spaceAfterSpill = calculateSpaceAfterSpillBuildPartition(partition);
            if (spaceAfterSpill == 0) {
                // Found the perfect one. Just returns this partition.
                return partition;
            } else if (spaceAfterSpill > 0 && spaceAfterSpill < minSpaceAfterSpill) {
                // We want to find the best-fit partition to avoid many partition spills.
                minSpaceAfterSpill = spaceAfterSpill;
                minSpaceAfterSpillPartID = partition;
            }
        }

        return minSpaceAfterSpillPartID != null ? minSpaceAfterSpillPartID : nextAvailablePidToSpill;
    }

    protected void buildHashTable() throws HyracksDataException {
        try {
            table = new SerializableHashTable(buildPartitionManager.getTuplesInMemory(), jobletCtx,
                    bufferManagerForHashTable);
            if (this.inMemJoiner == null) {
                this.inMemJoiner = new InMemoryHashJoin(jobletCtx, new FrameTupleAccessor(probeRd), probeHpc,
                        new FrameTupleAccessor(buildRd), buildRd, buildHpc, isLeftOuter, nonMatchWriters, table, isReversed,
                        bufferManagerForHashTable);
            } else {
                inMemJoiner.releaseMemory();
                inMemJoiner.setTable(table);
            }
            for (Partition p : buildPartitionManager.getMemoryResidentPartitions()) {
                buildHashTableForPartition(p.getId());
            }
        } catch (Exception ex) {
            LOGGER.info("Error Building Hash Table");
        }
    }

    protected void buildHashTableForPartition(int pid) throws HyracksDataException {
        bufferManager.flushPartition(pid, new IFrameWriter() {
            @Override
            public void open() {
                // Only nextFrame method is needed to pass the frame to the next operator.
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                inMemJoiner.build(buffer);
            }

            @Override
            public void fail() {
                // Only nextFrame method is needed to pass the frame to the next operator.
            }

            @Override
            public void close() {
                // Only nextFrame method is needed to pass the frame to the next operator.
            }
        });
    }

    /**
     * @param comparator Comparator that determines if tuples match.
     * @throws HyracksDataException
     */
    @Override
    public void initProbe(ITuplePairComparator comparator) throws HyracksDataException {
        bufferManagerForHashTable = new FramePoolBackedFrameBufferManager(framePool);
        bufferManagerProbe = new VPartitionTupleBufferManager(
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus),
                numOfPartitions, framePool);
        probePartitionManager = new PartitionManager(numOfPartitions, jobletCtx, bufferManagerProbe, probeHpc,
                accessorProbe, bigFrameAppender, probeSpilledStatus, probeRelName);
        if (inMemJoiner == null)
            throw new HyracksDataException("Build must be closed");
        inMemJoiner.setComparator(comparator);
        bufferManager.setConstrain(VPartitionTupleBufferManager.NO_CONSTRAIN);
    }

    @Override
    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();
        inMemJoiner.resetAccessorProbe(accessorProbe);
        int partitionsTuples[] = new int[numOfPartitions];
        if (buildPartitionManager.areAllPartitionsMemoryResident()) {
            for (int i = 0; i < tupleCount; ++i) {
                // NOTE: probePredEval is guaranteed to be 'null' for outer join and in case of role reversal
                if (probePredEval == null || probePredEval.evaluate(accessorProbe, i)) {
                    try {
                        inMemJoiner.join(i, writer);
                    } catch (Exception ex) {
                        throw new HyracksDataException("Error Probing");
                    }
                }
            }
        } else {
            for (int i = 0; i < tupleCount; ++i) {
                // NOTE: probePredEval is guaranteed to be 'null' for outer join and in case of role reversal
                if (probePredEval == null || probePredEval.evaluate(accessorProbe, i)) {
                    int pid = probeHpc.partition(accessorProbe, i, numOfPartitions);
                    IPartition partition = buildPartitionManager.getPartition(pid);
                    if (partition.getTuplesProcessed() > 0 || isLeftOuter) { //Tuple has potential match from previous phase
                        if (partition.getSpilledStatus()) { //pid is Spilled
                            processTupleProbePhase(i, pid);
                        } else { //pid is Resident
                            inMemJoiner.join(i, writer);
                        }
                    }
                }
            }
        }
    }

    /**
     * Process tuple during PROBE phase, inserting the tuple into a partition.
     *
     * @param tupleId TupleId inside the tupple acessor.
     * @param pid     Parition Id.
     * @throws HyracksDataException
     */
    private void processTupleProbePhase(int tupleId, int pid) throws HyracksDataException {
        if (!probePartitionManager.insertTuple(tupleId)) {
            int recordSize =
                    VPartitionTupleBufferManager.calculateActualSize(null, accessorProbe.getTupleLength(tupleId));
            // If the partition is at least half-full and insertion fails, that partition is preferred to get
            // spilled, otherwise the biggest partition gets chosen as the victim.
            boolean modestCase = recordSize <= (jobletCtx.getInitialFrameSize() / 2);
            IPartition partition = probePartitionManager.getPartition(pid);
            int victim = (modestCase && partition.getTuplesInMemory() > 0) ? pid
                    : spillPolicy.findSpilledPartitionWithMaxMemoryUsage();
            // This method is called for the spilled partitions, so we know that this tuple is going to get written to
            // disk, sooner or later. As such, we try to reduce the number of writes that happens. So if the record is
            // larger than the size of the victim partition, we just flush it to the disk, otherwise we spill the
            // victim and this time insertion should be successful.
            //TODO:(More optimization) There might be a case where the leftover memory in the last frame of the
            // current partition + free memory in buffer manager + memory from the victim would be sufficient to hold
            // the record.
            if (victim >= 0 && probePartitionManager.getPartition(victim).getMemoryUsed() >= recordSize) {
                probePartitionManager.spillPartition(victim);
            }
            probePartitionManager.insertTuple(tupleId);
        }
    }

    /**
     * Complete the probe Phase
     * <ul>
     *     <li>Spill partitions that have tuples in Memory</li>
     *     <li>Complete the In Memory Join</li>
     *     <li>Updates the Operator Stats</li>
     * </ul>
     *
     * @param writer
     * @throws HyracksDataException
     */
    @Override
    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
        //We do NOT join the spilled partitions here, that decision is made at the descriptor level
        //(which join technique to use)
        for (Partition p : buildPartitionManager.getSpilledOrInconsistentPartitions()) {
            if (probePartitionManager.getTuplesInMemory(p.getId()) > 0) {
                buildPartitionManager.spillPartition(p.getId());
                probePartitionManager.spillPartition(p.getId());
            }
        }
        inMemJoiner.completeJoin(writer);
    }

    /**
     * Release resources
     * <ul>
     *     <li>Close Hash Table</li>
     *     <li>Close Spilled Partitions</li>
     *     <li>Close buffer manager</li>
     *     <li>Close Hash Table's buffer manager</li>
     *     <li>Erase In Memory Joiner</li>
     * </ul>
     *
     * @throws HyracksDataException
     */
    @Override
    public void releaseResource() throws HyracksDataException {
        inMemJoiner.closeTable();
        probePartitionManager.closeSpilledPartitions();
        bufferManager.close();
        inMemJoiner = null;
        bufferManager = null;
        bufferManagerForHashTable = null;
    }

    /**
     * Return File reader for temporary file related to one of the <b>BUILD</b> partition.
     *
     * @param pid Partition's Id
     * @return File Reader
     * @throws HyracksDataException
     */
    @Override
    public RunFileReader getBuildRFReader(int pid) throws HyracksDataException {
        return buildPartitionManager.getPartition(pid).getFileReader();
    }

    /**
     * Return File reader for temporary file related to one of the <b>PROBE</b> partition.
     *
     * @param pid Partition's Id
     * @return File Reader
     * @throws HyracksDataException
     */
    @Override
    public RunFileReader getProbeRFReader(int pid) throws HyracksDataException {
        return probePartitionManager.getPartition(pid).getFileReader();
    }

    /**
     * Get number of tuples in a <b>BUILD</b> partition.
     *
     * @param pid Partition's Id
     * @return
     */
    @Override
    public int getBuildPartitionSizeInTup(int pid) {
        return buildPartitionManager.getPartition(pid).getTuplesProcessed();
    }

    /**
     * Get number of tuples in a <b>PROBE</b> partition.
     *
     * @param pid Partition's Id
     * @return
     */
    @Override
    public int getProbePartitionSizeInTup(int pid) {
        return probePartitionManager.getPartition(pid).getTuplesProcessed();
    }

    /**
     * Get the number of tuples in the largest <b>BUILD</b> partition.
     *
     * @return Number of Tuples
     */
    @Override
    public int getMaxBuildPartitionSize() {
        return buildPartitionManager.getNumberOfTuplesOfLargestPartition();
    }

    /**
     * Get the number of tuples in the largest <b>PROBE</b> partition.
     *
     * @return Number of Tuples
     */
    @Override
    public int getMaxProbePartitionSize() {
        return probePartitionManager.getNumberOfTuplesOfLargestPartition();
    }

    /**
     * Get bitset representing the spilled status of the <b>BUILD</b> partitions
     *
     * @return Bitset
     */
    @Override
    public BitSet getPartitionStatus() {
        return buildPartitionManager.getSpilledStatus();
    }

    /**
     * Get bitset representing the inconsistent status of the <b>BUILD</b> partitions.
     * <p>A partition is called inconsistent if at some point it was spilled or reloaded.
     * This is usefully during probe phase with dynamic memory</p>
     *
     * @return Bitset
     */
    @Override
    public BitSet getInconsistentStatus() {
        BitSet inconsistent = buildPartitionManager.getInconsistentStatus();
        inconsistent.or(probePartitionManager.getSpilledStatus());
        return inconsistent;
    }

    /**
     * Get Partition size in <b>FRAMES</b>
     *
     * @param pid
     * @return
     */
    @Override
    public int getPartitionSize(int pid) {
        return bufferManager.getPhysicalSize(pid);
    }

    @Override
    public void setIsReversed(boolean reversed) {
        if (reversed && (buildPredEval != null || probePredEval != null)) {
            throw new IllegalStateException();
        }
        this.isReversed = reversed;
    }

    @Override
    public void setOperatorStats(IOperatorStats stats) {

        this.stats = stats;
    }

    @Override
    public int updateMemoryBudgetBuild(int newBudget) throws HyracksDataException {
        return 0;
        //Nothing to Do here
    }

    @Override
    public int updateMemoryBudgetProbe(int newBudget) throws HyracksDataException {
        return 0;
        //Do Nothing
    }

    @Override
    public long getBuildFramesInMemory() {
        return buildPartitionManager.getTotalFrames();
    }

}
