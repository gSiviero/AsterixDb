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
import java.util.Random;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePoolDynamicBudget;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;

public class MemoryContentionResponsiveHHJ extends OptimizedHybridHashJoin {
    /**Frame Count**/
    private int processedFrames = 0;
    /**@todo: This is used to set margins for the memory update, it should not go to production code.**/
    private int originalBudget;
    private Random random;
    ITuplePairComparator comparator;
    BitSet inconsistentStatus;

    //region Knobs
    /**
     * Interval of frames to call the memory update methods
     **/
    private int frameInterval = 10;
    /**
     * If set as true a memory update will happen before spilling a partition during Build Phase
     **/
    private boolean eventBased = true;
    /**
     * Enables memory expansion during Build phase
     **/
    private boolean memoryExpansionBuild = true;
    /**
     * Enables memory contention during Build phase
     **/
    private boolean memoryContentionBuild = true;
    /**
     * Enables memory expansion during Probe phase
     **/
    private boolean memoryExpansionProbe = true;
    /**
     * Enables memory expansion during Probe phase
     **/
    private boolean memoryContentionProbe = true;
    /**
     * Enables probing inconsistent partitons before completing the build phase
     **/
    private boolean probeInconsistentThisRound = true;

    //endregion
    public MemoryContentionResponsiveHHJ(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions,
                                         String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd,
                                         ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval,
                                         IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
        super(jobletCtx, memSizeInFrames, numOfPartitions, probeRelName, buildRelName, probeRd, buildRd, probeHpc,
                buildHpc, probePredEval, buildPredEval, isLeftOuter, nullWriterFactories1);
        originalBudget = memSizeInFrames;
        inconsistentStatus = new BitSet(numOfPartitions);
        random = new Random(-933090634); //This random seed is generating the Hash Table insertion Failure for my test query.
    }

    //region BUILD
    public void initBuild() throws HyracksDataException {
        IDeallocatableFramePool framePool =
                new DeallocatableFramePoolDynamicBudget(jobletCtx, memSizeInFrames * jobletCtx.getInitialFrameSize());
        initBuildInternal(framePool);
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        if (frameInterval > 0 && processedFrames % frameInterval == 0) {
            updateMemoryBudgetBuildPhase();
        }
        super.build(buffer);
        processedFrames++;
    }

    public void closeBuild() throws HyracksDataException {
        try {
            super.closeBuild();
        } catch (Exception ex) {
            this.fail();
        }
    }

    /**
     * Updates Memory Budget During Build Phase
     */
    private void updateMemoryBudgetBuildPhase() throws HyracksDataException {
        int newBudgetInFrames = random.nextInt(originalBudget) + this.originalBudget;
        if (newBudgetInFrames >= memSizeInFrames && memoryExpansionBuild) {
            bufferManager.updateMemoryBudget(newBudgetInFrames);
            memSizeInFrames = newBudgetInFrames;
        } else if (memoryContentionBuild) {
            while (!bufferManager.updateMemoryBudget(newBudgetInFrames)) {
                int victimPartition = spillPolicy.findSpilledPartitionWithMaxMemoryUsage();
                if (victimPartition < 0) {
                    victimPartition = spillPolicy.findInMemPartitionWithMaxMemoryUsage();
                }
                if (victimPartition < 0) {
                    break;
                }
                int framesToRelease = bufferManager.getPhysicalSize(victimPartition) / jobletCtx.getInitialFrameSize();
                spillPartition(victimPartition);
                memSizeInFrames -= framesToRelease;
                memSizeInFrames = memSizeInFrames <= newBudgetInFrames ? newBudgetInFrames : memSizeInFrames;
            }
        }
    }

    /**
     * Override the original method, if the knobe ("eventBased") is set as true the memory budget update is called in case of tuple insertion failed.
     *
     * @param tid Tuple Id
     * @param pid Partition Id
     * @throws HyracksDataException
     */
    protected void processTupleBuildPhase(int tid, int pid) throws HyracksDataException {
        //If event Based Memory adaption is on and the first insert try fails updates the memory budget.
        if (eventBased) {
            if (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
                updateMemoryBudgetBuildPhase();
                super.processTupleBuildPhaseInternal(tid, pid);
            }
        } else {
            super.processTupleBuildPhaseInternal(tid, pid);
        }
    }
    //endregion

    //region PROBE
    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        if (frameInterval > 0 && processedFrames % frameInterval == 0) {
            updateMemoryBudgetProbePhase();
        }
        super.probe(buffer, writer);
        processedFrames++;
    }

    private void updateMemoryBudgetProbePhase() throws HyracksDataException {
        int newBudgetInFrames = random.nextInt(originalBudget) + this.originalBudget;
        if (newBudgetInFrames > memSizeInFrames && memoryExpansionProbe) { //Memory Expansion Scenario
            if (bufferManager.updateMemoryBudget(newBudgetInFrames)) {
                memSizeInFrames = newBudgetInFrames;
            }
            memoryExpansionProbe();
        } else if (memoryContentionProbe) {
            memoryContentionPorbe(newBudgetInFrames);
        }
    }

    /**
     * Spill memory resident partition to disk during probe.
     *
     * @param newBudgetInFrames
     * @throws HyracksDataException
     */
    private void memoryContentionPorbe(int newBudgetInFrames) throws HyracksDataException {
        while (!bufferManager.updateMemoryBudget(newBudgetInFrames)) {
            int victimPartition = spillPolicy.findInMemPartitionWithMaxMemoryUsage();
            if (victimPartition < 0) {
                break;
            }
            int framesToRelease = bufferManager.getPhysicalSize(victimPartition) / jobletCtx.getInitialFrameSize();
            spillPartition(victimPartition);
            memSizeInFrames -= framesToRelease;
            memSizeInFrames = memSizeInFrames <= newBudgetInFrames ? newBudgetInFrames : memSizeInFrames;
        }
    }

    /**
     * Reload spilled partitions into memory and then call the to rebuild the Hash Table.
     * <ul>
     *     <li>Select a partition to Reload (this can be optimized)</li>
     *     <li>Spill buffer frames used as output frames for the Probe Partition to disk</li>
     *     <li>Clears the Partition's buffer</li>
     *     <li>Reload build partition's tuples into memory./<li>
     *     <li>Rebuild Hash Table</li>
     * </ul>
     *
     * @throws HyracksDataException
     */
    private void memoryExpansionProbe() throws HyracksDataException {
        freeSpace fs = calculateFreeSpace();
        boolean shouldRebuildHashTable = false;
        int partitionToReload = selectAPartitionToReloadProbe(fs.freeSpace, fs.tuplesInMemory);
        while (partitionToReload >= 0) {
            long hashTableSizeAfter = SerializableHashTable.getExpectedTableByteSize(
                    fs.tuplesInMemory + getBuildPartitionSizeInTup(partitionToReload), jobletCtx.getInitialFrameSize());
            LOGGER.info(String.format("Partition Size: %d | Hash Size After Rebuild: %d | Free Space: %d",
                    getPartitionSize(partitionToReload), hashTableSizeAfter, fs.freeSpace));
            //Flush Tuples that are in the probe output buffer of the partition that will be reloaded to disk.
            //Before reloading Build Tuples into memory.
            //Clear the outputbuffer later
            int tuplesToProbeLater = bufferManager.getNumTuples(partitionToReload);
            if (tuplesToProbeLater > 0) {
                RunFileWriter probeRFWriter =
                        getSpillWriterOrCreateNewOneIfNotExist(probeRFWriters, probeRelName, partitionToReload);
                bufferManager.flushPartition(partitionToReload, probeRFWriter);
                bufferManager.clearPartition(partitionToReload);
                LOGGER.info(String.format("Probe Partition %d later | Number of tuples: %d | file size: %d Bytes",
                        partitionToReload, tuplesToProbeLater, probeRFWriter.getFileSize()));
            }
            //Reload Build Tuples from disk into memory.
            RunFileWriter buildRFWriter =
                    getSpillWriterOrCreateNewOneIfNotExist(buildRFWriters, buildRelName, partitionToReload);
            if (loadSpilledPartitionToMem(partitionToReload, buildRFWriter)) {
                inconsistentStatus.set(partitionToReload, true);
                shouldRebuildHashTable = true;
            }

            fs = calculateFreeSpace();
            partitionToReload = selectAPartitionToReload(fs.freeSpace, 0, fs.tuplesInMemory);
        }
        if (shouldRebuildHashTable) {
            rebuildHashTable();
        }
    }

    /**
     * Finds a partition that can fit in the left over memory.
     *
     * @param freeSpace     current free space
     * @param inMemTupCount number of tuples currently in memory
     * @return partition id of selected partition to reload
     */
    protected int selectAPartitionToReloadProbe(long freeSpace, int inMemTupCount) {
        int frameSize = jobletCtx.getInitialFrameSize();
        // Add one frame to freeSpace to consider the one frame reserved for the spilled partition
        long totalFreeSpace = freeSpace + frameSize;
        if (totalFreeSpace > 0) {
            for (int i = spilledStatus.nextSetBit(0); i >= 0 && i < numOfPartitions; i =
                    spilledStatus.nextSetBit(i + 1)) {
                int spilledTupleCount = buildPSizeInTups[i];
                // Expected hash table size increase after reloading this partition
                long originalHashTableSize = table.getCurrentByteSize();
                totalFreeSpace += originalHashTableSize;
                long expectedHashTableSize =
                        SerializableHashTable.getExpectedTableByteSize(inMemTupCount + spilledTupleCount, frameSize);
                if (totalFreeSpace >= buildRFWriters[i].getFileSize() + expectedHashTableSize) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * Creates a file reader that will not be deleted after closing.
     *
     * @param pid
     * @return
     * @throws HyracksDataException
     */
    @Override
    public RunFileReader getProbeRFReader(int pid) throws HyracksDataException {
        return probeRFWriters[pid] == null ? null : probeRFWriters[pid].createReader();
    }

    /**
     * Overrides the original method, but it does not close the File Reader that can be used latter.
     *
     * @param pid
     * @param wr
     * @return
     * @throws HyracksDataException
     */
    protected boolean loadSpilledPartitionToMem(int pid, RunFileWriter wr) throws HyracksDataException {
        RunFileReader r = wr.createReader();
        r.open();
        if (reloadBuffer == null) {
            reloadBuffer = new VSizeFrame(jobletCtx);
        }
        while (r.nextFrame(reloadBuffer)) {
            if (stats != null) {
                //TODO: be certain it is the case this is actually eagerly read
                stats.getBytesRead().update(reloadBuffer.getBuffer().limit());
            }
            accessorBuild.reset(reloadBuffer.getBuffer());
            for (int tid = 0; tid < accessorBuild.getTupleCount(); tid++) {
                if (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
                    // for some reason (e.g. fragmentation) if inserting fails, we need to clear the occupied frames
                    bufferManager.clearPartition(pid);
                    return false;
                }
            }
        }
        // Closes and deletes the run file if it is already loaded into memory.
        spilledStatus.set(pid, false);
        buildRFWriters[pid] = null;
        return true;
    }

    /**
     * Rebuild Hash Table, it throws away the original hashtable and builds a new one.
     *
     * @throws HyracksDataException
     */
    private void rebuildHashTable() throws HyracksDataException {
        freeSpace fs = calculateFreeSpace();
        table.close(); //This releases the frames used by the original Hash Table.
        //Instantiates new hashtable considering the tuples reloaded from disk.
        table = new SerializableHashTable(fs.tuplesInMemory, jobletCtx, bufferManagerForHashTable);
        this.inMemJoiner = new InMemoryHashJoin(jobletCtx, new FrameTupleAccessor(probeRd), probeHpc,
                new FrameTupleAccessor(buildRd), buildRd, buildHpc, isLeftOuter, nonMatchWriters, table, isReversed,
                bufferManagerForHashTable);
        inMemJoiner.setComparator(this.comparator);
        bufferManager.setConstrain(VPartitionTupleBufferManager.NO_CONSTRAIN);
        buildHashTable();
        // Check if the Way I am calculating the free space is right?
        // Size of the Hash Table should fit in my free space.
        // Check the Actual Size of the Hash Table.
        //
    }

    @Override
    public void initProbe(ITuplePairComparator comparator) {
        this.comparator = comparator;
        probePSizeInTups = new int[numOfPartitions];
        inMemJoiner.setComparator(comparator);
        bufferManager.setConstrain(VPartitionTupleBufferManager.NO_CONSTRAIN);
    }

    /**
     * Override the original method, returning inconsistent and spilled partitions.
     * Inconsistent partitions will be probed latter.
     *
     * @return
     */
    @Override
    public BitSet getPartitionStatus() {
        inconsistentStatus.or(spilledStatus);
        return inconsistentStatus;
    }


    /**
     * Complete Probe Phase
     * If the Knobe ("probeInconsistentThisRound") is set as true the inconsistent partitions will be probed before completing this probe phase.
     *
     * @param writer
     * @throws HyracksDataException
     */
    @Override
    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
        if (probeInconsistentThisRound) {
            probeInconsistent(writer);
        }
        super.completeProbe(writer);
    }

    /**
     * Probe inconsistent partitions before completing the Probe Phase.
     *
     * @param writer Output File Writer
     * @throws HyracksDataException
     */
    private void probeInconsistent(IFrameWriter writer) throws HyracksDataException {
        for (int i = inconsistentStatus.nextSetBit(0); i < numOfPartitions && i >= 0; i =
                inconsistentStatus.nextSetBit(i + 1)) {
            if (spilledStatus.get(i)) {
                return;
            }
            RunFileReader reader = getProbeRFReader(i);
            if (reader != null) {
                LOGGER.info(String.format("Partition %d is Inconsistent file has %d bytes", i, reader.getFileSize()));
                reader.open();
                while (reader.nextFrame(reloadBuffer)) {
                    accessorProbe.reset(reloadBuffer.getBuffer());
                    super.probe(reloadBuffer.getBuffer(), writer);
                }
                bufferManager.clearPartition(i);
                reader.setDeleteAfterClose(true);
                reader.close();
                inconsistentStatus.clear(i);
            }
        }
        LOGGER.info(String.format("Finished Probing inconsistent"));
    }

    //endregion
    protected void closeAllSpilledPartitions(RunFileWriter[] runFileWriters, String refName)
            throws HyracksDataException {
        spillRemaining(runFileWriters, refName);
    }
}
