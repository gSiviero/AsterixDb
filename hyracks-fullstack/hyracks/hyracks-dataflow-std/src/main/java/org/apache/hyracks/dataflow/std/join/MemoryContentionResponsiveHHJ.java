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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePoolDynamicBudget;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;

public class MemoryContentionResponsiveHHJ extends OptimizedHybridHashJoin {
    /**Frame Count**/
    private int processedFrames = 0;
    /**@todo: This is used to set margins for the memory update, it should not go to production code.**/
    private final int originalBudget;
    ITuplePairComparator comparator;

    //region Knobs
    /**
     * Interval of frames to call the memory update methods
     **/
    private final int frameInterval = 10;
    /**
     * Enables restoration of Build Partitions during the Probe Phase.
     **/
    private final boolean enableRestoration = true;

    private boolean printMemoryState = true;

    int tuplesToProbeLater[];
    private ResourceBrokerOperator resourceBrokerOperator;


    //endregion
    public MemoryContentionResponsiveHHJ(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions,
            String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd,
            ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval,
            IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
        super(jobletCtx, memSizeInFrames, numOfPartitions, probeRelName, buildRelName, probeRd, buildRd, probeHpc,
                buildHpc, probePredEval, buildPredEval, isLeftOuter, nullWriterFactories1);
        this.resourceBrokerOperator = ResourceBrokerFake.registerOperator(10000,1000,numOfPartitions);
        originalBudget = this.resourceBrokerOperator.getBudget();
        this.memSizeInFrames = originalBudget;
        tuplesToProbeLater = new int[numOfPartitions];
    }


    //region BUILD
    public void initBuild() throws HyracksDataException {
        IDeallocatableFramePool framePool =
                new DeallocatableFramePoolDynamicBudget(jobletCtx, memSizeInFrames * jobletCtx.getInitialFrameSize());

        initBuildInternal(framePool);
        this.resourceBrokerOperator.setActualBudget(memSizeInFrames);
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        if (this.resourceBrokerOperator.getStatus()) {
            updateMemoryBudgetBuildPhase(false);
        }
        super.build(buffer);
        processedFrames++;
    }

    /**
     * Updates Memory Budget During Build Phase
     * <ul>
     * <li>This Method check with the Resource Broker if there is a new budget assigned.</li>
     * <li>In case there is a larger budget, the bufferManager will increase its budget.</li>
     * <li>Otherwise partitions will be spilled to free the amount of space needed to adapt to the new budget.</li>
     * </ul>
     */
    private void updateMemoryBudgetBuildPhase(boolean allowGrow) throws HyracksDataException {
        int newBudgetInFrames = this.resourceBrokerOperator.getBudget();
        if (newBudgetInFrames > memSizeInFrames && allowGrow) {
            bufferManager.updateMemoryBudget(newBudgetInFrames);
            memSizeInFrames = newBudgetInFrames;
            this.resourceBrokerOperator.setActualBudget(memSizeInFrames);
        } else if (newBudgetInFrames < memSizeInFrames) {
            memoryContraction(newBudgetInFrames);
        }
    }

    /**
     * Override the original method, if the knob ("eventBased") is set as true the memory budget update is called in case of tuple insertion failed.
     *
     * @param tid Tuple Id
     * @param pid Partition Id
     * @throws HyracksDataException
     */
    protected void processTupleBuildPhase(int tid, int pid) throws HyracksDataException {
        //If event Based Memory adaption is on and the first insert try fails updates the memory budget.
        if (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
            updateMemoryBudgetBuildPhase(true);
            super.processTupleBuildPhaseInternal(tid, pid);
        }
    }
    //endregion

    //region PROBE
    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        try {
            if (frameInterval > 0 && processedFrames % frameInterval == 0) {
                updateMemoryBudgetProbePhase();
            }
            super.probe(buffer, writer);
        } catch (Exception ex) {
            throw new HyracksDataException(ex.getMessage());
        }
        processedFrames++;
    }

    private void updateMemoryBudgetProbePhase() throws HyracksDataException {
        int newBudgetInFrames = this.resourceBrokerOperator.getBudget();
        if (newBudgetInFrames > memSizeInFrames && enableRestoration) { //Memory Expansion Scenario
            if (bufferManager.updateMemoryBudget(newBudgetInFrames)) {
                memSizeInFrames = newBudgetInFrames;
                restoration();
            }
        } else if (newBudgetInFrames < memSizeInFrames) {
            memoryContraction(newBudgetInFrames);
        }
    }

    /**
     * This method spills memory resident partitions in order to adapt to a new memory budget.
     * @summary - It spill memory resident partitions in decreasing order by size, larger partitions are spilled first.
     *          - The Hash table is not rebuilt (during probe).
     * @param newBudgetInFrames - New Budget assigned to the operator.
     * @throws HyracksDataException - Default Exception
     */
    private void memoryContraction(int newBudgetInFrames) throws HyracksDataException {
        while (!bufferManager.updateMemoryBudget(newBudgetInFrames)) {
            int victim = spillPolicy.findInMemPartitionWithMaxMemoryUsage();
            //If there is no resident partition to spill or the largest one is using only one frame, stop.
            if (victim < 0 ||  bufferManager.getPhysicalSize(victim) <= jobletCtx.getInitialFrameSize()) {
                break;
            }
            spillPartition(victim);
        }
        memSizeInFrames = newBudgetInFrames;
        this.resourceBrokerOperator.setActualBudget(memSizeInFrames);
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
    private void restoration() throws HyracksDataException {
        freeSpace fs = calculateFreeSpace();
        int partitionToRestore = selectAPartitionToRestore(fs.freeSpace, fs.tuplesInMemory);
        while (partitionToRestore >= 0) {
            flushProbeOutputBuffer(partitionToRestore);
            //Reload Build Tuples from disk into memory.
            RunFileWriter buildRFWriter =
                    getSpillWriterOrCreateNewOneIfNotExist(buildRFWriters, buildRelName, partitionToRestore);
            if (loadSpilledPartitionToMem(partitionToRestore, buildRFWriter)) {
                inconsistentStatus.set(partitionToRestore, true);
                rebuildHashTable();
            } else {
                break;
            }
            fs = calculateFreeSpace();
            partitionToRestore = selectAPartitionToRestore(fs.freeSpace, fs.tuplesInMemory);
        }
    }

    /**
     * Flush a Partition's output buffer from probe phase.<br>
     * Writes tuples stored in this outputbuffer into disk and then clears the output buffer.
     * @param partitionId Output buffer to be spilled.
     */
    private void flushProbeOutputBuffer(int partitionId) throws HyracksDataException{
        int numberOfInconsistentTuples = bufferManager.getNumTuples(partitionId);
        if (numberOfInconsistentTuples > 0) {
            tuplesToProbeLater[partitionId] += numberOfInconsistentTuples;
            RunFileWriter probeRFWriter =
                    getSpillWriterOrCreateNewOneIfNotExist(probeRFWriters, probeRelName, partitionId);
            int bytesSpilled = bufferManager.flushPartition(partitionId, probeRFWriter);
            if(stats != null) {
                stats.getBytesWritten().update(bytesSpilled);
            }
            bufferManager.clearPartition(partitionId);
            inconsistentStatus.set(partitionId, true);
        }
    }

    /**
     * Finds a partition that can fit in the leftover memory. <br/>
     * The increase in the hash table size is also taken into account.<br/><br/>
     * A partition is only suitable to be restored if the current free memory can fit the entire partition and the increase in hash table caused by the insertion of its tuples.
     *
     * @param freeSpace     Current free space
     * @param inMemTupCount Number of tuples currently in memory
     * @return Partition id of selected partition to reload, -1 if no partition can be reloaded
     */
    protected int selectAPartitionToRestore(long freeSpace, int inMemTupCount) {
        int frameSize = jobletCtx.getInitialFrameSize();
        // Add one frame to freeSpace to consider the one frame reserved for the spilled partition
        long totalFreeSpace = freeSpace; //ToDo Giulliano: Check if it is working
        if (totalFreeSpace > 0) {
            for (int i = spilledStatus.nextSetBit(0); i >= 0 && i < numOfPartitions; i =
                    spilledStatus.nextSetBit(i + 1)) {
                int spilledTupleCount = buildPSizeInTups[i]; //Recalculate the number of Tuples based on the reloaded partition.
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
     * Rebuild Hash Table, it throws away the original hashtable and builds a new one.
     *
     * @throws HyracksDataException
     */
    private void rebuildHashTable() throws HyracksDataException {
        freeSpace fs = calculateFreeSpace();
        inMemJoiner.closeTable();
       //Instantiates new hashtable considering the tuples reloaded from disk.
        table = new SerializableHashTable(fs.tuplesInMemory, jobletCtx, bufferManagerForHashTable);
        this.inMemJoiner.resetTable(table);
        buildHashTable();
    }

    @Override
    public void initProbe(ITuplePairComparator comparator) {
        this.comparator = comparator;
        probePSizeInTups = new int[numOfPartitions];
        inMemJoiner.setComparator(comparator);
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
        if (enableRestoration) {
            probeInconsistent(writer);
        }
        inMemJoiner.completeJoin(writer);
        this.memSizeInFrames = originalBudget;
        bufferManager.updateMemoryBudget(this.memSizeInFrames);
        ResourceBrokerFake.removeOperator(this.resourceBrokerOperator);
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
                reader.open();
                while (reader.nextFrame(reloadBuffer)) {
                    super.probe(reloadBuffer.getBuffer(), writer);
                }
                reader.setDeleteAfterClose(true);
                reader.close();
                inconsistentStatus.clear(i);
                tuplesToProbeLater[i] = 0;
            }
        }
    }

    //endregion
//    protected void closeAllSpilledPartitions(RunFileWriter[] runFileWriters, String refName)
//            throws HyracksDataException {
//        spillRemaining(runFileWriters, refName);
//    }

}
