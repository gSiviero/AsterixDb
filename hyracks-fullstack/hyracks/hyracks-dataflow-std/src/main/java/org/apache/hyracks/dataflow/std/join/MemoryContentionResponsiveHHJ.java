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
    private int originalBudget;
    ITuplePairComparator comparator;

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
     * Enables probing inconsistent partitions before completing the build phase
     **/
    private boolean probeInconsistentThisRound = true;

    private boolean printMemoryState = false;

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
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        if (this.resourceBrokerOperator.getStatus()) {
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
        int newBudgetInFrames = this.resourceBrokerOperator.getBudget();;
        if(newBudgetInFrames == -1){
            return;
        }
        if (newBudgetInFrames >= memSizeInFrames && memoryExpansionBuild) {
            bufferManager.updateMemoryBudget(newBudgetInFrames);
            memSizeInFrames = newBudgetInFrames;
            this.resourceBrokerOperator.setActualBudget(memSizeInFrames);
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
            this.resourceBrokerOperator.setActualBudget(memSizeInFrames);
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
        try {
            super.probe(buffer, writer);
        } catch (Exception ex) {
            throw new HyracksDataException(ex.getMessage());
        }
        processedFrames++;
    }

    private void updateMemoryBudgetProbePhase() throws HyracksDataException {
        int newBudgetInFrames = this.resourceBrokerOperator.getBudget();
        if(newBudgetInFrames == -1){
            return;
        }
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
            if (victimPartition < 0 || getBuildPartitionSizeInTup(victimPartition) <= 0) {
                break;
            }
            int framesToRelease = bufferManager.getPhysicalSize(victimPartition) / jobletCtx.getInitialFrameSize();
            spillPartition(victimPartition);
            memSizeInFrames -= framesToRelease;
            memSizeInFrames = memSizeInFrames <= newBudgetInFrames ? newBudgetInFrames : memSizeInFrames;
        }
        rebuildHashTable();
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
        int partitionToReload = selectAPartitionToReloadProbe(fs.freeSpace, fs.tuplesInMemory);
        while (partitionToReload >= 0) {
            //Flush Tuples that are in the probe output buffer of the partition that will be reloaded to disk.
            //Before reloading Build Tuples into memory.
            //Clear the outputbuffer later
            int numberOfInconsistentTuples = bufferManager.getNumTuples(partitionToReload);
            if (numberOfInconsistentTuples > 0) {
                tuplesToProbeLater[partitionToReload] += numberOfInconsistentTuples;
                RunFileWriter probeRFWriter =
                        getSpillWriterOrCreateNewOneIfNotExist(probeRFWriters, probeRelName, partitionToReload);
                bufferManager.flushPartition(partitionToReload, probeRFWriter);
                bufferManager.clearPartition(partitionToReload);
                inconsistentStatus.set(partitionToReload, true);
            }
            //Reload Build Tuples from disk into memory.
            RunFileWriter buildRFWriter =
                    getSpillWriterOrCreateNewOneIfNotExist(buildRFWriters, buildRelName, partitionToReload);
            if (loadSpilledPartitionToMem(partitionToReload, buildRFWriter)) {
                inconsistentStatus.set(partitionToReload, true);
                rebuildHashTable();
            } else {
                break;
            }
            fs = calculateFreeSpace();
            partitionToReload = selectAPartitionToReloadProbe(fs.freeSpace, fs.tuplesInMemory);
        }
    }

    /**
     * Finds a partition that can fit in the left over memory.
     *
     * @param freeSpace     current free space
     * @param inMemTupCount number of tuples currently in memory
     * @return partition id of selected partition to reload
     * @Todo Giulliano: Print what is the actual state in during the Event.
     * Use Warn Logger Level.
     *
     */
    protected int selectAPartitionToReloadProbe(long freeSpace, int inMemTupCount) {
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
        table.close();
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
        if (probeInconsistentThisRound) {
            probeInconsistent(writer);
        }
        inMemJoiner.completeJoin(writer);
        ResourceBrokerFake.removeOperator(this.resourceBrokerOperator);
    }

    /**
     * Probe inconsistent partitions before completing the Probe Phase.
     *
     * @param writer Output File Writer
     * @throws HyracksDataException
     */
    private void probeInconsistent(IFrameWriter writer) throws HyracksDataException {
        printMemoryState("Before probing Inconsistent");
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
    protected void closeAllSpilledPartitions(RunFileWriter[] runFileWriters, String refName)
            throws HyracksDataException {
        spillRemaining(runFileWriters, refName);
    }

    private void printMemoryState(String event) throws HyracksDataException {
        if (this.printMemoryState) {
            int tuplesInMemory = 0;
            int memUsed = 0;
            for (int i = 0; i < this.numOfPartitions; i++) {
                memUsed += this.bufferManager.getPhysicalSize(i);
                if (!spilledStatus.get(i))
                    tuplesInMemory += buildPSizeInTups[i];
            }
            LOGGER.info("#########################################################################################");
            LOGGER.info(event);
            LOGGER.info("#########################################################################################");
            LOGGER.info(String.format("Memory Budget    : %2d FRAMES | %9d BYTES", memSizeInFrames,
                    memSizeInFrames * jobletCtx.getInitialFrameSize()));
            LOGGER.info(String.format("Partitions Buffer: %2d FRAMES | %9d BYTES",
                    memUsed / jobletCtx.getInitialFrameSize(), memUsed));
            if (this.table != null) {
                LOGGER.info(String.format("Hash Table       : %2d FRAMES | %9d BYTES | %d ENTRIES",
                        this.table.getCurrentByteSize() / jobletCtx.getInitialFrameSize(),
                        this.table.getCurrentByteSize(), this.table.getTableSize()));
            }
            long expectedHTSize =
                    SerializableHashTable.getExpectedTableByteSize(tuplesInMemory, jobletCtx.getInitialFrameSize());
            LOGGER.info(String.format("Exp. Hash Table  : %2d FRAMES | %9d BYTES | %d ENTRIES \n",
                    expectedHTSize / jobletCtx.getInitialFrameSize(), expectedHTSize, tuplesInMemory));
            LOGGER.info(
                    "#####################################################################################################");
            LOGGER.info(
                    "|                                         Partitions                                                |");
            LOGGER.info(
                    "#####################################################################################################");
            LOGGER.info(
                    "|---------------------------------------------------------------------------------------------------|");
            LOGGER.info(
                    "|Partition |  Spilled | Inconsis |  Memory  | Build Tup| Build File| Probe Tup|Inc.Tuples|Probe File|");
            LOGGER.info(
                    "|---------------------------------------------------------------------------------------------------|");

            for (int i = 0; i < this.numOfPartitions; i++) {
                long buildFileSize = 0;
                if (buildRFWriters[i] != null) {
                    buildFileSize = buildRFWriters[i].getFileSize();
                }
                long probeFileSize = 0;
                if (probeRFWriters[i] != null) {
                    probeFileSize = probeRFWriters[i].getFileSize();
                }
                memUsed += this.bufferManager.getPhysicalSize(i);
                int probeTuples = 0;
                if (probePSizeInTups != null) {
                    probeTuples = probePSizeInTups[i];
                }
                LOGGER.info(
                        String.format("|%10d|%10b|%10b|%8d B|%10d|%8d B|%10d|%10d|%9d B|", i, this.spilledStatus.get(i),
                                this.inconsistentStatus.get(i), this.bufferManager.getPhysicalSize(i),
                                buildPSizeInTups[i], buildFileSize, probeTuples, tuplesToProbeLater[i], probeFileSize));
            }
            LOGGER.info(
                    "|---------------------------------------------------------------------------------------------------|");
        }
    }
}
