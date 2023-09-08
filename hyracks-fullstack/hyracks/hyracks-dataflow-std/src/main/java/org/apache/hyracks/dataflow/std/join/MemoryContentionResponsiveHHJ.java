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

import com.couchbase.client.deps.com.lmax.disruptor.FatalExceptionHandler;
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
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;

public class MemoryContentionResponsiveHHJ extends OptimizedHybridHashJoin {
    /**Frame Count**/

    long bytesSpilledDueToContraction = 0;
    long bytesReloadDueToExpansion = 0;
    int restoreOperationsExpansion = 0;
    int spillOperationsContraction = 0;
    int updatesByFrequencyBuild = 0;
    int updatesByFrequencyProbe = 0;
    int proactiveUpdates = 0;
    int memoryIncreaseBuildEvents =0;
    int spillsAvoided =0;
    long executionTime = 0;


    /**@todo: This is used to set margins for the memory update, it should not go to production code.**/
    private final int originalBudget;
    ITuplePairComparator comparator;

    //region Knobs
    /**
     * Interval of frames to call the memory update methods
     **/
    private int frameInterval = 2500;
    /**
     * Enables restoration of Build Partitions during the Probe Phase.
     **/
    private final boolean enableRestoration = true;
    private long memBudgetSum = 0;
    private Random rand = new Random();



    //endregion
    public MemoryContentionResponsiveHHJ(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions,
            String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd,
            ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval,
            IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
        super(jobletCtx, memSizeInFrames, numOfPartitions, probeRelName, buildRelName, probeRd, buildRd, probeHpc,
                buildHpc, probePredEval, buildPredEval, isLeftOuter, nullWriterFactories1);
        originalBudget = memSizeInFrames;
        this.memSizeInFrames = originalBudget;
        executionTime = System.currentTimeMillis();
    }


    //region BUILD
    public void initBuild() throws HyracksDataException {
        IDeallocatableFramePool framePool =
                new DeallocatableFramePoolDynamicBudget(jobletCtx, memSizeInFrames * jobletCtx.getInitialFrameSize());

        initBuildInternal(framePool);
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        if (frameInterval > 0 && buildFrames % frameInterval == 0) {
            updatesByFrequencyBuild++;
            updateMemoryBudgetBuildPhase(true);
        }
        super.build(buffer);
        memBudgetSum+= memSizeInFrames;
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
        int newBudgetInFrames = ResourceBrokerFake.generateNewBudget();
        if (newBudgetInFrames > memSizeInFrames && allowGrow) {
            memoryIncreaseBuildEvents++;
            bufferManager.updateMemoryBudget(newBudgetInFrames);
            memSizeInFrames = newBudgetInFrames;
        } else if (newBudgetInFrames < memSizeInFrames && newBudgetInFrames >= numOfPartitions) {
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
        try {
            int victim = super.processTupleBuildPhaseInternal(tid, pid);
            if (victim != -1) {
                int newBudgetInFrames = ResourceBrokerFake.generateNewBudget();
                if(newBudgetInFrames > memSizeInFrames){
                    spillsAvoided++;
                    bufferManager.updateMemoryBudget(newBudgetInFrames);
                    memSizeInFrames = newBudgetInFrames;
                }
                else{
                    spillPartition(victim);
                }
                super.processTupleBuildPhaseInternal(tid, pid); //This insertion can never fail.
                proactiveUpdates++;
            }
        } catch (Exception ex) {
            LOGGER.info("EXCEPTION Prohascessing Tuple Build P");
            throw new HyracksDataException("Error Build Update");
        }
    }
    //endregion

    //region PROBE
    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        try {
            if (frameInterval > 0 && probeFrames % frameInterval == 0) {
                updatesByFrequencyProbe++;
                updateMemoryBudgetProbePhase();
            }
            super.probe(buffer, writer);
        } catch (Exception ex) {
            throw new HyracksDataException(ex.getMessage());
        }
        memBudgetSum+= memSizeInFrames;
    }

    private void updateMemoryBudgetProbePhase() throws HyracksDataException {
        int newBudgetInFrames = ResourceBrokerFake.generateNewBudget();
        if (newBudgetInFrames > memSizeInFrames && enableRestoration) { //Memory Expansion Scenario
            if (bufferManager.updateMemoryBudget(newBudgetInFrames)) {
                memSizeInFrames = newBudgetInFrames;
                restoration();
            }
        } else if (newBudgetInFrames < memSizeInFrames ) {
            if(bufferManager.updateMemoryBudget(newBudgetInFrames)){
                memSizeInFrames = newBudgetInFrames;
            }
            else if(spilledStatus.cardinality() < numOfPartitions) {
                memoryContraction(newBudgetInFrames);
            }
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
        while (!bufferManager.updateMemoryBudget(newBudgetInFrames) ) {
            int victim = spillPolicy.findInMemPartitionWithMaxMemoryUsage();
            //If there is no resident partition to spill or the largest one is using only one frame, stop.
            if (victim < 0 ||  bufferManager.getPhysicalSize(victim) <= jobletCtx.getInitialFrameSize()) {
                break;
            }
            int bytesSpilled = spillPartition(victim);
            memSizeInFrames -= bytesSpilled/jobletCtx.getInitialFrameSize();
            bytesSpilledDueToContraction += bytesSpilled /jobletCtx.getInitialFrameSize();
            spillOperationsContraction++;
            memSizeInFrames = Math.max(memSizeInFrames,newBudgetInFrames);
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
    private void restoration() throws HyracksDataException {
        freeSpace fs = calculateFreeSpace();
        int partitionToRestore = selectAPartitionToRestore(fs.freeSpace, fs.tuplesInMemory);
        while (partitionToRestore >= 0 && spilledStatus.cardinality() > 0) {
            flushProbeOutputBuffer(partitionToRestore);
            //Reload Build Tuples from disk into memory.
            RunFileWriter buildRFWriter =
                    getSpillWriterOrCreateNewOneIfNotExist(buildRFWriters, buildRelName, partitionToRestore);
            long bytesToReload = buildRFWriter.getFileSize();
            //Partition must be set as resident before reloading, otherwise its buffer will not grow beyond 1 frame
            spilledStatus.clear(partitionToRestore);
            if (loadSpilledPartitionToMem(partitionToRestore, buildRFWriter)) {
                inconsistentStatus.set(partitionToRestore, true);
                restoreOperationsExpansion++;
                bytesReloadDueToExpansion += bytesToReload/jobletCtx.getInitialFrameSize();
                try {
                    rebuildHashTable();
                }
                //As the calculation of the Hash Table size in calculateFreeSpace is merely an estimative,
                // Rebuilding the new hash table with actual tuples can cause an error. In this case spill partition again
                catch (Exception e){
                    spillPartition(partitionToRestore);
                    break;
                }
            } else {
                spilledStatus.set(partitionToRestore);
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
            RunFileWriter probeRFWriter =
                    getSpillWriterOrCreateNewOneIfNotExist(probeRFWriters, probeRelName, partitionId);
            int bytesSpilled = bufferManager.flushPartition(partitionId, probeRFWriter);
            if(stats != null) {
                stats.getBytesWritten().update(bytesSpilled);
            }
            bytesSpilledDueToContraction += bytesSpilled/ jobletCtx.getInitialFrameSize();
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
    protected int selectAPartitionToRestore(long freeSpace, int inMemTupCount) throws  HyracksDataException{
        int frameSize = jobletCtx.getInitialFrameSize();
        // Add one frame to freeSpace to consider the one frame reserved for the spilled partition
        long totalFreeSpace = freeSpace;
        if (totalFreeSpace > 0) {
            totalFreeSpace += SerializableHashTable.getExpectedTableByteSize(inMemTupCount , frameSize);                //Add Size of released Hash Table
            for (int i = spilledStatus.nextSetBit(0); i >= 0 && i < numOfPartitions; i =                       //For each spilled partition
                    spilledStatus.nextSetBit(i + 1)) {
                int spilledTupleCount = buildPSizeInTups[i];                                                            //Recalculate the number of in memory tuples if this partition was reloaded.
                // Expected hash table size increase after reloading this partition
                long expectedHashTableSize =                                                                            //Calculate the size of the new hash table
                        SerializableHashTable.getExpectedTableByteSize(inMemTupCount + spilledTupleCount, frameSize);
                if (totalFreeSpace >= getBuildRFReader(i).getFileSize() + expectedHashTableSize) {                        //If Partition Reloaded and the new hash table fits in memory than returl its index.
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
        try {
            freeSpace fs = calculateFreeSpace();
            table.close();
            table = null;
            //Instantiates new hashtable considering the tuples reloaded from disk.
            table = new SerializableHashTable(fs.tuplesInMemory, jobletCtx, bufferManagerForHashTable);
            this.inMemJoiner.resetTable(table);
            buildHashTable();

        }
        catch (Exception e){
            throw new HyracksDataException("ERROR REBUILDING HASH TABLE");
        }
    }

    @Override
    public void initProbe(ITuplePairComparator comparator) {
        this.comparator = comparator;
        probePSizeInTups = new int[numOfPartitions];
        inMemJoiner.setComparator(comparator);
        bufferManager.setConstrain(VPartitionTupleBufferManager.NO_CONSTRAIN);
        bufferManager.setConstrain( PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus));
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

        probeInconsistent(writer);
        inMemJoiner.completeJoin(writer);
        this.memSizeInFrames = originalBudget;
        bufferManager.updateMemoryBudget(this.memSizeInFrames);
        long spilledTotal = 0;
        long reloadedTotal = 0;
        if(stats != null){
            spilledTotal = (long) stats.getBytesWritten().get() /jobletCtx.getInitialFrameSize();
            reloadedTotal = (long) stats.getBytesRead().get()/jobletCtx.getInitialFrameSize();
        }
        LOGGER.info("[FINISH EXPERIMENT]{"+
                "\"FrameInterval\":"+ frameInterval +
                ",\"ExecutionTime\":" + (System.currentTimeMillis() - executionTime) +
                ",\"AvgInMemoryPartitionsBuild\":\""+ (100*(inMemPartitionsBuildSum/buildFrames)/numOfPartitions) +"%\""+
                ",\"AvgInMemoryPartitionsProbe\":\""+ (100*(inMemPartitionsProbeSum/probeFrames)/numOfPartitions) +"%\""+
                ",\"AverageMemory(Bytes)\":"+ (memBudgetSum/(buildFrames+probeFrames)) * jobletCtx.getInitialFrameSize()+
                ",\"ProbeFrames\":"+ probeFrames +
                ",\"BuildFrames\":"+ buildFrames +
                ",\"NumberOfUpdateBuild\":"+updatesByFrequencyBuild+
                ",\"NumberOfProactiveUpdates\":"+proactiveUpdates+
                ",\"NumberOfEffectiveProactiveUpdates\":"+proactiveUpdates+
                ",\"FrequencyUpdatesProbe\":"+updatesByFrequencyProbe+
                ",\"SpillOperationsToContraction\":"+ spillOperationsContraction +
                ",\"SpillOperationsAvoidedDueToExpansion\":"+ spillsAvoided +
                ",\"TotalSpilledContraction(Bytes)\":" + bytesSpilledDueToContraction+
                ",\"TotalSpilledFrames\": "+spilledTotal+
                ",\"RestoredOperationsDueToExpansion\":"+ restoreOperationsExpansion+
                ",\"TotalRestoredFramesDueToExpansion(Bytes)\":"+ bytesReloadDueToExpansion +
                ",\"TotalRestoredFrames\":"+reloadedTotal+"}[/FINISH EXPERIMENT]");
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
                flushProbeOutputBuffer(i);
            }
            else{
                spillPartition(i);
            }
        }
    }

}
