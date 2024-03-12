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

import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MemoryContentionResponsiveHHJ extends HybridHashJoin implements IHybridHashJoin {
    private static final Logger LOGGER = LogManager.getLogger();

    public MemoryContentionResponsiveHHJ(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions,
                                         String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd,
                                         ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval,
                                         IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
        super(jobletCtx, memSizeInFrames, numOfPartitions, probeRelName, buildRelName, probeRd, buildRd, probeHpc,
                buildHpc, probePredEval, buildPredEval, isLeftOuter, nullWriterFactories1);
        dynamicMemory = true;
        this.deleteAfterReload = false;
    }

    /**
     * Update Memory Budget, spilling partitions that can released memory.
     * <p>Partitions already spilled are prefered to be spilled again.</p>
     *
     * @param newMemory
     * @return Number of Frames Released (-) or aquired (+)
     * @throws HyracksDataException Exception
     */
    @Override
    public int updateMemoryBudgetBuild(int newMemory) throws HyracksDataException {
        int originalMemory = this.memSizeInFrames;
        if (!bufferManager.updateMemoryBudget(newMemory)) { //Try to update bufferManager's Budget, this will not spill any partition.
            //If it Fails, spill candidate Partitions until the desired budget is achieved
            LOGGER.info("Spilling due to Memory Contention on Build Phase");
            this.memSizeInFrames -= buildPartitionManager.spillToReleaseFrames(this.memSizeInFrames - newMemory);
        } else {
            //If was possible to update bufferManager's budget update the memSizeInFrames value.
            this.memSizeInFrames = newMemory;
        }
        return -(originalMemory - this.memSizeInFrames);
    }

    @Override
    public int updateMemoryBudgetProbe(int newMemory) throws HyracksDataException {
        int originalMemory = this.memSizeInFrames;
        if(newMemory < numOfPartitions+2){
            throw new HyracksDataException("Not enough Memory");
        }
        if (newMemory > this.memSizeInFrames) {
            LOGGER.info(String.format("Memory Expansion of %d",(newMemory-originalMemory)*jobletCtx.getInitialFrameSize()));
            bufferManager.updateMemoryBudget(newMemory);
            this.memSizeInFrames = newMemory;
            bringPartitionsBack(true);
        } else
        if (newMemory < this.memSizeInFrames) {
            if (!bufferManager.updateMemoryBudget(newMemory)) {
                this.memSizeInFrames -= probePartitionManager.spillToReleaseFrames(this.memSizeInFrames - newMemory);
                LOGGER.info("Spilling PROBE Partition due to memory contention in Probe");
            }
            if (!bufferManager.updateMemoryBudget(newMemory)) {
                this.memSizeInFrames -= buildPartitionManager.spillToReleaseFrames(this.memSizeInFrames - newMemory);
                LOGGER.info("Spilling BUILD Partition due to memory contention in Probe");
            } else {
                this.memSizeInFrames = newMemory;
            }
        }
        return -(originalMemory - this.memSizeInFrames);
    }
}