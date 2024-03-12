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
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.dataflow.common.io.RunFileReader;

public interface IHybridHashJoin {
    void initBuild() throws HyracksDataException;

    void build(ByteBuffer buffer) throws HyracksDataException;

    void closeBuild() throws HyracksDataException;

    void clearBuildTempFiles() throws HyracksDataException;

    void clearProbeTempFiles() throws HyracksDataException;

    void fail() throws HyracksDataException;

    /**
     * Initiate Probe Phase
     * @param comparator Comparator that determines if tuples match.
     * @throws HyracksDataException
     */
    void initProbe(ITuplePairComparator comparator) throws HyracksDataException;

    void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException;

    void completeProbe(IFrameWriter writer) throws HyracksDataException;

    void releaseResource() throws HyracksDataException;

    RunFileReader getBuildRFReader(int pid) throws HyracksDataException;

    int getBuildPartitionSizeInTup(int pid);

    RunFileReader getProbeRFReader(int pid) throws HyracksDataException;

    int getProbePartitionSizeInTup(int pid);

    int getMaxBuildPartitionSize();

    int getMaxProbePartitionSize();

    BitSet getPartitionStatus();

    BitSet getInconsistentStatus();

    int getPartitionSize(int pid);

    void setIsReversed(boolean reversed);

    void setOperatorStats(IOperatorStats stats);

    int updateMemoryBudgetBuild(int newBudget) throws HyracksDataException;

    int updateMemoryBudgetProbe(int newBudget) throws HyracksDataException;

    long getBuildFramesInMemory();

}
