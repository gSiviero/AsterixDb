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

package org.apache.hyracks.tests.integration;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.join.HybridHashJoin;
import org.apache.hyracks.dataflow.std.join.MemoryContentionResponsiveHHJ;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class DynamicMemoryHybridHashJoinTests extends HybridHashJoinTests {

    private static final Logger LOGGER = LogManager.getLogger();

    @Test
    public void contructorTest() {
        try {
            dmhhj = new MemoryContentionResponsiveHHJ(context, memorySizeInFrames, numberOfPartitions, "RelS", "RelR",
                    buildRecordDescriptor, buildRecordDescriptor, new simplePartitionComputer(),
                    new simplePartitionComputer(), new dummyPredicateEvaluator(), new dummyPredicateEvaluator(), false,
                    dummyMissingWriters);

            assertNotNull(dmhhj);
            hhj = new HybridHashJoin(context, memorySizeInFrames, numberOfPartitions, "RelS", "RelR",
                    buildRecordDescriptor, buildRecordDescriptor, new simplePartitionComputer(),
                    new simplePartitionComputer(), new dummyPredicateEvaluator(), new dummyPredicateEvaluator(), false,
                    dummyMissingWriters);
            assertNotNull(hhj);
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    void memoryExpansionDuringBuild() throws HyracksDataException {
        initBuildTest();
        for (int i = 0; i < 15; i++) {
            IFrame frame = generateIntFrame();
            dmhhj.build(frame.getBuffer());
            hhj.build(frame.getBuffer());
            dmhhj.updateMemoryBudget(20);
        }
        assertTrue(dmhhj.getPartitionStatus().cardinality() < hhj.getPartitionStatus().cardinality());
    }

    @Test
    void memoryContentionDuringBuild() throws HyracksDataException {
        initBuildTest();
        for (int i = 0; i < 15; i++) {
            IFrame frame = generateIntFrame();
            dmhhj.build(frame.getBuffer());
            hhj.build(frame.getBuffer());
            dmhhj.updateMemoryBudget(7);
        }
        assertTrue(dmhhj.getPartitionStatus().cardinality() > hhj.getPartitionStatus().cardinality());
    }

    @Test
    void memoryContentionDuringProbe() throws HyracksDataException {
        initBuildTest();
        for (int i = 0; i < 15; i++) {
            IFrame frame = generateIntFrame();
            dmhhj.build(frame.getBuffer());
        }
        FileReference file1 = context.createManagedWorkspaceFile("Output_Dynamic_");
        RunFileWriter frameWriterDynamic = new RunFileWriter(file1, context.getIoManager());
        frameWriterDynamic.open();
        dmhhj.closeBuild();
        int spilledStatusBuild = dmhhj.getPartitionStatus().cardinality();
        dmhhj.initProbe(new simpleTupleComparator());
        IFrame frame;
        for (int i = 0; i < memorySizeInFrames; i++) {
            frame = generateIntFrame();
            dmhhj.probe(frame.getBuffer(), frameWriterDynamic);
        }
        //30 so it can fit 5 frames for each build Partition, 5 frames for each probe Partition,
        // plus the Hash Table and some space to bring a partition back.
        dmhhj.updateMemoryBudgetProbe(memorySizeInFrames + 30);
        assertTrue(dmhhj.getPartitionStatus().cardinality() < spilledStatusBuild);
    }
}
