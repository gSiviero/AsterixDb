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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.join.HybridHashJoin;
import org.apache.hyracks.dataflow.std.join.MemoryContentionResponsiveHHJ;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

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
    public void memoryExpansionDuringBuild() throws HyracksDataException {
        initBuildTest();
        for (int i = 0; i < 15; i++) {
            IFrame frame = generateIntFrame();
            dmhhj.build(frame.getBuffer());
            hhj.build(frame.getBuffer());
            dmhhj.updateMemoryBudgetBuild(20);
        }
        assertTrue(dmhhj.getPartitionStatus().cardinality() < hhj.getPartitionStatus().cardinality());
    }

    @Test
    public void memoryContentionDuringBuild() throws HyracksDataException {
        initBuildTest();
        for (int i = 0; i < 15; i++) {
            IFrame frame = generateIntFrame();
            dmhhj.build(frame.getBuffer());
            hhj.build(frame.getBuffer());
            dmhhj.updateMemoryBudgetBuild(7);
        }
        assertTrue(dmhhj.getPartitionStatus().cardinality() > 0);
        assertTrue(dmhhj.getPartitionStatus().cardinality() > hhj.getPartitionStatus().cardinality());
    }

    @Test
    public void memoryExpansionDuringProbe() throws HyracksDataException {
        initBuildTest();
        for (int i = 0; i < 2; i++) {
            IFrame frame = generateIntFrameToPartition(i%numberOfPartitions);
            dmhhj.build(frame.getBuffer());
        }
        FileReference file1 = context.createManagedWorkspaceFile("Output_Dynamic_");
        RunFileWriter frameWriterDynamic = new RunFileWriter(file1, context.getIoManager());
        frameWriterDynamic.open();
        dmhhj.closeBuild();
        dmhhj.initProbe(new simpleTupleComparator());
        IFrame frame;
        for (int i = 0; i < memorySizeInFrames; i++) {
            frame = generateIntFrame();
            dmhhj.probe(frame.getBuffer(), frameWriterDynamic);
        }
        assertTrue(dmhhj.getPartitionStatus().cardinality() > 0);
        //Probe Partitions use 5 frames
        //Build Partitions use 15 frames minimum considering even distribution
        //The Hash Table uses 50 Frames total (calculated debugging)
        //Therefore at least one partition should be spilled if we increase memory budget to less than (5+15+50) = 70
        dmhhj.updateMemoryBudgetProbe(70);
        assertTrue(dmhhj.getPartitionStatus().cardinality() > 0);
        //All partitions should be in memory if the memory budget is equal or greater than 72.
        dmhhj.updateMemoryBudgetProbe(71);
        assertEquals(dmhhj.getPartitionStatus().cardinality(), 0);
        assertEquals(dmhhj.getBuildFramesInMemory(),16);
    }

    @Test
    public void memoryContentionDuringProbe() throws HyracksDataException {
        initBuildTest();
        for (int i = 1; i <= 15; i++) {
            IFrame frame = generateIntFrameToPartition(i%numberOfPartitions);
            dmhhj.build(frame.getBuffer());
        }
        FileReference file1 = context.createManagedWorkspaceFile("Output_Dynamic_");
        RunFileWriter frameWriterDynamic = new RunFileWriter(file1, context.getIoManager());
        frameWriterDynamic.open();
        dmhhj.closeBuild();
        dmhhj.initProbe(new simpleTupleComparator());
        IFrame frame;
        for (int i = 0; i < memorySizeInFrames; i++) {
            frame = generateIntFrame();
            dmhhj.probe(frame.getBuffer(), frameWriterDynamic);
        }
        dmhhj.updateMemoryBudgetProbe(72);
        assertEquals(dmhhj.getPartitionStatus().cardinality(), 0);
        dmhhj.updateMemoryBudgetProbe(7);
        assertEquals(dmhhj.getPartitionStatus().cardinality() ,5);
        assertEquals(dmhhj.getBuildFramesInMemory(),0);
        try{
            dmhhj.updateMemoryBudgetProbe(6);
            fail();
        }
        catch(Exception ex){
            return;
        }
    }
}
