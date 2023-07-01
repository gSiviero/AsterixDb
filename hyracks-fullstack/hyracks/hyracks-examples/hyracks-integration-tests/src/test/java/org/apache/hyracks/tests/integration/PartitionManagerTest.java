
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

import java.util.BitSet;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.join.IPartition;
import org.apache.hyracks.dataflow.std.join.Partition;
import org.apache.hyracks.dataflow.std.join.PartitionComparatorBuilder;
import org.apache.hyracks.dataflow.std.join.PartitionManager;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class PartitionManagerTest {
    int numberOfPartitions = 5;
    int frameSize = 32768;
    int totalNumberOfFrames = 10;
    IHyracksJobletContext context = TestUtils.create(frameSize).getJobletContext();
    RecordDescriptor recordDescriptor =
            new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
    PartitionManager partitionManager;
    BitSet status;
    IFrameTupleAccessor tupleAccessor;
    IPartitionedTupleBufferManager bufferManager;
    IDeallocatableFramePool framePool;
    private final Random rnd = new Random(50);

    public PartitionManagerTest() throws HyracksDataException {
        framePool = new DeallocatableFramePool(context, totalNumberOfFrames * frameSize, false);
        bufferManager =
                new VPartitionTupleBufferManager(null, numberOfPartitions, framePool);
        status = new BitSet(numberOfPartitions);
        bufferManager.setConstrain(
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(status));
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        tupleAccessor.reset(generateIntFrame().getBuffer());
        IFrameTupleAppender tupleAppender = new FrameTupleAppender(new VSizeFrame(context));
        ITuplePartitionComputer partitionComputer = new simplePartitionComputer();
        partitionManager = new PartitionManager(numberOfPartitions, context, bufferManager, partitionComputer,
                tupleAccessor, tupleAppender, status, "RelS");
    }

    @Test
    public void InvalidBufferSizeAndPartitionNumber() throws HyracksDataException {
        int largeNumberOfPartitions = 10;
        int smallFramePoolSize = 5;
        IDeallocatableFramePool framePool = new DeallocatableFramePool(context, smallFramePoolSize * frameSize, false);
        bufferManager =
                new VPartitionTupleBufferManager(null, largeNumberOfPartitions, framePool);
        status = new BitSet(largeNumberOfPartitions);
        bufferManager.setConstrain(
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(status));
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        tupleAccessor.reset(generateIntFrame().getBuffer());
        IFrameTupleAppender tupleAppender = new FrameTupleAppender(new VSizeFrame(context));
        ITuplePartitionComputer partitionComputer = new simplePartitionComputer();
        try {
            partitionManager = new PartitionManager(largeNumberOfPartitions, context, bufferManager, partitionComputer,
                    tupleAccessor, tupleAppender, status, "RelS");
            fail();
        } catch (Exception ex) {
            return;
        }
    }

    @Test
    public void ConstructorTest() {
        assertEquals(partitionManager.getTuplesInMemory(), 0);
        assertEquals(partitionManager.getNumberOfPartitions(), 5);
        assertEquals(partitionManager.getTotalMemory(), 0,0);
    }

    @Test
    public void InsertTuplesIntoPartitionTest() throws HyracksDataException {
        assertEquals(partitionManager.getTotalFrames(), 0);
        assertEquals(partitionManager.getPartition(0).getFramesUsed(),0);
        for(int i=0;i<5;i++) {
            tupleAccessor.reset(generateIntFrameToPartition(0).getBuffer());
            for(int j = 0; j<tupleAccessor.getTupleCount();j++){
                if(!partitionManager.insertTuple(j)){
                    break;
                }
            }
        }
        assertEquals(partitionManager.getPartition(0).getFramesUsed(),5);
        assertEquals(partitionManager.getPartition(1).getFramesUsed(),0);
        assertEquals(partitionManager.getPartition(2).getFramesUsed(),0);
        assertEquals(partitionManager.getPartition(3).getFramesUsed(),0);
        assertEquals(partitionManager.getPartition(4).getFramesUsed(),0);
        assertEquals(partitionManager.getTotalFrames(),5);
        tupleAccessor.reset(generateIntFrameToPartition(0).getBuffer());
        partitionManager.insertTuple(0);
        assertEquals(partitionManager.getPartition(0).getFramesUsed(),6);
    }

    @Test
    public void SpillPartition() throws HyracksDataException {
        int numberOfTuples = 0;
        for(int i=0;i<5;i++) {
            tupleAccessor.reset(generateIntFrameToPartition(0).getBuffer());
            for(int j = 0; j<tupleAccessor.getTupleCount();j++){
                if(!partitionManager.insertTuple(j)){
                    break;
                }
                numberOfTuples++;
            }
        }
        int frames = partitionManager.getPartition(0).getFramesUsed();
        assertEquals(partitionManager.spillPartition(0), frames);
        assertEquals(partitionManager.getTuplesProcessed(), numberOfTuples);
        assertEquals(partitionManager.getTuplesSpilled(), numberOfTuples);
        assertEquals(partitionManager.getTuplesInMemory(), 0);
        assertEquals(partitionManager.getSpilledStatus().cardinality(), 1);
        assertEquals(partitionManager.getBytesReloaded(), 0);
        assertEquals(partitionManager.getBytesSpilled(), 5*frameSize);

    }

    @Test
    public void SpillLargePartition() throws HyracksDataException {
        InsertFrameToPartitionUntilFillBuffers(0, 10);
        assertEquals(partitionManager.spillPartition(0), 10); //Spill all
    }

    @Test
    public void SpillAndReloadPartition() throws HyracksDataException {
        int numberOfTuples = 0;
        for(int i=0;i<5;i++) {
            tupleAccessor.reset(generateIntFrameToPartition(0).getBuffer());
            for(int j = 0; j<tupleAccessor.getTupleCount();j++){
                if(!partitionManager.insertTuple(j)){
                    break;
                }
                numberOfTuples++;
            }
        }
        int framesAvailable = totalNumberOfFrames - (int) partitionManager.getTotalFrames();
        assertEquals(partitionManager.spillPartition(0),5);
        assertTrue(partitionManager.reloadPartition(0, false,framesAvailable));
        assertEquals(partitionManager.getTuplesProcessed(), numberOfTuples);
        assertEquals(partitionManager.getTuplesSpilled(), 0);
        assertEquals(partitionManager.getTuplesInMemory(), numberOfTuples);
        assertEquals(partitionManager.getSpilledStatus().cardinality(), 0);
        Partition p = partitionManager.getPartitionById(0);
        assertEquals(partitionManager.getBytesReloaded(), 5*frameSize);
        assertEquals(partitionManager.getBytesSpilled(), 5*frameSize);

    }

    @Test
    public void GetSpilledPartitionWithLargestBuffer() throws HyracksDataException {
        tupleAccessor.reset(generateIntFrameToPartition(0).getBuffer());
        partitionManager.insertTuple(0);
        tupleAccessor.reset(generateIntFrameToPartition(1).getBuffer());
        partitionManager.insertTuple(1);
        assertEquals(partitionManager.getTuplesInMemory(), 2);
        partitionManager.spillPartition(1);
        assertEquals(partitionManager.getTuplesInMemory(), 1);
        partitionManager.insertTuple(1);
        assertEquals(partitionManager.areAllPartitionsMemoryResident(), false);
        assertEquals(partitionManager.getSpilledStatus().nextSetBit(0), 1);
    }

    @Test
    public void TestTupleCounterComparator() throws HyracksDataException {
        IFrame frame = generateIntFrame();
        tupleAccessor.reset(frame.getBuffer());
        insertFrameToPartition(0);
        InsertFrameToPartitionUntilFillBuffers(1, 5);
        List<Partition> resident = partitionManager.getMemoryResidentPartitions();
        PartitionComparatorBuilder builder = new PartitionComparatorBuilder();
        builder.addInMemoryTupleComparator(true);
        resident.sort(builder.build());
        assertEquals(resident.get(0).getId(), 1);
        builder = new PartitionComparatorBuilder();
        builder.addInMemoryTupleComparator(false);
        resident.sort(builder.build());
        assertEquals(resident.get(0).getId(), 2);
    }


    @Test
    public void TestBufferSizeComparator() throws HyracksDataException {
        IFrame frame = generateIntFrame();
        tupleAccessor.reset(frame.getBuffer());
        insertFrameToPartition(0);
        InsertFrameToPartitionUntilFillBuffers(1, 5);
        List<Partition> resident = partitionManager.getMemoryResidentPartitions();
        PartitionComparatorBuilder builder = new PartitionComparatorBuilder();
        builder.addBufferSizeComparator(true);
        resident.sort(builder.build());
        assertEquals(resident.get(0).getId(), 1);
    }

    @Test
    public void TestSpillToRelease() throws HyracksDataException {
        assertEquals(partitionManager.getTotalFrames(),0);
        for(int i=0;i<6;i++){
            insertFrameToPartition(0);
        }
        assertEquals(partitionManager.getTotalFrames(),6);
        insertFrameToPartition(0);
        assertEquals(partitionManager.getTotalFrames(),7);

    }

    public void InsertFrameToPartitionUntilFillBuffers(int partitionId, int buffersToFill)
            throws HyracksDataException {
        while (partitionManager.getPartition(partitionId).getFramesUsed() < buffersToFill) {
            insertFrameToPartition(partitionId);
        }
    }

    private int tupleToInt(IFrameTupleAccessor accessor, int tupleId) {
        tupleId += 1;
        byte[] arr = accessor.getBuffer().array();
        int t1 = accessor.getFieldEndOffset(tupleId, 0) + accessor.getTupleStartOffset(tupleId);
        return ((arr[t1] & 0xFF) << 24) | ((arr[t1 + 1] & 0xFF) << 16) | ((arr[t1 + 2] & 0xFF) << 8)
                | ((arr[t1 + 3] & 0xFF) << 0);
    }

    protected class simplePartitionComputer implements ITuplePartitionComputer {
        @Override
        public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
            return tupleToInt(accessor, tIndex) % nParts;
        }
    }

    protected IFrame generateIntFrameToPartition(int id) throws HyracksDataException {
        VSizeFrame buffer = new VSizeFrame(context);
        int fieldCount = 1;
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(buffer, true);
        int i = 0;
        while (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            int n = (i++) * numberOfPartitions + id;
            TupleUtils.createIntegerTuple(tb, tuple, n);
            tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        }
        buffer.getFrameSize();
        return buffer;
    }

    private void insertFrameToPartition(int id) throws HyracksDataException{
        IFrame frame = generateIntFrameToPartition(id);
        tupleAccessor.reset(frame.getBuffer());
        int tupleCount = tupleAccessor.getTupleCount();
        for(int i=0;i<tupleCount;i++){
            partitionManager.insertTupleWithSpillPolicy(i,new PreferToSpillFullyOccupiedFramePolicy(bufferManager, partitionManager.getSpilledStatus()));
        }
    }

    private IFrame generateIntFrame() throws HyracksDataException {
        VSizeFrame buffer = new VSizeFrame(context);
        int fieldCount = 1;
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(buffer, true);
        while (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            TupleUtils.createIntegerTuple(tb, tuple, rnd.nextInt());
            tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        }
        return buffer;
    }


}
