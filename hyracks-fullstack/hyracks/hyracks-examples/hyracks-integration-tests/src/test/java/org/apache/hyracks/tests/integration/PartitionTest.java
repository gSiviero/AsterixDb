
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
import java.util.Random;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.join.Partition;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Test;

import static org.junit.Assert.*;

//region [TEMPORARY FILES MANAGEMENT]
public class PartitionTest {
    int numberOfPartitions = 5;
    int frameSize = 32768;
    int totalNumberOfFrames = 10;
    IHyracksJobletContext context = TestUtils.create(frameSize).getJobletContext();
    RecordDescriptor recordDescriptor =
            new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
    Partition partition;
    BitSet status;
    IFrameTupleAccessor tupleAccessor;
    IDeallocatableFramePool framePool;
    IFrameTupleAppender tupleAppender;
    IPartitionedTupleBufferManager bufferManager;
    VSizeFrame reloadBuffer;
    private final Random rnd = new Random(50);

    public PartitionTest() throws HyracksDataException {
        String relationName = "RelS";
        reloadBuffer = new VSizeFrame(context);
        framePool = new DeallocatableFramePool(context, totalNumberOfFrames * frameSize, true);
        bufferManager =
                new VPartitionTupleBufferManager(null,numberOfPartitions,framePool);
        status = new BitSet(numberOfPartitions);
        bufferManager.setConstrain(
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(status));
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        tupleAppender = new FrameTupleAppender(new VSizeFrame(context));
        tupleAccessor.reset(generateIntFrame().getBuffer());
        partition = new Partition(0, bufferManager, context, tupleAccessor, tupleAppender, reloadBuffer, "RelS",4);
    }

    /**
     * Insert a single tuple into Partition and check the number of tuples in memory, spilled and processed.
     * @throws HyracksDataException
     */
    @Test
    public void TestInsertTupleSuccess() throws HyracksDataException {
        partition.insertTuple(1);
        assertEquals(partition.getId(), 0);
        assertEquals(partition.getTuplesSpilled(), 0);
        assertEquals(partition.getTuplesInMemory(), 1);
        assertEquals(partition.getTuplesProcessed(), 1);
    }

    /**
     * Spill Partition and check the number of tuples in memory, spilled and processed.
     * @throws HyracksDataException
     */
    @Test
    public void Spill() throws HyracksDataException {
        TestInsertTupleSuccess();
        partition.spill();
        assertEquals(partition.getTuplesSpilled(), 1);
        assertEquals(partition.getTuplesInMemory(), 0);
        assertEquals(partition.getTuplesProcessed(), 1);
        assertEquals(partition.getFileSize(), frameSize);
        assertEquals(partition.getMemoryUsed(), 0);
        assertEquals(partition.getBytesSpilled(), frameSize);
        assertEquals(partition.getBytesReloaded(), 0);
    }

    /**
     * Reload the spilled Partition and check the numbers
     * @throws HyracksDataException
     */
    @Test
    public void SpillAndReload10Times() throws HyracksDataException {
        int numberOfTuplesProcessed =0;
        for (int i = 0; i < 10; i++) {
            tupleAccessor.reset(generateIntFrame().getBuffer());
            numberOfTuplesProcessed += InsertFrame();
        }
        for(int i = 0; i< 10 ;i++) {
            partition.spill();
            assertEquals(partition.getFileSize(), 10 * frameSize);
            assertEquals(partition.getFramesUsed(), 0);
            assertEquals(partition.reload(true), true);
            assertEquals(partition.getTuplesSpilled(), 0);
            assertEquals(partition.getTuplesInMemory(), numberOfTuplesProcessed);
            assertEquals(partition.getTuplesSpilled(), 0);
            assertEquals(partition.getFramesUsed(), 10);
            assertEquals(partition.getFileSize(), 0);
        }

        tupleAccessor.reset(generateIntFrame().getBuffer());
        numberOfTuplesProcessed += InsertFrame();
        partition.spill();
        assertEquals(partition.reload(true), false); // Can´t fit Partition in Memory
    }

    /**
     * Reload the spilled Partition and check the numbers
     * @throws HyracksDataException
     */
    @Test
    public void ReloadError() throws HyracksDataException {
        for(int i=0;i<11;i++){
            InsertFrame();
        }
        assertEquals(partition.getFramesUsed(),1);
        assertEquals(partition.getFileSize(),10*frameSize);
        assertEquals(partition.reload(false),false);
        assertEquals(partition.getFramesUsed(),1);
        assertEquals(partition.getFileSize(),10*frameSize);
    }

    @Test
    public void ClosePartition() throws HyracksDataException {
        for (int i = 0; i < 10; i++) {
            tupleAccessor.reset(generateIntFrame().getBuffer());
            InsertFrame();
        }
        assertEquals(partition.getFileReader(), null);
        partition.spill();
        assertNotEquals(partition.getFileReader(), null);
        partition.close();
        assertEquals(partition.getTuplesInMemory(), 0);
        assertEquals(partition.getMemoryUsed(), 0);
        assertNotEquals(partition.getFileReader(), null);
    }

    @Test
    public void CleanUpPartition() throws HyracksDataException {
        for (int i = 0; i < 20; i++) {
            tupleAccessor.reset(generateIntFrame().getBuffer());
            InsertFrame();
        }
        assertNotEquals(partition.getFileReader(), null);
        partition.cleanUp();
        assertEquals(partition.getTuplesInMemory(), 0);
        assertEquals(partition.getFileReader(), null);
    }

    /**
     * Insert one frame and compare if the number of tuples inserted match.
     * @throws HyracksDataException
     */
    @Test
    public void InsertSingleFrame() throws HyracksDataException {
        assertEquals(InsertFrame(), partition.getTuplesProcessed());
    }
    @Test
    /**
     * Insert 10 frames to partition, there should be spills since the Buffer Size is 10 Frames.
     */
    public void Insert20Frames() throws HyracksDataException {
        int numberOfTuplesProcessed = 0;
        //It will only insert 10 frames due to the Frame Pool Size.
        for (int i = 0; i < 20; i++) {
            tupleAccessor.reset(generateIntFrame().getBuffer());
            numberOfTuplesProcessed += InsertFrame();
        }
        assertEquals(numberOfTuplesProcessed, partition.getTuplesProcessed());
        assertEquals(partition.getMemoryUsed(), 10 * frameSize);
        assertEquals(partition.getFramesUsed(), 10);
        assertEquals(partition.getFileSize(), 10 * frameSize);
        partition.spill();
        assertEquals(partition.getMemoryUsed(), 0);
        assertEquals(partition.getFramesUsed(), 0);
        assertEquals(partition.getFileSize(), 20 * frameSize);
        assertEquals(partition.reload(true),false);
        partition.close();
        assertEquals(partition.getMemoryUsed(), 0);
        assertEquals(partition.getFramesUsed(), 0);
    }

    @Test
    /**
     * Insert 10 frames to partition, there should be spills since the Buffer Size is 10 Frames.
     */
    public void Insert20FramesNoSpillPolicy() throws HyracksDataException {
        int numberOfTuplesProcessed = 0;
        //It will only insert 10 frames due to the Frame Pool Size.
        for (int i = 0; i < 20; i++) {
            tupleAccessor.reset(generateIntFrame().getBuffer());
            for(int j =0; j < tupleAccessor.getTupleCount();j++){
                partition.insertTuple(j);
                numberOfTuplesProcessed ++;
            }
        }
        assertNotEquals(numberOfTuplesProcessed, partition.getTuplesProcessed());
        assertEquals(partition.getMemoryUsed(), 10 * frameSize);
        assertEquals(partition.getFramesUsed(), 10);
        assertEquals(partition.getFileSize(), 0 * frameSize);
        partition.spill();
        assertEquals(partition.getMemoryUsed(), 0);
        assertEquals(partition.getFramesUsed(), 0);
        assertEquals(partition.getFileSize(), 10 * frameSize);
        assertEquals(partition.reload(true),true);
        partition.close();
        assertEquals(partition.getMemoryUsed(), 0);
        assertEquals(partition.getFramesUsed(), 0);
    }


    /**
     * @Title Insert a Large Object into Partition.<br>
     * @Summary Altough this Object can fit in memory (Object < Memory Budget) ,
     * we insert partitions to make sure this object can't fit in the available memory,
     * this will force the object to be spilled.
     * @throws HyracksDataException
     */
    @Test
    public void InsertLargeObject() throws HyracksDataException {
        int numberOfTuplesProcessed = InsertFrame(); //Insert Frame To Occupy memory.
        numberOfTuplesProcessed += InsertFrame(); //Insert Frame To Occupy memory.
        //This Frame is Large Enough not to fit in a regular Buffer but smaller than the memory Budget.
        IFrame frame = generateStringFrame(3 * context.getInitialFrameSize());
        tupleAccessor.reset(frame.getBuffer());
        partition.insertTuple(0);
        assertEquals(false, partition.getSpilledStatus());
        assertEquals(0, partition.getTuplesSpilled());
        assertEquals(numberOfTuplesProcessed + 1, partition.getTuplesInMemory());
        frame = generateStringFrame(4 * context.getInitialFrameSize());
        tupleAccessor.reset(frame.getBuffer());
        partition.insertTuple(0);
        assertEquals(true, partition.getSpilledStatus());
        assertEquals(1, partition.getTuplesSpilled());
        assertEquals(numberOfTuplesProcessed + 2, partition.getTuplesProcessed());
    }

    @Test
    public void FailToInsertTupleLargerThanBudget() throws HyracksDataException {
        //This Frame is Larger than Memory Budget.
        int frameSize = 10 * context.getInitialFrameSize();
        tupleAccessor.reset(generateStringFrame(frameSize).getBuffer());
        try {
            partition.insertTuple(0);
            fail();
        } catch (Exception ex) {
            return;
        }
    }

    /**
     * Insert tuples from Tuple Accessor into Partition,
     * If fails spill the partition.
     * @return
     * @throws HyracksDataException
     */
    int InsertFrame() throws HyracksDataException {
        return InsertFrame(partition);
    }

    int InsertFrame(Partition p) throws HyracksDataException {
        int numberOfTuplesInFrame = tupleAccessor.getTupleCount();
        tupleAccessor.reset(generateIntFrame().getBuffer());
        for(int i = 0;i<numberOfTuplesInFrame;i++){
            while(!p.insertTuple(i)){
                p.spill();
            };
        }
        return numberOfTuplesInFrame;
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

    private IFrame generateStringFrame(int length) throws HyracksDataException {
        int fieldCount = 1;
        VSizeFrame frame = new VSizeFrame(context, context.getInitialFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(frame, true);
        ISerializerDeserializer[] fieldSerdes = { new UTF8StringSerializerDeserializer() };
        String data = "";
        for (int i = 0; i < length; i++) {
            data += "X";
        }
        TupleUtils.createTuple(tb, tuple, fieldSerdes, data);
        tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        return frame;
    }
}
