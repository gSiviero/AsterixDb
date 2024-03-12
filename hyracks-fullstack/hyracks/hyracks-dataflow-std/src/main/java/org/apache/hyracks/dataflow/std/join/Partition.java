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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * This Class describes a Partition of a Hybrid Hash Join Operator.<br>
 * It works as a unique source of truth for its status.
 */
public class Partition implements IPartition {

    //region [PROPERTIES]
    /**
     * Partition id
     */
    private final int id;

    /**
     * Return Partition's ID
     *
     * @return Partition id
     */
    @Override
    public int getId() {
        return id;
    }

    /**
     * Number of Tuples resident in memory.
     */
    private int tuplesInMemory = 0;

    /**
     * Return number of Tuples resident in memory
     *
     * @return Number of Tuples in Memory
     */
    @Override
    public int getTuplesInMemory() {
        return tuplesInMemory;
    }

    /**
     * Number of Tuples Spilled to Disk
     */
    private int tuplesSpilled = 0;

    /**
     * Return number of Tuples spilled to disk
     *
     * @return Number of Tuples spilled to disk
     */
    @Override
    public int getTuplesSpilled() {
        return tuplesSpilled;
    }

    /**
     * Number of Bytes Spilled to Disk
     */
    private int bytesSpilled = 0;

    /**
     * Return number of Bytes Spilled to Disk
     **/
    @Override
    public int getBytesSpilled() {
        return bytesSpilled;
    }

    /**
     * Number of Bytes Reloaded from Disk
     */
    private int bytesReloaded = 0;

    /**
     * Return number of Bytes Reloaded from Disk
     **/
    @Override
    public int getBytesReloaded() {
        return bytesReloaded;
    }

    private String relationName;

    /**
     * Spilled Status<br>
     * <b>TRUE</b> if Partition is spilled.<br>
     * <b>FALSE</b> if Partition is memory resident.
     */
    private boolean spilled = false;

    /**
     * Return the spilled status of Partition.
     *
     * @return <b>TRUE</b> if Partition is spilled.<br>
     * <b>FALSE</b> if Partition is memory resident.
     */
    @Override
    public boolean getSpilledStatus() {
        return spilled;
    }

    /**
     * Reloaded Status<br>
     * <b>TRUE</b> if Partition was reloaded at some point.<br>
     * <b>FALSE</b> if Partition was never reloaded.
     */
    private boolean reloaded = false;

    /**
     * Bets reloaded Status.
     *
     * @return <b>TURE</b> if this partition was reloaded at least once.
     */
    @Override
    public boolean getReloadedStatus() {
        return reloaded;
    }

    /**
     * Get total number of Tuples processed in this Partition, considering both memory resident and spilled tuples.
     *
     * @return Total number of processed tuples
     */
    @Override
    public int getTuplesProcessed() {
        return tuplesInMemory + tuplesSpilled;
    }

    /**
     * Get number of <b>BYTES</b> used by Partition.
     *
     * @return Number of <b>BYTES</b>
     */
    @Override
    public int getMemoryUsed() {
        return bufferManager.getPhysicalSize(id);
    }

    /**
     * Get number of <b>FRAMES</b> used by Partition.
     *
     * @return Number of <b>FRAMES</b>
     */
    @Override
    public int getFramesUsed() {
        return getMemoryUsed() / context.getInitialFrameSize();
    }

    /**
     * Get the File Reader for the temporary file that store spilled tuples.
     *
     * @return File Reader
     */
    public RunFileReader getFileReader() throws HyracksDataException {
        createFileReaderIfNotExist();
        return  rfReader;
    }

    /**
     * Frame accessor, provides access to tuples in a byte buffer. It must be created with a record descriptor.
     */
    private final IFrameTupleAccessor frameTupleAccessor;
    private final TuplePointer tuplePointer = new TuplePointer();
    /**
     * Buffer manager, responsable for allocating frames from a memory pool to partition buffers.<br>
     * It is shared among Build Partitions, Probe Partitions and Hash Table.
     */
    private final IPartitionedTupleBufferManager bufferManager;
    /**
     * File reader, gives writer access to a temporary file to store tuples spilled.
     */
    private RunFileWriter rfWriter;
    /**
     * File reader, gives reader access to a temporary file to store tuples spilled.
     */
    private RunFileReader rfReader;
    /**
     * Buffer used during reload fo partition from Disk, it is not managed by the buffer pool.
     */
    private final IFrame reloadBuffer;
    /**
     * Tuple appender used to store larger tuples.
     */
    private final IFrameTupleAppender tupleAppender;
    /**
     * Joblet context
     */
    private final IHyracksJobletContext context;
    boolean closed = false;
    /**
     * Maximum number of bytes a frame can hold.
     * If a tuple is larger than this limit than it will be inserted as a large tuple.
     * This condition is verified in {@link #insertTuple(int)}.
     */
    private int frameLimit= Integer.MAX_VALUE;

    /**
     * Cluster Target is defined as the limit of frames a spilled partition can hold.
     * This is better described in
     * <a href="https://www.vldb.org/conf/1994/P379.PDF"> Memory-Contention Responsive Hash Joins</a> section 3.1
     */
    private int clusterTarget = -1;
    //endregion

    //region [CONSTRUCTORS]

    /**
     * Defailt Constructor
     *
     * @param id                 Id of Partition, assigned by the Partition Manger
     * @param bufferManager      Buffer manager shared between other objects
     * @param context            Database context
     * @param frameTupleAccessor Frame Tupple acessor
     * @param tupleAppender      Tupple appender for Large tupples
     * @param reloadBuffer       Buffer used to reload Partition from disk
     * @param relationName       Relation's Name
     */
    public Partition(int id, IPartitionedTupleBufferManager bufferManager, IHyracksJobletContext context,
                     IFrameTupleAccessor frameTupleAccessor, IFrameTupleAppender tupleAppender, IFrame reloadBuffer,
                     String relationName,int frameLimit) throws HyracksDataException{
        this.id = id;
        this.bufferManager = bufferManager;
        this.frameTupleAccessor = frameTupleAccessor;
//        bufferManager.reserveBufer();
        this.tupleAppender = tupleAppender;
        this.reloadBuffer = reloadBuffer;
        this.context = context;
        this.relationName = relationName;
        this.frameLimit = frameLimit;
    }

    /**
     * Set value of cluster target
     * @param clusterTarget
     */
    public void setClusterTarget(int clusterTarget){
        this.clusterTarget = clusterTarget;
    }

    //endregion

    //region [METHODS]

    /**
     * Insert Tuple into Partition's Buffer.
     * If insertion fails return <b>FALSE</b>, a failure happens means that the Buffer is Full.
     *
     * @param tupleId id of tuple in Buffer being inserted into Partition
     * @return <b>TRUE</b> if tuple was inserted successfully<br> <b>FALSE</b> If buffer is full.
     * @throws HyracksDataException Exception
     */
    public boolean insertTuple(int tupleId) throws HyracksDataException {
        int framesNeededForTuple = bufferManager.framesNeeded(frameTupleAccessor.getTupleLength(tupleId), 0);
        if (framesNeededForTuple * context.getInitialFrameSize() > bufferManager.getBufferPoolSize()) {
            throw HyracksDataException.create(ErrorCode.INSUFFICIENT_MEMORY);
        }
        else if (framesNeededForTuple > this.frameLimit) {
            insertLargeTuple(tupleId);
            return true;
        }
        else if((getFramesUsed() >= this.clusterTarget+1)  && spilled && this.clusterTarget != -1){
            return false;
        }
        if (bufferManager.insertTuple(this.id, frameTupleAccessor, tupleId, tuplePointer)) {
            tuplesInMemory++;
            return true;
        }
        return false;
    }

    /**
     * If Tuple is larger than the buffer can fit, automatically spill it to disk and mark the partition as spilled.
     *
     * @param tupleId Large Tuple that should be flushed to disk
     * @throws HyracksDataException Exception
     */
    public void insertLargeTuple(int tupleId) throws HyracksDataException {
        createFileWriterIfNotExist();
        if (!tupleAppender.append(frameTupleAccessor, tupleId)) {
            throw new HyracksDataException("The given tuple is too big");
        }
        tupleAppender.write(rfWriter, true);
        spilled = true;
        tuplesSpilled++;
    }

    /**
     * Spill Partition to Disk, writing all its tuples into a file on disk.
     *
     * @throws HyracksDataException Exception
     */
    public void spill() throws HyracksDataException {
        if (tuplesInMemory == 0) {
            return;
        }
        try {
            createFileWriterIfNotExist();
            bytesSpilled += bufferManager.getPhysicalSize(id);
            bufferManager.flushPartition(id, rfWriter);
            bufferManager.clearPartition(id);
            tuplesSpilled += tuplesInMemory;
            tuplesInMemory = 0;
            this.spilled = true;
        } catch (Exception ex) {
            throw new HyracksDataException("Error spilling Partition");
        }

    }

    /**
     * Spill Partition and Close Temporary File.
     *
     * @throws HyracksDataException Exception
     */
    public void close() throws HyracksDataException {
        if (tuplesInMemory > 0)
            spill();
        closed = true;
    }

    public boolean reload(boolean deleteAfterReload,int framesAvailable) throws HyracksDataException{
        if(this.getFileSize()/context.getInitialFrameSize() <= framesAvailable){
            return reload(deleteAfterReload);
        }
        return false;
    }

    /**
     * Reload partition from Disk.
     *
     * @return <b>TRUE</b> if Partition was reloaded successfully.<br> <b>FALSE</b> if something goes wrong.
     * @throws HyracksDataException Exception
     */
    private boolean reload(boolean deleteAfterReload) throws HyracksDataException {
        if (!spilled)
            return true;
        createFileReaderIfNotExist();
        try {
            while (rfReader.nextFrame(reloadBuffer)) {
                frameTupleAccessor.reset(reloadBuffer.getBuffer());
                for (int tid = 0; tid < frameTupleAccessor.getTupleCount(); tid++) {
                    if (!bufferManager.insertTuple(this.id, frameTupleAccessor, tid, tuplePointer)) {
                        // for some reason (e.g. fragmentation) if inserting fails, we need to clear the occupied frames
                        bufferManager.clearPartition(this.id);
                        return false;
                    }
                }
            }
            bytesReloaded += rfReader.getFileSize();
        } catch (Exception ex) {
            throw new HyracksDataException(ex.getMessage());
        } finally {
            if (deleteAfterReload) {
                rfReader.close();
                rfWriter.close();
                rfWriter = null;
                rfReader = null;
            }
        }
        closed = false;
        spilled = false;
        this.tuplesInMemory += this.tuplesSpilled;
        this.tuplesSpilled = 0;
        reloaded = true;
        return true;
    }

    /**
     * Get Temporary File Size in <b>BYTES</b>
     *
     * @return Number containing the temporary File Size in <b>BYTES</b>
     */
    public long getFileSize() {
        if (rfWriter == null)
            return 0;
        return rfWriter.getFileSize();
    }

    /**
     * Reset all Properties, close temporary Files and Delete them.
     *
     * @throws HyracksDataException Exception
     */
    public void cleanUp() throws HyracksDataException {
        tuplesSpilled = 0;
        tuplesInMemory = 0;
        closed = false;
        spilled = false;
        bufferManager.clearPartition(id);
        if (rfWriter != null) {
            CleanupUtils.fail(rfWriter, null);
        }
        rfReader = null;
        rfWriter = null;
    }

    /**
     * Internal method to create a File Writer if it was not created yet.
     *
     * @throws HyracksDataException
     */
    private void createFileWriterIfNotExist() throws HyracksDataException {
        if (rfWriter == null) {
            String fileName = String.format("$%s-Partition_%d-", relationName, id);
            FileReference file = context.createManagedWorkspaceFile(fileName);
            rfWriter = new RunFileWriter(file, context.getIoManager());
            rfWriter.open();
        }
    }

    /**
     * Internal method to create a File Reader if it was not created yet.
     *
     * @throws HyracksDataException
     */
    private void createFileReaderIfNotExist() throws HyracksDataException {
        if (rfWriter != null && rfReader == null) {
            rfReader = rfReader != null ? rfReader : rfWriter.createDeleteOnCloseReader();
            rfReader.setDeleteAfterClose(true);
            rfReader.open();
        }
    }
    //endregion
}