/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.hdfs2.dataflow;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;

public class HDFSWriteOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private ConfFactory confFactory;
    private ITupleWriterFactory tupleWriterFactory;
    private String outputPath;

    /**
     * The constructor of HDFSWriteOperatorDescriptor.
     * 
     * @param spec
     *            the JobSpecification object
     * @param conf
     *            the Hadoop JobConf which contains the output path
     * @param tupleWriterFactory
     *            the ITupleWriterFactory implementation object
     * @throws HyracksException
     */
    public HDFSWriteOperatorDescriptor(JobSpecification spec, Job conf, String outputPath,
            ITupleWriterFactory tupleWriterFactory) throws HyracksException {
        super(spec, 1, 0);
        this.confFactory = new ConfFactory(conf);
        this.tupleWriterFactory = tupleWriterFactory;
        this.outputPath = outputPath;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        final Job conf = confFactory.getConf();

        return new AbstractUnaryInputSinkOperatorNodePushable() {

            private String fileName = outputPath + File.separator + "part-" + partition;
            private FSDataOutputStream dos;
            private RecordDescriptor inputRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);;
            private FrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRd);
            private FrameTupleReference tuple = new FrameTupleReference();
            private ITupleWriter tupleWriter;

            @Override
            public void open() throws HyracksDataException {
                tupleWriter = tupleWriterFactory.getTupleWriter();
                try {
                    FileSystem dfs = FileSystem.get(conf.getConfiguration());
                    dos = dfs.create(new Path(fileName), true);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tuple.reset(accessor, i);
                    tupleWriter.write(dos, tuple);
                }
            }

            @Override
            public void fail() throws HyracksDataException {

            }

            @Override
            public void close() throws HyracksDataException {
                try {
                    dos.close();
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

        };
    }
}
