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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.io.RunFileReader;

public interface IPartition {
    /**
     * Return Partition's ID
     *
     * @return Partition id
     */
    int getId();

    /**
     * Return number of Tuples in memory.
     *
     * @return
     */
    int getTuplesInMemory();

    /**
     * Return number of Tuples Spilled to Disk
     *
     * @return
     */
    int getTuplesSpilled();

    /**
     * Return number of Bytes Spilled to Disk
     *
     * @return
     */
    int getBytesSpilled();

    /**
     * Return number of <b>BYTES</b> reloaded from disk into memory
     **/
    int getBytesReloaded();

    /**
     * Return spilled status
     *
     * @return <b>FALSE</b> if this partition is memory resident. <b>TRUE</b> if this partition is spilled.
     */
    boolean getSpilledStatus();

    /**
     * Return reload status
     *
     * @return <b>TRUE</b> if this partition was reloaded from disk into memory at least once <b>FALSE</b> otherwise.
     */
    boolean getReloadedStatus();

    /**
     * Return number of Tuples Processed
     *
     * @return
     */
    int getTuplesProcessed();

    /**
     * Get number of <b>BYTES</b> used by Partition.
     *
     * @return Number of <b>BYTES</b>
     */
    int getMemoryUsed();

    /**
     * Get number of <b>FRAMES</b> used by Partition.
     *
     * @return Number of <b>FRAMES</b>
     */
    int getFramesUsed();

    /**
     * Get the File Reader for the temporary file that store spilled tuples.
     *
     * @return File Reader
     */
    RunFileReader getFileReader() throws HyracksDataException;
}
