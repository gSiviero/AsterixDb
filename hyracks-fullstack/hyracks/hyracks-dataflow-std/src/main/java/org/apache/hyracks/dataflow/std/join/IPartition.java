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
