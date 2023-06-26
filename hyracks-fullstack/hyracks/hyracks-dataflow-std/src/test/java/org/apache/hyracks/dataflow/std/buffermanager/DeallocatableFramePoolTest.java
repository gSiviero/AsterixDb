package org.apache.hyracks.dataflow.std.buffermanager;

import junit.framework.TestCase;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Assert;
public class DeallocatableFramePoolTest extends TestCase {

    public void testUpdateMemoryBudget() throws HyracksDataException {
        int frameSize = Common.commonFrameManager.getInitialFrameSize();
        DeallocatableFramePool framePool = new DeallocatableFramePool(Common.commonFrameManager,10*frameSize);
        Assert.assertTrue(framePool.updateMemoryBudget(20));
        Assert.assertEquals(framePool.getMemoryBudget(),10*frameSize);
        framePool = new DeallocatableFramePool(Common.commonFrameManager,10*frameSize,true);
        Assert.assertTrue(framePool.updateMemoryBudget(20));
        Assert.assertEquals(framePool.getMemoryBudget(),20*frameSize);
        Assert.assertTrue(framePool.updateMemoryBudget(10));
        Assert.assertEquals(framePool.getMemoryBudget(),10*frameSize);
        for(int i=0;i<10;i++){
            framePool.allocateFrame(frameSize);
        }
        Assert.assertFalse(framePool.updateMemoryBudget(8));
        Assert.assertEquals(framePool.getMemoryBudget(),10*frameSize);
        VSizeFrame buffer = new VSizeFrame(Common.commonFrameManager);
        VSizeFrame buffer2 = new VSizeFrame(Common.commonFrameManager);
        framePool.deAllocateBuffer(buffer.getBuffer());
        framePool.deAllocateBuffer(buffer2.getBuffer());
        Assert.assertTrue(framePool.updateMemoryBudget(8));
        Assert.assertEquals(framePool.getMemoryBudget(),8*frameSize);
    }
}