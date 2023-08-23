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
package org.apache.hyracks.dataflow.std.buffermanager;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;

public class DeallocatableFramePoolDynamicBudget extends DeallocatableFramePool {

    int desiredBuffer = Integer.MAX_VALUE;

    public DeallocatableFramePoolDynamicBudget(IHyracksFrameMgrContext ctx, int memBudgetInBytes) {
        super(ctx, memBudgetInBytes);
    }

    @Override
    public void deAllocateBuffer(ByteBuffer buffer) {
        if (buffer.capacity() != ctx.getInitialFrameSize() || desiredBuffer < allocated) {
            // simply deallocate the Big Object frame
            int framesReleased = buffer.capacity();
            ctx.deallocateFrames(buffer.capacity());
            if (desiredBuffer < allocated) {
                memBudget = memBudget - framesReleased;
                memBudget = Math.max(memBudget, desiredBuffer);
            }
            allocated -= framesReleased;
        } else {
            buffers.add(buffer);
        }
    }

    @Override
    public boolean updateMemoryBudget(int newBudget) {
        desiredBuffer = newBudget * ctx.getInitialFrameSize();
        if (this.allocated < desiredBuffer) {
            this.memBudget = desiredBuffer; //Simply Update Memory Budget
            return true;
        }
        return false; //There are more alocated Frames then the new Budget.
    }

}
