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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourceBrokerOperator {
    protected static final Logger LOGGER = LogManager.getLogger();
    int id;
    int minimumMemory;
    int newBudget;
    int actualBudget;
    private long timeToRelease;
    long totalTimeToRelease = 0;

    public ResourceBrokerOperator(int id, int minimumMemory, int initialMemory) {
        this.id = id;
        this.minimumMemory = minimumMemory;
        this.setNewBudget(initialMemory);
    }

    public synchronized void setNewBudget(int budget) {
        timeToRelease = System.currentTimeMillis();
        this.newBudget = budget;
//        if(!getStatus()){
//            LOGGER.info("UPDATE BUDGET OPERATOR "+ this.id +": "+this.newBudget);
//        }
    }

    public synchronized void setActualBudget(int budget) {
        this.actualBudget = budget;
        if (getStatus()) {
            totalTimeToRelease += System.currentTimeMillis() - timeToRelease;
//            LOGGER.info("UPDATE ACTUAL BUDGET OPERATOR "+ this.id +"("+ (System.currentTimeMillis() - timeToRelease) + "): "+this.actualBudget);
        }
    }

    public void printStats() {
        LOGGER.info("Operator Finished: " + id + " | Time To Release: " + totalTimeToRelease);
    }

    public int getBudget() {
        return getStatus() ? actualBudget : newBudget;
    }

    public synchronized boolean getStatus() {
        return newBudget == actualBudget;
    }
}
