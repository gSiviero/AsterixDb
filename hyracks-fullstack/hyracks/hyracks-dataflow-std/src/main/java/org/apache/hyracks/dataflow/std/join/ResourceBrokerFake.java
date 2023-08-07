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

import java.time.Duration;
import java.util.*;



public class ResourceBrokerFake {

    //create an object of SingleObject

    private Timer timer = new Timer();
    private Timer timerScenario = new Timer();

    protected static final Logger LOGGER = LogManager.getLogger();
    private static ArrayList<ResourceBrokerOperator> operators = new ArrayList<ResourceBrokerOperator>();
    private int nextId = 0;
    private static int totalMemoryBudget = 0;
    private static int memoryScenario = 0;
    private static Random random = new Random();
    private static int sigma = 0;
    private static int mean = 0;
    private static ResourceBrokerFake instance = new ResourceBrokerFake();

    private class TimerTaskScenario extends TimerTask{
        public void run() {
            memoryScenario = 32 * (random.nextInt(7) + 1);
            timerScenario.cancel();
            timerScenario = new Timer();
            timerScenario.schedule(new TimerTaskScenario(),getExponential(10));
        }
    }
    //make the constructor private so that this class cannot be
    //instantiated
    private ResourceBrokerFake(){
        TimerTask task = new TimerTask() {
            public void run() {
                distributeBudget();
            }
        };
        timer.schedule(task,0,10);
//        timerScenario.schedule(new TimerTaskScenario(),getExponential(1));
    }

    public int getExponential(int lambda) {
        return (int) Math.log(1- this.random.nextDouble())*-lambda;
    }

    private static void distributeBudget(){
        int newBudget = 0;
        if(random.nextInt(100) < 80){
            newBudget = (int) Math.ceil(20 + random.nextInt(totalMemoryBudget - 20));
        }
        else{
            int lowerBound = (int) Math.ceil(0.8*totalMemoryBudget);
            int variance = (int) Math.ceil(0.2*totalMemoryBudget);;
            newBudget =  (int) Math.ceil(lowerBound + random.nextInt(variance));
        }
        int finalNewBudget = newBudget;
        operators.forEach((o) -> {
            if(o.getStatus()){
                o.setNewBudget(Math.max(o.minimumMemory, finalNewBudget));
        }
        });
    }



    public static void setMemBudget(int memoryInFrames){
        mean = 32*8;
        sigma = memoryInFrames/24;
        totalMemoryBudget = 32*1;
    }

    public static ResourceBrokerOperator registerOperator(int largerRelationSize,int smallerRelationSize,int minimumMemory){
        int newBudget = (int) random.nextGaussian() * instance.sigma + instance.mean;
        ResourceBrokerOperator operator = new ResourceBrokerOperator(instance.nextId, minimumMemory,newBudget);
        operators.add(operator);
        instance.nextId++;
        return operator;
    }

    public static void removeOperator(ResourceBrokerOperator operator){
        operator.printStats();
        operators.remove(operator);
    }

}

 class  ResourceBrokerOperator{
     protected static final Logger LOGGER = LogManager.getLogger();
    int id;
    int largestRelationSize;
    int smallerRelationSize;
    int minimumMemory;
    Date startedAt;
    Date finishedAt;
    int newBudget;
    int actualBudget;
    private long timeToRelease;
    long totalTimeToRelease =0;
    public enum JoinType{
        SMALL,MEDIUM,LARGE
    }
    public ResourceBrokerOperator(int id,int minimumMemory,int initialMemory){
        this.id = id;
        this.minimumMemory =minimumMemory;
        this.setNewBudget(initialMemory);
    }
    public synchronized void setNewBudget(int budget){
        timeToRelease = System.currentTimeMillis();
        this.newBudget = budget;
        if(!getStatus()){
            LOGGER.info("UPDATE BUDGET OPERATOR "+ this.id +": "+this.newBudget);
        }
    }
    public synchronized void setActualBudget(int budget){
        this.actualBudget = budget;
        if(getStatus()){
            totalTimeToRelease += System.currentTimeMillis() - timeToRelease;
            LOGGER.info("UPDATE ACTUAL BUDGET OPERATOR "+ this.id +"("+ (System.currentTimeMillis() - timeToRelease) + "): "+this.actualBudget);
        }
    }

    public void printStats(){
        LOGGER.info("Operator Finished: "+id+" | Time To Release: " + totalTimeToRelease);
    }

    public int getBudget(){
        return getStatus() ? -1 : newBudget;
    }

    public synchronized boolean getStatus(){
        return newBudget == actualBudget;
    }
    public JoinType getType(int availableMemory){
        int largestRelation = largestRelationSize/availableMemory;
        int smallestRelation = smallerRelationSize / availableMemory;
        if(largestRelation <= 1 && smallestRelation <= 0.25){
            return JoinType.SMALL;
        }
        if(largestRelation > 1 && largestRelation < 4 && smallestRelation > 0.25 && smallestRelation < 1){
            return JoinType.MEDIUM;
        }
        return JoinType.LARGE;
    }
}