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

    private Timer timerScenario = new Timer();

    protected static final Logger LOGGER = LogManager.getLogger();
    private static ArrayList<ResourceBrokerOperator> operators = new ArrayList<ResourceBrokerOperator>();
    private int nextId = 0;
    private static int totalMemoryBudget = 0;
    private static Random random = new Random();
    private static ResourceBrokerFake instance = new ResourceBrokerFake();

    private class TimerTaskScenario extends TimerTask{
        public void run() {
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
        Timer timer = new Timer();
        timer.schedule(task,0,10);
//        timerScenario.schedule(new TimerTaskScenario(),getExponential(1));
    }

    public int getExponential(int lambda) {
        return (int) Math.log(1- random.nextDouble())*-lambda;
    }

    private static void distributeBudget(){
        operators.forEach((o) -> {
            if(o.getStatus()){
                o.setNewBudget(Math.max(o.minimumMemory, generateNewBudget()));
        }
        });
    }

    private static int generateNewBudget(){
        int newBudget = 0;
//        if(random.nextInt(100) < 80){
//            newBudget = random.nextInt(totalMemoryBudget);
//        }
//        else{
            int lowerBound = (int) Math.ceil(0.8*totalMemoryBudget);
            int variance = (int) Math.ceil(0.2*totalMemoryBudget);
            newBudget =  lowerBound + random.nextInt(variance);
//        }
        return newBudget;
    }

    public static void setMemBudget(int memoryInFrames){
        totalMemoryBudget = memoryInFrames;
    }

    public static ResourceBrokerOperator registerOperator(int largerRelationSize,int smallerRelationSize,int minimumMemory){
        ResourceBrokerOperator operator = new ResourceBrokerOperator(instance.nextId, minimumMemory,totalMemoryBudget);
        operators.add(operator);
        instance.nextId++;
        return operator;
    }

    public static void removeOperator(ResourceBrokerOperator operator){
        operator.printStats();
        operators.remove(operator);
    }

}

