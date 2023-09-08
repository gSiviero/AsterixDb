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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;



public class ResourceBrokerFake {

    //create an object of SingleObject


    protected static final Logger LOGGER = LogManager.getLogger();
    private int nextId = 0;
    /**
     * Target Memory In Frames
     * If the Distribution is Static, this will be the actual memory all the time.
     * If the Distribution is Gaussian, this will be the mean.
     * If the Distribution is Uniform this is the maximum memory.
     */
    private static int maxMemory = 0;
    private static int minimumMemory = 0;
    /**
     * This parameter is used by the Gaussian distribution. It is the standard deviation.<br/>
     * During 95.5% of the time the generated budget will be: <br/><br/>
     * <b>targetMemory - 2*sigma < B < targetMemory + 2*sigma</b>
     */
    private static int sigma = 0;
    private static final Random random = new Random();
    private static final ResourceBrokerFake instance = new ResourceBrokerFake();
    private static MemoryDistributionType type;
    public enum MemoryDistributionType{
        Uniform,
        Gaussian,
        Static,
    }
    private static int counter = 0;


    //make the constructor private so that this class cannot be
    //instantiated
    private ResourceBrokerFake(){
        counter = random.nextInt(4);
    }

    public static int generateNewBudget() {
        counter++;
        counter = counter % 5;
        switch (type) {
            case Gaussian:
                return generateGaussian();
            case Uniform:
                return generateUniform();
            default:
                return maxMemory;
        }
    }

    public static int generateStartingBudget() {
        return maxMemory - random.nextInt((int) (0.2*maxMemory) );
    }

    private static int generateUniform(){
        if(counter != 0){
            return maxMemory - random.nextInt((int) (0.2*maxMemory) );
        }
        return minimumMemory + random.nextInt(maxMemory-minimumMemory);
    }

    private static int generateGaussian(){
        return (int) random.nextGaussian()*sigma + maxMemory;
    }

    /**
     * Configure the Resource Broker.
     * @param tgtMemory
     * @param distributionType
     * @param sig
     */
    public static void configure(int tgtMemory,int minMemory,MemoryDistributionType distributionType,int sig) {
        maxMemory = tgtMemory;
        if(distributionType == MemoryDistributionType.Gaussian){
            if(tgtMemory*3*sig > tgtMemory) {
                LOGGER.info("Target Memory + 3 sigma must be larger than minimum memory");
            }
            maxMemory = tgtMemory - 3*sig;
        }
        if(distributionType == MemoryDistributionType.Uniform && minMemory > tgtMemory){
            LOGGER.info("Target Memory must be larger than minimum memory");
        }
        type = distributionType;
        sigma=sig;
    }


    public static void updateMinimumMemory(int minMemory){
        minimumMemory = minMemory;
    }

}


