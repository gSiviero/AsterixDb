package org.apache.hyracks.dataflow.std.join;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.SystemMillisClock;

import java.time.Duration;
import java.util.*;

public class ResourceBrokerFake {

    //create an object of SingleObject
    private static ResourceBrokerFake instance = new ResourceBrokerFake();
    private Timer timer = new Timer();

    protected static final Logger LOGGER = LogManager.getLogger();
    private static ArrayList<ResourceBrokerOperator> operators = new ArrayList<ResourceBrokerOperator>();
    private int nextId = 0;
    private static int totalMemoryBudget = 0;
    private static Random random = new Random();
    private static int sigma = 0;
    private static int mean = 0;

    //make the constructor private so that this class cannot be
    //instantiated
    private ResourceBrokerFake(){
        TimerTask task = new TimerTask() {
            public void run() {
                distributeBudget();
            }
        };
        timer.schedule(task,0,10);
    }

    private static void distributeBudget(){
        int newBudget = (int) Math.ceil(random.nextGaussian() * instance.sigma + instance.mean);
        operators.forEach((o) -> {
            if(o.getStatus()){
                o.setNewBudget(Math.max(o.minimumMemory,newBudget));
        }
        });
    }

    public static void setMemBudget(int memoryInFrames){
        mean = memoryInFrames/4;
        sigma = memoryInFrames/24;
        totalMemoryBudget = memoryInFrames;
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