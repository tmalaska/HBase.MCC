package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class HTableStats {

  AtomicLong maxPutTime = new AtomicLong(0);
  AtomicLong maxPutListTime = new AtomicLong(0);
  AtomicLong maxDeleteTime = new AtomicLong(0);
  AtomicLong maxDeleteListTime = new AtomicLong(0);
  AtomicLong maxGetTime = new AtomicLong(0);
  AtomicLong maxGetListTime = new AtomicLong(0);

  AtomicLong putPrimary = new AtomicLong(0);
  AtomicLong putFailover = new AtomicLong(0);
  AtomicLong putListPrimary = new AtomicLong(0);
  AtomicLong putListFailover = new AtomicLong(0);
  AtomicLong getPrimary = new AtomicLong(0);
  AtomicLong getFailover = new AtomicLong(0);
  AtomicLong getListPrimary = new AtomicLong(0);
  AtomicLong getListFailover = new AtomicLong(0);
  AtomicLong deletePrimary = new AtomicLong(0);
  AtomicLong deleteFailover = new AtomicLong(0);
  AtomicLong deleteListPrimary = new AtomicLong(0);
  AtomicLong deleteListFailover = new AtomicLong(0);
  
  long lastPutPrimary = 0;
  long lastPutFailover = 0;
  long lastPutListPrimary = 0;
  long lastPutListFailover = 0;
  long lastGetPrimary = 0;
  long lastGetFailover = 0;
  long lastGetListPrimary = 0;
  long lastGetListFailover = 0;
  long lastDeletePrimary = 0;
  long lastDeleteFailover = 0;
  long lastDeleteListPrimary = 0;
  long lastDeleteListFailover = 0;
  
  
  static final int ROLLING_AVG_WINDOW = 90;
  BlockingQueue<Long> putRollingAverage = new LinkedBlockingQueue<Long>();
  BlockingQueue<Long> putListRollingAverage = new LinkedBlockingQueue<Long>();
  BlockingQueue<Long> getRollingAverage = new LinkedBlockingQueue<Long>();
  BlockingQueue<Long> getListRollingAverage = new LinkedBlockingQueue<Long>();
  BlockingQueue<Long> deleteRollingAverage = new LinkedBlockingQueue<Long>();
  BlockingQueue<Long> deleteListRollingAverage = new LinkedBlockingQueue<Long>();
  
  static final String newLine = System.getProperty("line.separator");
  
  public HTableStats() {
    for (int i = 0; i < ROLLING_AVG_WINDOW; i++) {
      putRollingAverage.add(-1l);
      putListRollingAverage.add(-1l);
      getRollingAverage.add(-1l);
      getListRollingAverage.add(-1l);
      deleteRollingAverage.add(-1l);
      deleteListRollingAverage.add(-1l);
    } 
  }
  
  public void printPrettyStats() {
    System.out.println("Stats: Max Time");
    System.out.println(" > maxPutTime:        " + maxPutTime);
    System.out.println(" > maxPutListTime:    " + maxPutListTime);
    System.out.println(" > maxGetTime:        " + maxGetTime);
    System.out.println(" > maxGetListTime:    " + maxGetListTime);
    System.out.println(" > maxDeleteTime:     " + maxDeleteTime);
    System.out.println(" > maxDeleteListTime: " + maxDeleteListTime);
    System.out.println("Stats: Rolling Avg");
    System.out.println(" > put:               " + getRollingAvg(putRollingAverage));
    System.out.println(" > putList:           " + getRollingAvg(putListRollingAverage));
    System.out.println(" > get:               " + getRollingAvg(getRollingAverage));
    System.out.println(" > getList:           " + getRollingAvg(getListRollingAverage));
    System.out.println(" > delete:            " + getRollingAvg(deleteRollingAverage));
    System.out.println(" > deleteList:        " + getRollingAvg(deleteListRollingAverage));
    System.out.println("Stats: Rolling Max");
    System.out.println(" > put:               " + getRollingMax(putRollingAverage));
    System.out.println(" > putList:           " + getRollingMax(putListRollingAverage));
    System.out.println(" > get:               " + getRollingMax(getRollingAverage));
    System.out.println(" > getList:           " + getRollingMax(getListRollingAverage));
    System.out.println(" > delete:            " + getRollingMax(deleteRollingAverage));
    System.out.println(" > deleteList:        " + getRollingMax(deleteListRollingAverage));
    System.out.println("Stats: Dst");
    System.out.println(" > put:               " + putPrimary + "/" + putFailover);
    System.out.println(" > putList:           " + putListPrimary + "/" + putListFailover);
    System.out.println(" > get:               " + getPrimary + "/" + getFailover);
    System.out.println(" > getList:           " + getListPrimary + "/" + getListFailover);
    System.out.println(" > delete:            " + deletePrimary + "/" + deleteFailover);
    System.out.println(" > deleteList:        " + deleteListPrimary + "/" + deleteListFailover);
  }
  
  public static void printCSVHeaders(Writer writer) throws IOException {
    writer.append("maxPutTime," +
        "maxPutListTime + ," +
        "maxGetTime," +
        "maxGetListTime," +
        "maxDeleteTime," + 
        "maxDeleteListTime," +
        "putRollingAverage," + 
        "putListRollingAverage," + 
        "getRollingAverage," + 
        "getListRollingAverage," + 
        "deleteRollingAverage," + 
        "deleteListRollingAverage," +
        "putRollingMax," + 
        "putListRollingMax," + 
        "getRollingMax," + 
        "getListRollingMax," + 
        "deleteRollingMax," + 
        "deleteListRollingMax," +
        "putPrimaryTotalCount," + 
        "putPrimaryRollingCount," + 
        "putFailoverTotalCount," +
        "putFailoverRollingCount," +
        "putListPrimaryTotalCount," +
        "putListPrimaryRollingCount," +
        "putListFailoverTotalCount," +
        "putListFailoverRollingCount," +
        "getPrimaryCount," +
        "getPrimaryRollingCount," +
        "getFailoverCount," +
        "getFailoverRollingCount," +
        "getListPrimaryCount," +
        "getListPrimaryRollingCount," +
        "getListFailoverCount," +
        "getListFailoverRollingCount," +
        "deletePrimaryCount," + 
        "deletePrimaryRollingCount," + 
        "deleteFailoverCount," + 
        "deleteFailoverRollingCount," + 
        "deleteListPrimaryCount," +
        "deleteListPrimaryRollingCount," +
        "deleteListFailoverCount," +
        "deleteListFailoverRollingCount," +
        newLine);
    
    
  }
  
  
  
  public void printCSVStats(Writer writer) throws IOException {
    writer.append(maxPutTime + "," +
        maxPutListTime + "," +
        maxGetTime + "," +
        maxGetListTime + "," +
        maxDeleteTime + "," + 
        maxDeleteListTime + "," +
        getRollingAvg(putRollingAverage) + "," + 
        getRollingAvg(putListRollingAverage) + "," + 
        getRollingAvg(getRollingAverage) + "," + 
        getRollingAvg(getListRollingAverage) + "," + 
        getRollingAvg(deleteRollingAverage) + "," + 
        getRollingAvg(deleteListRollingAverage) + "," +
        getRollingMax(putRollingAverage) + "," + 
        getRollingMax(putListRollingAverage) + "," + 
        getRollingMax(getRollingAverage) + "," + 
        getRollingMax(getListRollingAverage) + "," + 
        getRollingMax(deleteRollingAverage) + "," + 
        getRollingMax(deleteListRollingAverage) + "," +
        putPrimary + "," + 
        (putPrimary.get()-lastPutPrimary) + "," + 
        putFailover + "," +
        (putFailover.get()-lastPutFailover) + "," +
        putListPrimary + "," +
        (putListPrimary.get()-lastPutListPrimary) + "," +
        putListFailover + "," +
        (putListFailover.get()-lastPutListFailover) + "," +
        getPrimary + "," +
        (getPrimary.get()-lastGetPrimary) + "," + 
        (getFailover.get()) + "," + 
        (getFailover.get()-lastGetFailover) + "," + 
        (getListPrimary.get()) + "," +
        (getListPrimary.get()-lastGetListPrimary) + "," +
        (getListFailover.get()) + "," +
        (getListFailover.get()-lastGetListFailover) + "," +
        (deletePrimary.get()) + "," + 
        (deletePrimary.get()-lastDeletePrimary) + "," + 
        (deleteFailover.get()) + "," +
        (deleteFailover.get()-lastDeleteFailover) + "," +
        (deleteListPrimary.get()) + "," +
        (deleteListPrimary.get()-lastDeleteListPrimary) + "," +
        (deleteListFailover.get()) + "," +
        (deleteListFailover.get()-lastDeleteListFailover) +
        newLine);
    
    lastPutPrimary = putPrimary.get();
    lastPutFailover = putFailover.get();
    lastPutListPrimary = putListPrimary.get();
    lastPutListFailover = putListFailover.get();
    lastGetPrimary = getPrimary.get();
    lastGetFailover = getFailover.get();
    lastGetListPrimary = getListPrimary.get();
    lastGetListFailover = getListFailover.get();
    lastDeletePrimary = deletePrimary.get();
    lastDeleteFailover = deleteFailover.get();
    lastDeleteListPrimary = deleteListPrimary.get();
    lastDeleteListFailover = deleteListFailover.get();
    
  }
  
  public static long getRollingAvg(BlockingQueue<Long> queue) {
    Long[] times = queue.toArray(new Long[0]);
    
    long totalTime = 0;
    long totalCount = 0;
    
    for (Long time: times) {
      if (time > -1) {
        totalCount++;
        totalTime += time;
      }
    }
    if (totalCount != 0) {
      return totalTime/totalCount;
    } else {
      return 0;
    }
  }
  
  public static long getRollingMax(BlockingQueue<Long> queue) {
    Long[] times = queue.toArray(new Long[0]);
    
    long maxTime = 0;
    
    for (Long time: times) {
      if (time > maxTime) {
        maxTime = time;
      }
    }
    
    return maxTime;
    
  }
  
  public void addPut(Boolean isPrimary, long time) {
    updateStat(isPrimary, time, maxPutTime, putPrimary, putFailover, putRollingAverage);
  }
  
  public void addPutList(Boolean isPrimary, long time) {
    updateStat(isPrimary, time, maxPutListTime, putListPrimary, putListFailover, putListRollingAverage);
  }
  
  public void addGet(Boolean isPrimary, long time) {
    updateStat(isPrimary, time, maxGetTime, getPrimary, getFailover, getRollingAverage);
  }
  
  public void addGetList(Boolean isPrimary, long time) {
    updateStat(isPrimary, time, maxGetListTime, getListPrimary, getListFailover, getListRollingAverage);
  }

  public void addDelete(Boolean isPrimary, long time) {
    updateStat(isPrimary, time, maxDeleteTime, deletePrimary, deleteFailover, deleteRollingAverage);
  }
  
  public void addDeleteList(Boolean isPrimary, long time) {
    updateStat(isPrimary, time, maxDeleteListTime, deleteListPrimary, deleteListFailover, deleteListRollingAverage);
  }
  
  private static void updateStat(Boolean isPrimary, 
      long time, 
      AtomicLong maxTime, 
      AtomicLong primaryCount, 
      AtomicLong failoverCount, 
      BlockingQueue<Long> rollingAverage) {
    
    long max = maxTime.get();
    while (time > max) {
      if (!maxTime.compareAndSet(max, time)) {
        max = maxTime.get();
      } else {
        break;
      }
    }
    
    if (isPrimary != null) {
      if (isPrimary == true) {
        primaryCount.addAndGet(1);
      } else {
        failoverCount.addAndGet(1);
      }
    }
    
    rollingAverage.add(time);
    rollingAverage.poll();
  }
}
