package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SpeculativeRequester.RequestFunction;
import org.apache.hadoop.hbase.client.SpeculativeRequester.ResultWrapper;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class HTableMultiCluster implements HTableInterface {

  HTableInterface primaryHTable;
  Collection<HTableInterface> failoverHTables;
  Configuration originalConfiguration;
  boolean isMasterMaster;
  int waitTimeBeforeAcceptingResults;
  int waitTimeBeforeRequestingFailover;
  int waitTimeBeforeMutatingFailover;
  int waitTimeBeforeMutatingFailoverWithPrimaryException;
  int waitTimeBeforeAcceptingBatchResults;
  int waitTimeBeforeRequestingBatchFailover;
  int waitTimeBeforeMutatingBatchFailover;
  

  HTableStats stats = new HTableStats();


  static final Log LOG = LogFactory.getLog(HTableMultiCluster.class);

  public HTableStats getStats() {
    return stats;
  }

  public HTableMultiCluster(Configuration originalConfiguration,
      HTableInterface primaryHTable,
      Collection<HTableInterface> failoverHTables, boolean isMasterMaster,
      int waitTimeBeforeAcceptingResults, int waitTimeBeforeRequestingFailover,
      int waitTimeBeforeMutatingFailover,
      int waitTimeBeforeMutatingFailoverWithPrimaryException,
      int waitTimeBeforeAcceptingBatchResults,
      int waitTimeBeforeRequestingBatchFailover,
      int waitTimeBeforeMutatingBatchFailover) {

    this.primaryHTable = primaryHTable;
    this.failoverHTables = failoverHTables;
    this.isMasterMaster = isMasterMaster;
    this.waitTimeBeforeAcceptingResults = waitTimeBeforeAcceptingResults;
    this.waitTimeBeforeRequestingFailover = waitTimeBeforeRequestingFailover;
    this.waitTimeBeforeMutatingFailover = waitTimeBeforeMutatingFailover;
    this.waitTimeBeforeMutatingFailoverWithPrimaryException = waitTimeBeforeMutatingFailoverWithPrimaryException;
    this.waitTimeBeforeAcceptingBatchResults = waitTimeBeforeAcceptingBatchResults;
    this.waitTimeBeforeRequestingBatchFailover = waitTimeBeforeRequestingBatchFailover;
    this.waitTimeBeforeMutatingBatchFailover = waitTimeBeforeMutatingBatchFailover;

    this.originalConfiguration = originalConfiguration;
  }

  public byte[] getTableName() {
    return primaryHTable.getTableName();
  }

  public TableName getName() {
    return primaryHTable.getName();
  }

  public Configuration getConfiguration() {
    return originalConfiguration;
  }

  public HTableDescriptor getTableDescriptor() throws IOException {
    return primaryHTable.getTableDescriptor();
  }

  public boolean exists(final Get get) throws IOException {

    long startTime = System.currentTimeMillis();

    RequestFunction<Boolean> function = new RequestFunction<Boolean>() {
      @Override
      public Boolean call(HTableInterface table) throws Exception{
        return table.exists(get);
      }
    };
    
    ResultWrapper<Boolean> result = (new SpeculativeRequester<Boolean>(
        waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail)).
        request(function, primaryHTable, failoverHTables);

    stats.addGet(result.isPrimary, System.currentTimeMillis() - startTime);

    return result.t;
  }

  public Boolean[] exists(final List<Get> gets) throws IOException {

    long startTime = System.currentTimeMillis();

    RequestFunction<Boolean[]> function = new RequestFunction<Boolean[]>() {
      @Override
      public Boolean[] call(HTableInterface table) throws Exception{
        return table.exists(gets);
      }
    };
    
    ResultWrapper<Boolean[]> result = (new SpeculativeRequester<Boolean[]>(
        waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail)).
        request(function, primaryHTable, failoverHTables);

    stats.addGetList(result.isPrimary, System.currentTimeMillis() - startTime);

    return result.t;
  }

  public void batch(final List<? extends Row> actions, final Object[] results)
      throws IOException, InterruptedException {
    // TODO
  }

  public Object[] batch(final List<? extends Row> actions) throws IOException,
      InterruptedException {
    // TODO
    return null;
  }

  public <R> void batchCallback(List<? extends Row> actions, Object[] results,
      Callback<R> callback) throws IOException, InterruptedException {
    // TODO
  }

  public <R> Object[] batchCallback(List<? extends Row> actions,
      Callback<R> callback) throws IOException, InterruptedException {
    // TODO
    return null;
  }

  public Result get(final Get get) throws IOException {

    long ts = System.currentTimeMillis();
    
    RequestFunction<Result> function = new RequestFunction<Result>() {
      @Override
      public Result call(HTableInterface table) throws Exception{
        return table.get(get);
      }
    };
     
    ResultWrapper<Result> result = (new SpeculativeRequester<Result>(
        waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail)).
        request(function, primaryHTable, failoverHTables);

    stats.addGet(result.isPrimary, System.currentTimeMillis() - ts);

    return result.t;
  }

  public Result[] get(final List<Get> gets) throws IOException {
    long ts = System.currentTimeMillis();

    RequestFunction<Result[]> function = new RequestFunction<Result[]>() {
      @Override
      public Result[] call(HTableInterface table) throws Exception{
        return table.get(gets);
      }
    };
     
    ResultWrapper<Result[]> result = (new SpeculativeRequester<Result[]>(
        waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail)).
        request(function, primaryHTable, failoverHTables);

    
    stats.addGetList(result.isPrimary, System.currentTimeMillis() - ts);

    return result.t;

  }

  @Deprecated
  public Result getRowOrBefore(final byte[] row, final byte[] family)
      throws IOException {

    long ts = System.currentTimeMillis();

    RequestFunction<Result> function = new RequestFunction<Result>() {
      @Override
      public Result call(HTableInterface table) throws Exception{
        return table.getRowOrBefore(row, family);
      }
    };
     
    ResultWrapper<Result> result = (new SpeculativeRequester<Result>(
        waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail)).
        request(function, primaryHTable, failoverHTables);

    stats.addGet(result.isPrimary, System.currentTimeMillis() - ts);

    return result.t;
  }

  public ResultScanner getScanner(final Scan scan) throws IOException {

    long ts = System.currentTimeMillis();

    RequestFunction<ResultScanner> function = new RequestFunction<ResultScanner>() {
      @Override
      public ResultScanner call(HTableInterface table) throws Exception{
        return table.getScanner(scan);
      }
    };
     
    ResultWrapper<ResultScanner> result = (new SpeculativeRequester<ResultScanner>(
        waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail)).
        request(function, primaryHTable, failoverHTables);

    // need to add a scanner
    stats.addGet(result.isPrimary, System.currentTimeMillis() - ts);

    return result.t;
  }

  public ResultScanner getScanner(final byte[] family) throws IOException {

    long ts = System.currentTimeMillis();

    RequestFunction<ResultScanner> function = new RequestFunction<ResultScanner>() {
      @Override
      public ResultScanner call(HTableInterface table) throws Exception{
        return table.getScanner(family);
      }
    };
     
    ResultWrapper<ResultScanner> result = (new SpeculativeRequester<ResultScanner>(
        waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail)).
        request(function, primaryHTable, failoverHTables);

    // need to add a scanner
    stats.addGet(result.isPrimary, System.currentTimeMillis() - ts);

    return result.t;
  }

  public ResultScanner getScanner(final byte[] family, final byte[] qualifier)
      throws IOException {

    long ts = System.currentTimeMillis();

    RequestFunction<ResultScanner> function = new RequestFunction<ResultScanner>() {
      @Override
      public ResultScanner call(HTableInterface table) throws Exception{
        return table.getScanner(family, qualifier);
      }
    };
     
    ResultWrapper<ResultScanner> result = (new SpeculativeRequester<ResultScanner>(
        waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail)).
        request(function, primaryHTable, failoverHTables);

    // need to add a scanner
    stats.addGet(result.isPrimary, System.currentTimeMillis() - ts);

    return result.t;
  }

  AtomicLong lastPrimaryFail = new AtomicLong(0);

  public void put(final Put put) throws IOException {

    long ts = System.currentTimeMillis();

    final Put newPut = setTimeStampOfUnsetValues(put, ts);


    Callable<Void> primaryCallable = new Callable<Void>() {
      public Void call() throws Exception {
        try {
          primaryHTable.put(newPut);
          return null;
        } catch (java.io.InterruptedIOException e) {
          //throw e;
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          lastPrimaryFail.set(System.currentTimeMillis());
          Thread.currentThread().interrupt();
          //e.printStackTrace();
          //throw e;
        }
        return null;
      }
    };

    ArrayList<Callable<Void>> callables = new ArrayList<Callable<Void>>();
    for (final HTableInterface failoverTable : failoverHTables) {
      callables.add(new Callable<Void>() {
        public Void call() throws Exception {
          failoverTable.put(newPut);
          return null;
        }
      });
    }

    Boolean isPrimary = SpeculativeMutater.mutate(
        waitTimeBeforeAcceptingBatchResults,
        waitTimeBeforeMutatingFailoverWithPrimaryException, 
        primaryCallable, callables, lastPrimaryFail);


    long time = System.currentTimeMillis() - ts;
    
    stats.addPut(isPrimary, time);
  }

  private Put setTimeStampOfUnsetValues(final Put put, long ts)
      throws IOException {
    final Put newPut = new Put(put.row);
    for (Entry<byte[], List<Cell>> entity : put.getFamilyCellMap().entrySet()) {
      for (Cell cell : entity.getValue()) {
        // If no timestamp was given then use now.
        // This will protect us from a multicluster sumbission
        if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
          newPut
              .add(cell.getFamily(), cell.getQualifier(), ts, cell.getValue());
        } else {
          newPut.add(cell);
        }
      }
    }
    return newPut;
  }

  public void put(final List<Put> puts) throws IOException {

    long ts = System.currentTimeMillis();

    final List<Put> newPuts = new ArrayList<Put>();
    for (Put put : puts) {
      newPuts.add(setTimeStampOfUnsetValues(put, ts));
    }

    Callable<Void> primaryCallable = new Callable<Void>() {
      public Void call() throws Exception {
        try {
          primaryHTable.put(newPuts);
          return null;
        } catch (java.io.InterruptedIOException e) {
          throw e;
        } catch (Exception e) {
          lastPrimaryFail.set(System.currentTimeMillis());
          e.printStackTrace();
          throw e;
        }
      }
    };

    ArrayList<Callable<Void>> callables = new ArrayList<Callable<Void>>();
    for (final HTableInterface failoverTable : failoverHTables) {
      callables.add(new Callable<Void>() {
        public Void call() throws Exception {
          failoverTable.put(newPuts);
          return null;
        }
      });
    }

    Boolean isPrimary = SpeculativeMutater.mutate(
        waitTimeBeforeAcceptingBatchResults,
        waitTimeBeforeMutatingFailoverWithPrimaryException, 
        primaryCallable, callables, lastPrimaryFail);
    stats.addPutList(isPrimary, System.currentTimeMillis() - ts);
  }

  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException {
    return primaryHTable.checkAndPut(row, family, qualifier, value, put);
  }

  public void delete(final Delete delete) throws IOException {
    long ts = System.currentTimeMillis();

    Callable<Void> primaryCallable = new Callable<Void>() {
      public Void call() throws Exception {

        try {
          primaryHTable.delete(delete);
          return null;
        } catch (java.io.InterruptedIOException e) {
          Thread.currentThread().interrupt();
          //throw e;
        } catch (Exception e) {
          lastPrimaryFail.set(System.currentTimeMillis());
          //e.printStackTrace();
          Thread.currentThread().interrupt();
          //throw e;
        }
        return null;
      }
    };

    ArrayList<Callable<Void>> callables = new ArrayList<Callable<Void>>();
    for (final HTableInterface failoverTable : failoverHTables) {
      callables.add(new Callable<Void>() {
        public Void call() throws Exception {
          failoverTable.delete(delete);
          return null;
        }
      });
    }

    Boolean isPrimary = SpeculativeMutater.mutate(
        waitTimeBeforeAcceptingBatchResults,
        waitTimeBeforeMutatingFailoverWithPrimaryException, 
        primaryCallable, callables, lastPrimaryFail);

    stats.addDelete(isPrimary, System.currentTimeMillis() - ts);
  }

  public void delete(final List<Delete> deletes) throws IOException {
    long ts = System.currentTimeMillis();

    Callable<Void> primaryCallable = new Callable<Void>() {
      public Void call() throws Exception {
        try {
          primaryHTable.delete(deletes);
          return null;
        } catch (java.io.InterruptedIOException e) {
          throw e;
        } catch (Exception e) {
          lastPrimaryFail.set(System.currentTimeMillis());
          //e.printStackTrace();
          throw e;
        }
      }
    };

    ArrayList<Callable<Void>> callables = new ArrayList<Callable<Void>>();
    for (final HTableInterface failoverTable : failoverHTables) {
      callables.add(new Callable<Void>() {
        public Void call() throws Exception {
          failoverTable.delete(deletes);
          return null;
        }
      });
    }

    Boolean isPrimary = SpeculativeMutater.mutate(
        waitTimeBeforeAcceptingBatchResults,
        waitTimeBeforeMutatingFailoverWithPrimaryException, 
        primaryCallable, callables, lastPrimaryFail);

    stats.addDeleteList(isPrimary, System.currentTimeMillis() - ts);
  }

  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException {
    return primaryHTable.checkAndDelete(row, family, qualifier, value, delete);
  }

  public void mutateRow(final RowMutations rm) throws IOException {

    primaryHTable.mutateRow(rm);
  }

  public Result append(Append append) throws IOException {
    return primaryHTable.append(append);
  }

  public Result increment(Increment increment) throws IOException {
    return primaryHTable.increment(increment);
  }

  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount) throws IOException {
    return primaryHTable.incrementColumnValue(row, family, qualifier, amount);
  }

  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, Durability durability) throws IOException {
    return primaryHTable.incrementColumnValue(row, family, qualifier, amount,
        durability);
  }

  @Deprecated
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, boolean writeToWAL) throws IOException {
    return primaryHTable.incrementColumnValue(row, family, qualifier, amount,
        writeToWAL);
  }

  public boolean isAutoFlush() {

    boolean primaryAnswer = primaryHTable.isAutoFlush();

    return primaryAnswer;
  }

  public void flushCommits() throws IOException {

    Exception lastException = null;
    try {
      primaryHTable.flushCommits();
    } catch (Exception e) {
      LOG.error("Exception while flushCommits primary", e);
      lastException = e;
    }
    for (final HTableInterface failoverTable : failoverHTables) {
      try {
        failoverTable.flushCommits();
      } catch (Exception e) {
        LOG.error("Exception while flushCommitsy failover", e);
        lastException = e;
      }
    }
    if (lastException != null) {
      throw new IOException(lastException);
    }

  }

  public void close() throws IOException {
    Exception lastException = null;
    try {
      primaryHTable.close();
    } catch (Exception e) {
      LOG.error("Exception while flushCommits primary", e);
      lastException = e;
    }
    for (final HTableInterface failoverTable : failoverHTables) {
      try {
        failoverTable.close();
      } catch (Exception e) {
        LOG.error("Exception while flushCommitsy failover", e);
        lastException = e;
      }
    }
    if (lastException != null) {
      throw new IOException(lastException);
    }

  }

  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    // TODO Auto-generated method stub
    return null;
  }

  public <T extends Service, R> Map<byte[], R> coprocessorService(
      Class<T> service, byte[] startKey, byte[] endKey, Call<T, R> callable)
      throws ServiceException, Throwable {
    // TODO Auto-generated method stub
    return null;
  }

  public <T extends Service, R> void coprocessorService(Class<T> service,
      byte[] startKey, byte[] endKey, Call<T, R> callable, Callback<R> callback)
      throws ServiceException, Throwable {
    // TODO Auto-generated method stub

  }

  @Deprecated
  public void setAutoFlush(boolean autoFlush) {
    // TODO Auto-generated method stub

  }

  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    // TODO Auto-generated method stub

  }

  public void setAutoFlushTo(boolean autoFlush) {
    // TODO Auto-generated method stub

  }

  public long getWriteBufferSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    // TODO Auto-generated method stub

  }

  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      MethodDescriptor methodDescriptor, Message request, byte[] startKey,
      byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
    // TODO Auto-generated method stub
    return null;
  }

  public <R extends Message> void batchCoprocessorService(
      MethodDescriptor methodDescriptor, Message request, byte[] startKey,
      byte[] endKey, R responsePrototype, Callback<R> callback)
      throws ServiceException, Throwable {
    // TODO Auto-generated method stub

  }

}
