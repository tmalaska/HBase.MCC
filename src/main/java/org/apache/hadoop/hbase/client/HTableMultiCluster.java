package org.apache.hadoop.hbase.client;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

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
  int waitTimeFromLastPrimaryFail;

  AtomicLong lastPrimaryFail = new AtomicLong(0);
  HTableStats stats = new HTableStats();

  ArrayList<Put> bufferPutList = new ArrayList<Put>();
  boolean autoFlush = true;
  protected long currentWriteBufferSize;
  private long writeBufferSize;


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
                            int waitTimeBeforeMutatingBatchFailover,
                            int waitTimeFromLastPrimaryFail) {

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
    this.waitTimeFromLastPrimaryFail = waitTimeFromLastPrimaryFail;

    this.writeBufferSize = originalConfiguration.getLong("hbase.client.write.buffer", 2097152L);

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

    return multiClusterExists(get).getOriginalReturn();
  }

  public Tuple<Boolean> multiClusterExists(final Get get) throws IOException {
    long startTime = System.currentTimeMillis();

    HBaseTableFunction<Boolean> function = new HBaseTableFunction<Boolean>() {
      @Override
      public Boolean call(HTableInterface table) throws Exception {
        return table.exists(get);
      }
    };

    SpeculativeRequester.ResultWrapper<Boolean> result = (new SpeculativeRequester<Boolean>(
            waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail,
            waitTimeFromLastPrimaryFail)).
            request(function, primaryHTable, failoverHTables);

    stats.addGet(result.isPrimary, System.currentTimeMillis() - startTime);

    boolean doesExist = result.t;
    return new Tuple<Boolean>(result.isPrimary, doesExist);
  }

  public Boolean[] exists(final List<Get> gets) throws IOException {
    return multiClusterExists(gets).getOriginalReturn();
  }

  public Tuple<Boolean[]> multiClusterExists(final List<Get> gets) throws IOException {
    long startTime = System.currentTimeMillis();

    HBaseTableFunction<Boolean[]> function = new HBaseTableFunction<Boolean[]>() {
      @Override
      public Boolean[] call(HTableInterface table) throws Exception {
        return table.exists(gets);
      }
    };

    SpeculativeRequester.ResultWrapper<Boolean[]> result = (new SpeculativeRequester<Boolean[]>(
            waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail,
            waitTimeFromLastPrimaryFail)).
            request(function, primaryHTable, failoverHTables);

    stats.addGetList(result.isPrimary, System.currentTimeMillis() - startTime);

    Boolean[] doesExists = result.t;

    return new Tuple<Boolean[]>(result.isPrimary, doesExists);
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
    return multiClusterGet(get).originalReturn;
  }

  public Tuple<Result> multiClusterGet(final Get get) throws IOException {
    long ts = System.currentTimeMillis();

    HBaseTableFunction<Result> function = new HBaseTableFunction<Result>() {
      @Override
      public Result call(HTableInterface table) throws Exception {
        return table.get(get);
      }
    };

    SpeculativeRequester.ResultWrapper<Result> result = (new SpeculativeRequester<Result>(
            waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail,
            waitTimeFromLastPrimaryFail)).
            request(function, primaryHTable, failoverHTables);

    stats.addGet(result.isPrimary, System.currentTimeMillis() - ts);

    Result returnResults = result.t;
    return new Tuple<Result>(result.isPrimary, returnResults);
  }

  public Result[] get(final List<Get> gets) throws IOException {
    return multiClusterGet(gets).getOriginalReturn();
  }

  public Tuple<Result[]> multiClusterGet(final List<Get> gets) throws IOException {
    long ts = System.currentTimeMillis();

    HBaseTableFunction<Result[]> function = new HBaseTableFunction<Result[]>() {
      @Override
      public Result[] call(HTableInterface table) throws Exception {
        return table.get(gets);
      }
    };

    SpeculativeRequester.ResultWrapper<Result[]> result = (new SpeculativeRequester<Result[]>(
            waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail,
            waitTimeFromLastPrimaryFail)).
            request(function, primaryHTable, failoverHTables);


    stats.addGetList(result.isPrimary, System.currentTimeMillis() - ts);

    Result[] returnResults = result.t;
    return new Tuple<Result[]>(result.isPrimary, returnResults);
  }

  @Deprecated
  public Result getRowOrBefore(final byte[] row, final byte[] family)
          throws IOException {
    return multiClusterGetRowOrBefore(row, family).getOriginalReturn();
  }

  public Tuple<Result> multiClusterGetRowOrBefore(final byte[] row, final byte[] family) {
    long ts = System.currentTimeMillis();

    HBaseTableFunction<Result> function = new HBaseTableFunction<Result>() {
      @Override
      public Result call(HTableInterface table) throws Exception {
        return table.getRowOrBefore(row, family);
      }
    };

    SpeculativeRequester.ResultWrapper<Result> result = (new SpeculativeRequester<Result>(
            waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail,
            waitTimeFromLastPrimaryFail)).
            request(function, primaryHTable, failoverHTables);

    stats.addGet(result.isPrimary, System.currentTimeMillis() - ts);

    Result returnResult = result.t;
    return new Tuple<Result>(result.isPrimary, returnResult);
  }


  public ResultScanner getScanner(final Scan scan) throws IOException {
    return multiClusterGetScanner(scan).getOriginalReturn();
  }

  public Tuple<ResultScanner> multiClusterGetScanner(final Scan scan) throws IOException {
    long ts = System.currentTimeMillis();

    HBaseTableFunction<ResultScanner> function = new HBaseTableFunction<ResultScanner>() {
      @Override
      public ResultScanner call(HTableInterface table) throws Exception {
        return table.getScanner(scan);
      }
    };

    SpeculativeRequester.ResultWrapper<ResultScanner> result = (new SpeculativeRequester<ResultScanner>(
            waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail,
            waitTimeFromLastPrimaryFail)).
            request(function, primaryHTable, failoverHTables);

    // need to add a scanner
    stats.addGet(result.isPrimary, System.currentTimeMillis() - ts);

    ResultScanner resultScanner = result.t;
    return new Tuple<ResultScanner>(result.isPrimary, resultScanner);
  }

  public ResultScanner getScanner(final byte[] family) throws IOException {
    return multiClusterGetScanner(family).getOriginalReturn();
  }

  public Tuple<ResultScanner> multiClusterGetScanner(final byte[] family) throws IOException {
    long ts = System.currentTimeMillis();

    HBaseTableFunction<ResultScanner> function = new HBaseTableFunction<ResultScanner>() {
      @Override
      public ResultScanner call(HTableInterface table) throws Exception {
        return table.getScanner(family);
      }
    };

    SpeculativeRequester.ResultWrapper<ResultScanner> result = (new SpeculativeRequester<ResultScanner>(
            waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail,
            waitTimeFromLastPrimaryFail)).
            request(function, primaryHTable, failoverHTables);

    // need to add a scanner
    stats.addGet(result.isPrimary, System.currentTimeMillis() - ts);

    return new Tuple(result.isPrimary, result.t);
  }

  public ResultScanner getScanner(final byte[] family, final byte[] qualifier)
          throws IOException {
    return multiClusterGetScanner(family, qualifier).getOriginalReturn();
  }

  public Tuple<ResultScanner> multiClusterGetScanner(final byte[] family, final byte[] qualifier)
          throws IOException {
    long ts = System.currentTimeMillis();

    HBaseTableFunction<ResultScanner> function = new HBaseTableFunction<ResultScanner>() {
      @Override
      public ResultScanner call(HTableInterface table) throws Exception {
        return table.getScanner(family, qualifier);
      }
    };

    SpeculativeRequester.ResultWrapper<ResultScanner> result = (new SpeculativeRequester<ResultScanner>(
            waitTimeBeforeRequestingFailover, waitTimeBeforeAcceptingResults, lastPrimaryFail,
            waitTimeFromLastPrimaryFail)).
            request(function, primaryHTable, failoverHTables);

    // need to add a scanner
    stats.addGet(result.isPrimary, System.currentTimeMillis() - ts);

    return new Tuple(result.isPrimary, result.t);
  }


  public void put(final Put put) throws IOException {
    multiClusterPut(put);
  }



  public Boolean multiClusterPut(final Put put) throws IOException {
    if (autoFlush) {
      return autoFlushMutliClusterPut(put);
    } else {
      bufferPutList.add(put);
      currentWriteBufferSize += put.heapSize();
      if (currentWriteBufferSize > writeBufferSize) {
        Boolean isPrimary = autoFlushMutliClusterPut(bufferPutList);
        bufferPutList.clear();
        currentWriteBufferSize = 0l;
        return isPrimary;
      }
      return true;
    }
  }

  private Boolean autoFlushMutliClusterPut(final Put put) throws IOException {
    long ts = System.currentTimeMillis();

    final Put newPut = setTimeStampOfUnsetValues(put, ts);

    HBaseTableFunction<Void> function = new HBaseTableFunction<Void>() {
      @Override
      public Void call(HTableInterface table) throws Exception {
        synchronized (table) {
          System.out.println("table.put.start:" + table.getConfiguration().get("hbase.zookeeper.quorum") + " " + table.getName() + " " + Bytes.toString(newPut.getRow()));
          try {
            table.put(newPut);
          } catch (Exception e) {
            System.out.println("table.put.exception:" + table.getName() + " " + Bytes.toString(newPut.getRow()));
            throw new RuntimeException(e);
          }
          System.out.println("table.put.done:" + table);
        }
        return null;
      }
    };

    Boolean isPrimary = SpeculativeMutater.mutate(
            waitTimeBeforeAcceptingBatchResults,
            waitTimeBeforeMutatingFailoverWithPrimaryException,
            function, primaryHTable, failoverHTables, lastPrimaryFail,
            waitTimeFromLastPrimaryFail);

    long time = System.currentTimeMillis() - ts;



    stats.addPut(isPrimary, time);
    return isPrimary;
  }

  private Put setTimeStampOfUnsetValues(final Put put, long ts)
          throws IOException {
    final Put newPut = new Put(put.getRow());
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
    multiClustPut(puts);
  }

  public Boolean multiClustPut(final List<Put> puts) throws IOException {
    if (autoFlush) {
      return autoFlushMutliClusterPut(puts);
    } else {
      bufferPutList.addAll(puts);
      for (int i = 0; i < puts.size(); i++) {
        currentWriteBufferSize += puts.get(0).heapSize();
      }

      if (currentWriteBufferSize > writeBufferSize) {
        Boolean isPrimary = autoFlushMutliClusterPut(bufferPutList);
        bufferPutList.clear();
        currentWriteBufferSize = 0l;
        return isPrimary;
      }
      return true;
    }
  }

  public Boolean autoFlushMutliClusterPut(final List<Put> puts) throws IOException {
    long ts = System.currentTimeMillis();

    final List<Put> newPuts = new ArrayList<Put>();
    for (Put put : puts) {
      newPuts.add(setTimeStampOfUnsetValues(put, ts));
    }

    HBaseTableFunction<Void> function = new HBaseTableFunction<Void>() {
      @Override
      public Void call(HTableInterface table) throws Exception {
        table.put(newPuts);
        return null;
      }
    };

    Boolean isPrimary = SpeculativeMutater.mutate(
            waitTimeBeforeAcceptingBatchResults,
            waitTimeBeforeMutatingFailoverWithPrimaryException,
            function, primaryHTable, failoverHTables, lastPrimaryFail,
            waitTimeFromLastPrimaryFail);

    stats.addPutList(isPrimary, System.currentTimeMillis() - ts);
    return isPrimary;
  }


  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
                             byte[] value, Put put) throws IOException {
    return primaryHTable.checkAndPut(row, family, qualifier, value, put);
  }

  public void delete(final Delete delete) throws IOException {
    multiClusterDelete(delete);
  }

  public Boolean multiClusterDelete(final Delete delete) throws IOException {
    long ts = System.currentTimeMillis();

    HBaseTableFunction<Void> function = new HBaseTableFunction<Void>() {
      @Override
      public Void call(HTableInterface table) throws Exception {
        table.delete(delete);
        return null;
      }
    };

    Boolean isPrimary = SpeculativeMutater.mutate(
            waitTimeBeforeAcceptingBatchResults,
            waitTimeBeforeMutatingFailoverWithPrimaryException,
            function, primaryHTable, failoverHTables, lastPrimaryFail,
            waitTimeFromLastPrimaryFail);

    stats.addDelete(isPrimary, System.currentTimeMillis() - ts);
    return isPrimary;
  }

  public void delete(final List<Delete> deletes) throws IOException {
    multiClusterDelete(deletes);
  }

  public Boolean multiClusterDelete(final List<Delete> deletes) throws IOException {
    long ts = System.currentTimeMillis();

    HBaseTableFunction<Void> function = new HBaseTableFunction<Void>() {
      @Override
      public Void call(HTableInterface table) throws Exception {
        table.delete(deletes);
        return null;
      }
    };

    Boolean isPrimary = SpeculativeMutater.mutate(
            waitTimeBeforeAcceptingBatchResults,
            waitTimeBeforeMutatingFailoverWithPrimaryException,
            function, primaryHTable, failoverHTables, lastPrimaryFail,
            waitTimeFromLastPrimaryFail);

    stats.addDeleteList(isPrimary, System.currentTimeMillis() - ts);
    return isPrimary;
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
    if (bufferPutList.size() > 0) {
      autoFlushMutliClusterPut(bufferPutList);
      bufferPutList.clear();
      currentWriteBufferSize = 0l;
    }

  }

  public void close() throws IOException {
    if (bufferPutList.size() > 0) {
      autoFlushMutliClusterPut(bufferPutList);
      bufferPutList.clear();
      currentWriteBufferSize = 0l;
    }

    Exception lastException = null;
    try {
      synchronized (primaryHTable) {
        primaryHTable.close();
      }
    } catch (Exception e) {
      LOG.error("Exception while flushCommits primary", e);
      lastException = e;
    }
    for (final HTableInterface failoverTable : failoverHTables) {
      try {
        synchronized (failoverTable) {
          failoverTable.close();
        }
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
                                                        byte[] startKey, byte[] endKey,Call<T, R> callable, Callback<R> callback)
          throws ServiceException, Throwable {
    // TODO Auto-generated method stub
  }

  @Deprecated
  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
    if (autoFlush == true && bufferPutList.size() > 0) {
      try {
        autoFlushMutliClusterPut(bufferPutList);
        bufferPutList.clear();
        currentWriteBufferSize = 0l;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

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

  @Override
  public boolean checkAndMutate(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, byte[] bytes3, RowMutations rowMutations) throws IOException {
    return primaryHTable.checkAndMutate(bytes, bytes1, bytes2, compareOp, bytes3, rowMutations);
  }

  public static class Tuple<T extends Object> {
    private Boolean isPrimary;
    private T originalReturn;

    public Tuple(Boolean isPrimary, T originalReturn) {
      this.isPrimary = isPrimary;
      this.originalReturn = originalReturn;
    }

    public Boolean getIsPrimary() {
      return isPrimary;
    }

    public T getOriginalReturn() {
      return originalReturn;
    }
  }

}
