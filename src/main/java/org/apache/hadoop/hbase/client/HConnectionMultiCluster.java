package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.MasterKeepAliveConnection;
import org.apache.hadoop.hbase.client.NonceGenerator;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService.BlockingInterface;
import org.apache.hadoop.hbase.util.Bytes;

public class HConnectionMultiCluster implements HConnection {

  HConnection primaryConnection;
  List<HConnection> failoverConnections;
  Configuration originalConfiguration;
  boolean isMasterMaster;
  int waitTimeBeforeAcceptingResults;
  int waitTimeBeforeRequestingFailover;
  int waitTimeBeforeMutatingFailover;
  int waitTimeBeforeMutatingFailoverWithPrimaryException;
  int waitTimeBeforeAcceptingBatchResults;
  int waitTimeBeforeRequestingBatchFailover;
  int waitTimeBeforeMutatingBatchFailover;
  
  //private volatile ExecutorService batchPool = null;
  //private volatile boolean cleanupPool = false;

  static final Log LOG = LogFactory.getLog(HConnectionMultiCluster.class);
  
  ExecutorService executor; // = Executors.newFixedThreadPool(failoverHTables.size() + 1);

  public HConnectionMultiCluster(Configuration originalConfiguration,
      HConnection primaryConnection, List<HConnection> failoverConnections) {
    this.primaryConnection = primaryConnection;
    this.failoverConnections = failoverConnections;
    this.originalConfiguration = originalConfiguration;
    this.isMasterMaster = originalConfiguration
        .getBoolean(
            ConfigConst.HBASE_FAILOVER_MODE_CONFIG,
            false);
    this.waitTimeBeforeAcceptingResults = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_ACCEPTING_FAILOVER_RESULT_CONFIG,
            20);
    this.waitTimeBeforeMutatingFailover = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_MUTATING_FAILOVER_CONFIG,
            20);
    this.waitTimeBeforeMutatingFailoverWithPrimaryException = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_MUTATING_FAILOVER_WITH_PRIMARY_EXCEPTION_CONFIG,
            0);
    this.waitTimeBeforeRequestingFailover = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_REQUEST_FAILOVER_CONFIG,
            20);
    this.waitTimeBeforeAcceptingBatchResults = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_ACCEPTING_FAILOVER_BATCH_RESULT_CONFIG,
            20);
    this.waitTimeBeforeRequestingBatchFailover = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_MUTATING_BATCH_FAILOVER_CONFIG,
            20);
    this.waitTimeBeforeMutatingBatchFailover = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_REQUEST_BATCH_FAILOVER_CONFIG,
            20);
    executor = Executors.newFixedThreadPool(originalConfiguration.getInt(ConfigConst.HBASE_MULTI_CLUSTER_CONNECTION_POOL_SIZE, 20));
  }

  public void abort(String why, Throwable e) {
    primaryConnection.abort(why, e);
    for (HConnection failOverConnection : failoverConnections) {
      failOverConnection.abort(why, e);
    }
  }

  public boolean isAborted() {
    return primaryConnection.isAborted();
  }

  public void close() throws IOException {

    Exception lastException = null;
    try {
      primaryConnection.close();
    } catch (Exception e) {
      LOG.error("Exception while closing primary", e);
      lastException = e;
    }
    for (HConnection failOverConnection : failoverConnections) {
      try {
        failOverConnection.close();
      } catch (Exception e) {
        LOG.error("Exception while closing primary", e);
        lastException = e;
      }
    }
    if (lastException != null) {
      throw new IOException(lastException);
    }
  }

  public Configuration getConfiguration() {
    return originalConfiguration;
  }

  @Override
  public HTableInterface getTable(String tableName) throws IOException {
    System.out.println(" -- foo1");
    return this.getTable(Bytes.toBytes(tableName));
  }

  @Override
  public HTableInterface getTable(byte[] tableName) throws IOException {
    System.out.println(" -- foo2");
    return this.getTable(TableName.valueOf(tableName));
  }

  @Override
  public HTableInterface getTable(TableName tableName) throws IOException {
    System.out.println(" -- foo3");
    
    
    System.out.println(" -- getting primaryHTable" + primaryConnection.getConfiguration().get("hbase.zookeeper.quorum"));
    HTableInterface primaryHTable = primaryConnection.getTable(tableName);
    System.out.println(" --- got primaryHTable");
    ArrayList<HTableInterface> failoverHTables = new ArrayList<HTableInterface>();
    for (HConnection failOverConnection : failoverConnections) {
      System.out.println(" -- getting failoverHTable:" + failOverConnection.getConfiguration().get("hbase.zookeeper.quorum"));
      
      failoverHTables.add(failOverConnection.getTable(tableName));
      System.out.println(" --- got failoverHTable");
    }

    return new HTableMultiCluster(originalConfiguration, primaryHTable,
        failoverHTables, isMasterMaster, 
        waitTimeBeforeAcceptingResults,
        waitTimeBeforeRequestingFailover,
        waitTimeBeforeMutatingFailover,
        waitTimeBeforeMutatingFailoverWithPrimaryException,
        waitTimeBeforeAcceptingBatchResults,
        waitTimeBeforeRequestingBatchFailover,
        waitTimeBeforeMutatingBatchFailover);
  }

  public HTableInterface getTable(String tableName, ExecutorService pool)
      throws IOException {
    return this.getTable(TableName.valueOf(tableName), pool);
  }

  public HTableInterface getTable(byte[] tableName, ExecutorService pool)
      throws IOException {
    return this.getTable(TableName.valueOf(tableName), pool);
  }

  public HTableInterface getTable(TableName tableName, ExecutorService pool)
      throws IOException {
    HTableInterface primaryHTable = primaryConnection.getTable(tableName, pool);
    ArrayList<HTableInterface> failoverHTables = new ArrayList<HTableInterface>();
    for (HConnection failOverConnection : failoverConnections) {
      failoverHTables.add(failOverConnection.getTable(tableName, pool));
    }

    return new HTableMultiCluster(originalConfiguration, primaryHTable,
        failoverHTables, isMasterMaster, 
        waitTimeBeforeAcceptingResults,
        waitTimeBeforeRequestingFailover,
        waitTimeBeforeMutatingFailover,
        waitTimeBeforeMutatingFailoverWithPrimaryException,
        waitTimeBeforeAcceptingBatchResults,
        waitTimeBeforeRequestingBatchFailover,
        waitTimeBeforeMutatingBatchFailover);
  }

  public boolean isMasterRunning() throws MasterNotRunningException,
      ZooKeeperConnectionException {
    return primaryConnection.isMasterRunning();
  }
  
  public boolean isTableEnabled(TableName tableName) throws IOException {
    return primaryConnection.isTableEnabled(tableName);
  }

  @Deprecated
  public
  boolean isTableEnabled(byte[] tableName) throws IOException {
    return primaryConnection.isTableEnabled(tableName);
  }

  public boolean isTableDisabled(TableName tableName) throws IOException {
    return primaryConnection.isTableDisabled(tableName);
  }

  @Deprecated
  public
  boolean isTableDisabled(byte[] tableName) throws IOException {
    return primaryConnection.isTableDisabled(tableName);
  }

  public boolean isTableAvailable(TableName tableName) throws IOException {
    return primaryConnection.isTableAvailable(tableName);
  }

  @Deprecated
  public
  boolean isTableAvailable(byte[] tableName) throws IOException {
    return primaryConnection.isTableAvailable(tableName);
  }

  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys)
      throws IOException {
    return primaryConnection.isTableAvailable(tableName, splitKeys);
  }

  @Deprecated
  public
  boolean isTableAvailable(byte[] tableName, byte[][] splitKeys)
      throws IOException {
    return primaryConnection.isTableAvailable(tableName, splitKeys);
  }

  public HTableDescriptor[] listTables() throws IOException {
    return primaryConnection.listTables();
  }

  @Deprecated
  public String[] getTableNames() throws IOException {
    return primaryConnection.getTableNames();
  }

  public TableName[] listTableNames() throws IOException {
    return primaryConnection.listTableNames();
  }

  public HTableDescriptor getHTableDescriptor(TableName tableName)
      throws IOException {
    return primaryConnection.getHTableDescriptor(tableName);
  }

  @Deprecated
  public
  HTableDescriptor getHTableDescriptor(byte[] tableName) throws IOException {
    return primaryConnection.getHTableDescriptor(tableName);
  }

  public HRegionLocation locateRegion(TableName tableName, byte[] row)
      throws IOException {
    return primaryConnection.locateRegion(tableName, row);
  }

  @Deprecated
  public HRegionLocation locateRegion(byte[] tableName, byte[] row)
      throws IOException {
    return primaryConnection.locateRegion(tableName, row);
  }

  public void clearRegionCache() {
    primaryConnection.clearRegionCache();
  }

  public void clearRegionCache(TableName tableName) {
    primaryConnection.clearRegionCache(tableName);
  }

  @Deprecated
  public
  void clearRegionCache(byte[] tableName) {
    primaryConnection.clearRegionCache(tableName);

  }

  public void deleteCachedRegionLocation(HRegionLocation location) {
    primaryConnection.deleteCachedRegionLocation(location);
  }

  public HRegionLocation relocateRegion(TableName tableName, byte[] row)
      throws IOException {
    return primaryConnection.relocateRegion(tableName, row);
  }

  @Deprecated
  public
  HRegionLocation relocateRegion(byte[] tableName, byte[] row)
      throws IOException {
    return primaryConnection.relocateRegion(tableName, row);
  }

  public void updateCachedLocations(TableName tableName, byte[] rowkey,
      Object exception, HRegionLocation source) {
    primaryConnection.updateCachedLocations(tableName, rowkey, exception, source);
  }

  @Deprecated
  public
  void updateCachedLocations(byte[] tableName, byte[] rowkey, Object exception,
      HRegionLocation source) {
    primaryConnection.updateCachedLocations(tableName, rowkey, exception, source);
  }

  public HRegionLocation locateRegion(byte[] regionName) throws IOException {
    return primaryConnection.locateRegion(regionName);
  }

  public List<HRegionLocation> locateRegions(TableName tableName)
      throws IOException {
    return primaryConnection.locateRegions(tableName);
  }

  @Deprecated
  public
  List<HRegionLocation> locateRegions(byte[] tableName) throws IOException {
    return (List<HRegionLocation>) primaryConnection.locateRegion(tableName);
  }

  public List<HRegionLocation> locateRegions(TableName tableName,
      boolean useCache, boolean offlined) throws IOException {
    return (List<HRegionLocation>) primaryConnection.locateRegions(tableName,
        useCache, offlined);
  }

  @Deprecated
  public List<HRegionLocation> locateRegions(byte[] tableName,
      boolean useCache, boolean offlined) throws IOException {
    return (List<HRegionLocation>) primaryConnection.locateRegions(tableName,
        useCache, offlined);
  }

  public BlockingInterface getMaster() throws IOException {
    return primaryConnection.getMaster();
  }

  public org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService.BlockingInterface getAdmin(
      ServerName serverName) throws IOException {
    return primaryConnection.getAdmin(serverName);
  }

  public org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService.BlockingInterface getClient(
      ServerName serverName) throws IOException {
    return primaryConnection.getClient(serverName);
  }

  public org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService.BlockingInterface getAdmin(
      ServerName serverName, boolean getMaster) throws IOException {
    return primaryConnection.getAdmin(serverName);
  }

  public HRegionLocation getRegionLocation(TableName tableName, byte[] row,
      boolean reload) throws IOException {
    return primaryConnection.getRegionLocation(tableName, row, reload);
  }

  @Deprecated
  public
  HRegionLocation getRegionLocation(byte[] tableName, byte[] row, boolean reload)
      throws IOException {
    return primaryConnection.getRegionLocation(tableName, row, reload);
  }

  @Deprecated
  public
  void processBatch(List<? extends Row> actions, TableName tableName,
      ExecutorService pool, Object[] results) throws IOException,
      InterruptedException {
    throw new RuntimeException("processBatch not supported in " + this.getClass());
    
  }

  @Deprecated
  public
  void processBatch(List<? extends Row> actions, byte[] tableName,
      ExecutorService pool, Object[] results) throws IOException,
      InterruptedException {
    primaryConnection.processBatch(actions, tableName, pool, results);
  }

  @Deprecated
  public <R> void processBatchCallback(List<? extends Row> list,
      TableName tableName, ExecutorService pool, Object[] results,
      Callback<R> callback) throws IOException, InterruptedException {
    primaryConnection.processBatchCallback(list, tableName, pool, results, callback);
  }

  @Deprecated
  public <R> void processBatchCallback(List<? extends Row> list,
      byte[] tableName, ExecutorService pool, Object[] results,
      Callback<R> callback) throws IOException, InterruptedException {
    primaryConnection.processBatchCallback(list, tableName, pool, results, callback);

  }

  public void setRegionCachePrefetch(TableName tableName, boolean enable) {
    RuntimeException lastException = null;
    try {
      primaryConnection.setRegionCachePrefetch(tableName, enable);
    } catch (RuntimeException e) {
      LOG.error("Exception while closing primary", e);
      lastException = e;
    }
    for (HConnection failOverConnection : failoverConnections) {
      try {
        failOverConnection.setRegionCachePrefetch(tableName, enable);
      } catch (RuntimeException e) {
        LOG.error("Exception while closing failOverConnection", e);
        lastException = e;
      }
    }
    if (lastException != null) {
      throw lastException;
    }

  }

  public void setRegionCachePrefetch(byte[] tableName, boolean enable) {
    this.setRegionCachePrefetch(TableName.valueOf(tableName), enable);
  }

  public boolean getRegionCachePrefetch(TableName tableName) {
    return this.getRegionCachePrefetch(tableName);
  }

  public boolean getRegionCachePrefetch(byte[] tableName) {
    return this.getRegionCachePrefetch(TableName.valueOf(tableName));
  }

  public int getCurrentNrHRS() throws IOException {
    return primaryConnection.getCurrentNrHRS();
  }

  public HTableDescriptor[] getHTableDescriptorsByTableName(
      List<TableName> tableNames) throws IOException {
    return primaryConnection.getHTableDescriptorsByTableName(tableNames);
  }

  @Deprecated
  public
  HTableDescriptor[] getHTableDescriptors(List<String> tableNames)
      throws IOException {
    return primaryConnection.getHTableDescriptors(tableNames);
  }

  public boolean isClosed() {
    return primaryConnection.isClosed();
  }

  public void clearCaches(ServerName sn) {
    RuntimeException lastException = null;
    try {
      primaryConnection.clearCaches(sn);
    } catch (RuntimeException e) {
      LOG.error("Exception while closing primary", e);
      lastException = e;
    }
    for (HConnection failOverConnection : failoverConnections) {
      try {
        failOverConnection.clearCaches(sn);
      } catch (RuntimeException e) {
        LOG.error("Exception while closing failOverConnection", e);
        lastException = e;
      }
    }
    if (lastException != null) {
      throw lastException;
    }
  }

  public boolean isDeadServer(ServerName serverName) {
    return primaryConnection.isDeadServer(serverName);
  }

  public NonceGenerator getNonceGenerator() {
    return primaryConnection.getNonceGenerator();
  }


  @Deprecated
  public
  MasterKeepAliveConnection getKeepAliveMasterService()
      throws MasterNotRunningException {
    // TODO Auto-generated method stub
    return null;
  }
}
