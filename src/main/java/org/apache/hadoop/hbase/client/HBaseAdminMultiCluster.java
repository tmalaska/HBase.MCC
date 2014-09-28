package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin.MasterCallable;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.log4j.Logger;

import com.google.protobuf.ServiceException;

public class HBaseAdminMultiCluster extends HBaseAdmin {

  Logger log = Logger.getLogger(HBaseAdminMultiCluster.class);

  Map<String, HBaseAdmin> failoverAdminMap = new HashMap<String, HBaseAdmin>();

  public HBaseAdminMultiCluster(Configuration c)
      throws MasterNotRunningException, ZooKeeperConnectionException,
      IOException {
    super(HBaseMultiClusterConfigUtil.splitMultiConfigFile(c).get(
        HBaseMultiClusterConfigUtil.PRIMARY_NAME));

    Map<String, Configuration> configs = HBaseMultiClusterConfigUtil
        .splitMultiConfigFile(c);

    for (Entry<String, Configuration> entry : configs.entrySet()) {

      if (entry.getKey().equals(HBaseMultiClusterConfigUtil.PRIMARY_NAME)) {

      } else {
        HBaseAdmin admin = new HBaseAdmin(entry.getValue());
        log.info("creating HBaseAdmin for : " + entry.getKey());
        failoverAdminMap.put(entry.getKey(), admin);
        log.info(" - successfully creating HBaseAdmin for : " + entry.getKey());
      }
    }
    log.info("Successful loaded all HBaseAdmins");

  }

  @Override
  /**
   * This will only return tables that all three HBase clusters have
   */
  public HTableDescriptor[] listTables() throws IOException {
    Map<String, HTableDescriptor> tableMap = new HashMap<String, HTableDescriptor>();

    HTableDescriptor[] primaryList = super.listTables();

    for (HTableDescriptor table : primaryList) {
      tableMap.put(table.getNameAsString(), table);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      Map<String, HTableDescriptor> tempTableMap = new HashMap<String, HTableDescriptor>();

      HTableDescriptor[] failureList = super.listTables();

      for (HTableDescriptor table : failureList) {
        String tableName = table.getNameAsString();
        if (tableMap.containsKey(tableName)) {
          tempTableMap.put(tableName, tableMap.get(tableName));
        }
      }
      tableMap = tempTableMap;
    }

    HTableDescriptor[] results = new HTableDescriptor[tableMap.size()];
    int counter = 0;
    for (HTableDescriptor table : tableMap.values()) {
      results[counter++] = table;
    }

    return results;
  }

  @Override
  public void createTable(HTableDescriptor desc) throws IOException {
    try {
      super.createTable(desc, null);
      
    } catch (Exception e) {
      log.error("Unable to create table " + desc.getTableName()
          + " in primary.", e);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().createTable(desc, null);
      } catch (Exception e) {
        log.error("Unable to create table " + desc.getTableName() + " in "
            + entry.getKey(), e);
      }
    }
  }

  @Override
  public void createTable(HTableDescriptor desc, byte[] startKey,
      byte[] endKey, int numRegions) {
    try {
      super.createTable(desc, startKey, endKey, numRegions);
    } catch (Exception e) {
      log.error("Unable to create table " + desc.getTableName()
          + " in primary.", e);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().createTable(desc, startKey, endKey, numRegions);
      } catch (Exception e) {
        log.error("Unable to create table " + desc.getTableName() + " in "
            + entry.getKey(), e);
      }
    }
  }

  @Override
  public void createTable(final HTableDescriptor desc, byte[][] splitKeys) {
    try {
      super.createTable(desc, splitKeys);
    } catch (Exception e) {
      log.error("Unable to create table " + desc.getTableName()
          + " in primary.", e);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().createTable(desc, splitKeys);
      } catch (Exception e) {
        log.error("Unable to create table " + desc.getTableName() + " in "
            + entry.getKey(), e);
      }
    }
  }

  @Override
  public void createTableAsync(final HTableDescriptor desc,
      final byte[][] splitKeys) {
    try {
      super.createTableAsync(desc, splitKeys);
    } catch (Exception e) {
      log.error("Unable to create table " + desc.getTableName()
          + " in primary.", e);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().createTableAsync(desc, splitKeys);
      } catch (Exception e) {
        log.error("Unable to create table " + desc.getTableName() + " in "
            + entry.getKey(), e);
      }
    }
  }

  @Override
  public void deleteTable(final TableName tableName) throws IOException {
    try {
      super.deleteTable(tableName);
    } catch (Exception e) {
      log.error("Unable to delete table " + tableName + " in primary.", e);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().deleteTable(tableName);
      } catch (Exception e) {
        log.error(
            "Unable to delete table " + tableName + " in " + entry.getKey(), e);
      }
    }
  }

  @Override
  public void deleteTable(final String tableName) throws IOException {
    deleteTable(TableName.valueOf(tableName));
  }

  @Override
  public void deleteTable(final byte[] tableName) throws IOException {
    deleteTable(TableName.valueOf(tableName));
  }

  @Override
  public HTableDescriptor[] deleteTables(String regex) throws IOException {
    return deleteTables(Pattern.compile(regex));
  }

  @Override
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    HTableDescriptor[] results = null;

    try {
      results = super.deleteTables(pattern);
    } catch (Exception e) {
      log.error("Unable to delete tables " + pattern + " in primary.", e);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().deleteTables(pattern);
      } catch (Exception e) {
        log.error(
            "Unable to delete tables " + pattern + " in " + entry.getKey(), e);
      }
    }
    return results;
  }

  @Override
  public void enableTable(final TableName tableName) {
    try {
      super.enableTable(tableName);
    } catch (Exception e) {
      log.error("Unable to enable tables " + tableName + " in primary.", e);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().enableTable(tableName);
      } catch (Exception e) {
        log.error(
            "Unable to enable tables " + tableName + " in " + entry.getKey(), e);
      }
    }
  }

  @Override
  public void enableTable(final byte[] tableName) throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  @Override
  public void enableTable(final String tableName) throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  public void enableTableAsync(final TableName tableName) {
    try {
      super.enableTableAsync(tableName);
    } catch (Exception e) {
      log.error("Unable to enable tables " + tableName + " in primary.", e);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().enableTableAsync(tableName);
      } catch (Exception e) {
        log.error(
            "Unable to enable tables " + tableName + " in " + entry.getKey(), e);
      }
    }
  }

  @Override
  public void enableTableAsync(final byte[] tableName) throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  @Override
  public void enableTableAsync(final String tableName) throws IOException {
    enableTableAsync(TableName.valueOf(tableName));
  }

  @Override
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    return enableTables(Pattern.compile(regex));
  }

  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {

    HTableDescriptor[] results = null;

    try {
      results = super.enableTables(pattern);
    } catch (Exception e) {
      log.error("Unable to enable tables " + pattern + " in primary.", e);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().enableTables(pattern);
      } catch (Exception e) {
        log.error(
            "Unable to enable tables " + pattern + " in " + entry.getKey(), e);
      }
    }
    return results;
  }

  @Override
  public void disableTableAsync(final TableName tableName) throws IOException {
    try {
      super.disableTableAsync(tableName);
    } catch (Exception e) {
      log.error("Unable to enable tables " + tableName + " in primary.", e);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().disableTableAsync(tableName);
      } catch (Exception e) {
        log.error(
            "Unable to enable tables " + tableName + " in " + entry.getKey(), e);
      }
    }
  }

  @Override
  public void disableTableAsync(final byte[] tableName) throws IOException {
    disableTableAsync(TableName.valueOf(tableName));
  }

  @Override
  public void disableTableAsync(final String tableName) throws IOException {
    disableTableAsync(TableName.valueOf(tableName));
  }

  @Override
  public void disableTable(final TableName tableName) {
    try {
      super.disableTable(tableName);
    } catch (Exception e) {
      log.error("Unable to disableTable  " + tableName + " in primary.", e);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().disableTable(tableName);
      } catch (Exception e) {
        log.error(
            "Unable to disableTable  " + tableName + " in " + entry.getKey(), e);
      }
    }
  }
 
  
  @Override
  public void disableTable(final byte[] tableName) throws IOException {
    disableTable(TableName.valueOf(tableName));
  }

  @Override
  public void disableTable(final String tableName) throws IOException {
    disableTable(TableName.valueOf(tableName));
  }
  
  /**
   * @param tableName
   *          Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  public Map<String, Boolean> tableExistMultiCluster(final TableName tableName)
      throws IOException {
    Map<String, Boolean> results = new HashMap<String, Boolean>();
    results.put(HBaseMultiClusterConfigUtil.PRIMARY_NAME,
        super.tableExists(tableName));

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      results.put(entry.getKey(), entry.getValue().tableExists(tableName));
    }
    return results;
  }

  public void close() throws IOException {
    try {
      super.close();
    } catch (Exception e) {
      log.error("Unable to close in primary.", e);
    }
    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (Exception e) {
        log.error("Unable to close  " + entry.getKey(), e);
      }
    }
  }

  public void abort(String why, Throwable e) {
    try {
      super.abort(why, e);
    } catch (Exception e2) {
      log.error("Unable to abort in primary.", e2);
    }
    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().abort(why, e);
      } catch (Exception e2) {
        log.error("Unable to abort  " + entry.getKey(), e2);
      }
    }

  }

  public boolean isAborted() {
    // TODO Auto-generated method stub
    return false;
  }

}
