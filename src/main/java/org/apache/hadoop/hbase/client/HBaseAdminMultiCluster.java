package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
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

import com.google.common.base.Optional;
import com.google.protobuf.ServiceException;

public class HBaseAdminMultiCluster extends HBaseAdmin {

  Logger LOG = Logger.getLogger(HBaseAdminMultiCluster.class);

  Map<String, HBaseAdmin> failoverAdminMap = new HashMap<String, HBaseAdmin>();

  public HBaseAdminMultiCluster(Configuration c)
      throws MasterNotRunningException, ZooKeeperConnectionException,
      IOException {
    super(HBaseMultiClusterConfigUtil.splitMultiConfigFile(c).get(
        HBaseMultiClusterConfigUtil.PRIMARY_NAME));

    Map<String, Configuration> configs = HBaseMultiClusterConfigUtil
        .splitMultiConfigFile(c);

    for (Entry<String, Configuration> entry : configs.entrySet()) {

      if (!entry.getKey().equals(HBaseMultiClusterConfigUtil.PRIMARY_NAME)) {
        HBaseAdmin admin = new HBaseAdmin(entry.getValue());
        LOG.info("creating HBaseAdmin for : " + entry.getKey());
        failoverAdminMap.put(entry.getKey(), admin);
        LOG.info(" - successfully creating HBaseAdmin for : " + entry.getKey());
      }
    }
    LOG.info("Successful loaded all HBaseAdmins");

  }

  @Override
  /**
   * This will only return tables that all three HBase clusters have
   */
  public HTableDescriptor[] listTables() throws IOException {
    Map<TableName, HTableDescriptor> tableMap = new HashMap<TableName, HTableDescriptor>();

    HTableDescriptor[] primaryList = super.listTables();

    for (HTableDescriptor table : primaryList) {
      tableMap.put(table.getTableName(), table);
    }

    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      Map<TableName, HTableDescriptor> tempTableMap = new HashMap<TableName, HTableDescriptor>();

      HTableDescriptor[] failureList = super.listTables();

      for (HTableDescriptor table : failureList) {
        TableName tableName = table.getTableName();
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
  public void createTable(final HTableDescriptor desc) throws IOException {

    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.createTable(desc, null);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().createTable(desc, null);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "createTable");
  }

  @Override
  public void createTable(final HTableDescriptor desc, final byte[] startKey,
      final byte[] endKey, final int numRegions) throws IOException {

    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.createTable(desc, startKey, endKey,
            numRegions);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().createTable(desc, startKey, endKey, numRegions);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "createTable");
  }

  @Override
  public void createTable(final HTableDescriptor desc, 
      final byte[][] splitKeys) throws IOException {
    
    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.createTable(desc, splitKeys);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().createTable(desc, splitKeys);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "createTable");
  }

  @Override
  public void createTableAsync(final HTableDescriptor desc,
      final byte[][] splitKeys) throws IOException {
    
    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.createTableAsync(desc, splitKeys);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().createTableAsync(desc, splitKeys);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "createTableAsync");

  }

  @Override
  public void deleteTable(final TableName tableName) throws IOException {
    
    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.deleteTable(tableName);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().deleteTable(tableName);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "deleteTable");
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
  public HTableDescriptor[] deleteTables(final Pattern pattern) throws IOException {
    
    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;
    final Value<HTableDescriptor[]> results = new Value<HTableDescriptor[]>();
    
    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        results.setValue(HBaseAdminMultiCluster.super.deleteTables(pattern));
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().deleteTables(pattern);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "deleteTables");
    
    return results.getValue();
  }

  @Override
  public void enableTable(final TableName tableName) throws IOException {
    @SuppressWarnings("unchecked")
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.enableTable(tableName);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().enableTable(tableName);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "enableTable");
  }

  @Override
  public void enableTable(final byte[] tableName) throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  @Override
  public void enableTable(final String tableName) throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  public void enableTableAsync(final TableName tableName) throws IOException {
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.enableTableAsync(tableName);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().enableTableAsync(tableName);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "enableTableAsync");
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

  public HTableDescriptor[] enableTables(final Pattern pattern) throws IOException {
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;
    final Value<HTableDescriptor[]> results = new Value<HTableDescriptor[]>();

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        results.setValue(HBaseAdminMultiCluster.super.enableTables(pattern));
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().enableTables(pattern);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "enableTables");
    
    return results.getValue();
  }

  @Override
  public void disableTableAsync(final TableName tableName) throws IOException {
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.disableTableAsync(tableName);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().disableTableAsync(tableName);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "disableTableAsync");
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
  public void disableTable(final TableName tableName) throws IOException {
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.disableTable(tableName);
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().disableTable(tableName);
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "disableTable");
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
    Callable<Void>[] callArray = new Callable[failoverAdminMap.size() + 1];
    int counter = 0;

    callArray[counter++] = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HBaseAdminMultiCluster.super.close();
        return null;
      }
    };

    for (final Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      callArray[counter++] = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          entry.getValue().close();
          return null;
        }
      };
    }
    replicationClusterExecute(callArray, "close");
    
  }

  public void abort(String why, Throwable e) {
    try {
      super.abort(why, e);
    } catch (Exception e2) {
      LOG.error("Unable to abort in primary.", e2);
    }
    for (Entry<String, HBaseAdmin> entry : failoverAdminMap.entrySet()) {
      try {
        entry.getValue().abort(why, e);
      } catch (Exception e2) {
        LOG.error("Unable to abort  " + entry.getKey(), e2);
      }
    }

  }

  public boolean isAborted() {
    // TODO Auto-generated method stub
    return false;
  }

  private void replicationClusterExecute(Callable<Void>[] callArray,
      String actionMethodName) throws IOException {
    Exception exp = null;
    int expCounter = 0;

    for (Callable<Void> call : callArray) {
      try {
        call.call();
      } catch (Exception e) {
        if (exp == null) {
          exp = e;
        }
        expCounter++;
      }
    }
    if (expCounter > 0) {
      throw new IOException("Got " + expCounter
          + " exceptions trying to execute " + actionMethodName, exp);
    }
  }
  
  private static class Value<T extends Object> {
    T value;
    public void setValue(T value) {
      this.value = value;
    }
    
    public T getValue() {
      return value;
    }
  }
}
