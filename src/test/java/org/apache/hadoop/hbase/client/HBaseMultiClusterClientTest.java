package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HBaseMultiClusterClientTest {

  @Test
  public void testHBaseMultiClusterClientTest() throws Exception {

    DynamicClassLoader g;

    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("{Start}");

    //Cluster1
    HBaseTestingUtility htu1 = new HBaseTestingUtility();
    htu1.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/x1");
    htu1.getConfiguration().set(HConstants.ZOOKEEPER_CLIENT_PORT,
            "64410");
    htu1.getConfiguration().set(HConstants.MASTER_INFO_PORT, "64310");
    //htu1.setZkCluster(htu0.getZkCluster());


    // Cluster 2
    HBaseTestingUtility htu2 = new HBaseTestingUtility();
    htu2.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/x2");
    htu2.getConfiguration().set(HConstants.ZOOKEEPER_CLIENT_PORT,
            "64110");
    htu2.getConfiguration().set(HConstants.MASTER_INFO_PORT, "64210");

    //htu2.setZkCluster(htu0.getZkCluster());

    try {

      System.out.println(strBuilder.toString());

      htu1.startMiniCluster();

      System.out.println("------------------------1");

      htu2.startMiniCluster();

      System.out.println("------------------------2");

      final TableName TABLE_NAME = TableName.valueOf("test");
      final byte[] FAM_NAME = Bytes.toBytes("fam");
      final byte[] QUAL_NAME = Bytes.toBytes("qual");
      final byte[] VALUE = Bytes.toBytes("value");

      System.out.println(strBuilder.toString());

      HTable table1 = htu1.createTable(TABLE_NAME, FAM_NAME);
      HTable table2 = htu2.createTable(TABLE_NAME, FAM_NAME);

      Configuration combinedConfig = HBaseMultiClusterConfigUtil.combineConfigurations(htu1.getConfiguration(),
              htu2.getConfiguration());

      combinedConfig.setInt(ConfigConst.HBASE_WAIT_TIME_BEFORE_TRYING_PRIMARY_AFTER_FAILURE, 0);

      HConnection connection = HConnectionManagerMultiClusterWrapper.createConnection(combinedConfig);

      HTableInterface multiTable = connection.getTable(TABLE_NAME);

      Put put1 = new Put(Bytes.toBytes("A1"));
      put1.add(FAM_NAME, QUAL_NAME, VALUE);
      multiTable.put(put1);
      multiTable.flushCommits();

      Get get1 = new Get(Bytes.toBytes("A1"));
      Result r1_1 = table1.get(get1);
      Result r1_2 = table2.get(get1);

      System.out.println("----------------------------");
      System.out.println(r1_1 + " " + r1_2);
      System.out.println(r1_1.isEmpty() + " " + r1_2.isEmpty());
      System.out.println("----------------------------");
      assertFalse("A1 not found in htu1", r1_1.isEmpty());
      assertTrue("A1 found in htu2", r1_2.isEmpty());

      strBuilder.append("{r1_1.isEmpty():" + r1_1.isEmpty() + "}");
      strBuilder.append("{r1_2.isEmpty():" + r1_2.isEmpty() + "}");

      System.out.println(strBuilder.toString());


      htu1.deleteTable(TABLE_NAME);
      System.out.println("------------2");

      Put put2 = new Put(Bytes.toBytes("A2"));
      put2.add(FAM_NAME, QUAL_NAME, VALUE);
      System.out.println("------------3");
      table2.put(put2);

      Get get2 = new Get(Bytes.toBytes("A2"));
      System.out.println(table2.get(get2));

      System.out.println("------------5");
      multiTable.put(put2);
      //multiTable.flushCommits();
      //Get get2 = new Get(Bytes.toBytes("A2"));
      Result r2_2 = table2.get(get2);
      assertFalse("A2 not found in htu2", r2_2.isEmpty());

      strBuilder.append("{r2_2.getExists():" + r2_2.isEmpty() + "}");

      System.out.println(strBuilder.toString());

      System.out.println("------------6");
      table1 = htu1.createTable(TABLE_NAME, FAM_NAME);

      System.out.println("------------7");

      Put put3 = new Put(Bytes.toBytes("A3"));
      put3.add(FAM_NAME, QUAL_NAME, VALUE);
      multiTable = connection.getTable(TABLE_NAME);
      multiTable.put(put3);
      multiTable.flushCommits();
      System.out.println("------------8");

      Get get3 = new Get(Bytes.toBytes("A3"));
      Result r3_1 = table1.get(get3);
      Result r3_2 = table2.get(get3);

      System.out.println("----------------------------");
      System.out.println(r3_1 + " " + r3_2);
      System.out.println(r3_1.isEmpty() + " " + r3_2.isEmpty());
      System.out.println("----------------------------");

      assertFalse("A3 not found in htu1", r3_1.isEmpty());

      assertTrue("A3 found in htu2", r3_2.isEmpty());

      strBuilder.append("{r3_1.isEmpty():" + r3_1.isEmpty() + "}");
      strBuilder.append("{r3_2.isEmpty():" + r3_2.isEmpty() + "}");

      table1.close();
      table2.close();

      System.out.println(strBuilder.toString());

    } finally {
      htu2.shutdownMiniCluster();
      htu1.shutdownMiniCluster();
    }
  }

}
