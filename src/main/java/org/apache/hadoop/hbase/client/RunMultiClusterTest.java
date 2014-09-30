package org.apache.hadoop.hbase.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;


public class RunMultiClusterTest {
  
  static final Log LOG = LogFactory.getLog(RunMultiClusterTest.class);
  
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("RunMultiClusterTest <core-site file> <hbase-site file> <hdfs-site file> <tableName> <familyName> <numberOfPuts> <millisecond of wait> <outputCsvFile>");
    }
    
    Configuration config = HBaseConfiguration.create();
    config.addResource(new FileInputStream(new File(args[0])));
    config.addResource(new FileInputStream(new File(args[1])));
    config.addResource(new FileInputStream(new File(args[2])));
    
    String tableName = args[3];
    String familyName = args[4];
    int numberOfPuts = Integer.parseInt(args[5]);
    int secondsOfWait = Integer.parseInt(args[6]);
    String outputCsvFile = args[7];
    
    System.out.println("Getting HAdmin");
    
    System.out.println(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG + ": " + config.get(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG));
    System.out.println("hbase.zookeeper.quorum: " + config.get("hbase.zookeeper.quorum"));
    System.out.println("hbase.failover.cluster.fail1.hbase.hstore.compaction.max: " + config.get("hbase.failover.cluster.fail1.hbase.hstore.compaction.max"));
    
    HBaseAdmin admin = new HBaseAdminMultiCluster(config);
    
    try {
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
    } catch (Exception e) {
      //nothing
    }
    
    System.out.println(" - Got HAdmin:" + admin.getClass());
    
    HTableDescriptor tableD = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor columnD = new HColumnDescriptor(Bytes.toBytes(familyName));
    tableD.addFamily(columnD);
    
    byte[][] splitKeys = new byte[10][1];
    splitKeys[0][0] = '0';
    splitKeys[1][0] = '1';
    splitKeys[2][0] = '2';
    splitKeys[3][0] = '3';
    splitKeys[4][0] = '4';
    splitKeys[5][0] = '5';
    splitKeys[6][0] = '6';
    splitKeys[7][0] = '7';
    splitKeys[8][0] = '8';
    splitKeys[9][0] = '9';
    
    admin.createTable(tableD, splitKeys);
    
    System.out.println("Getting HConnection");
    
    config.set("hbase.client.retries.number", "1");
    config.set("hbase.client.pause", "1");
    
    HConnection connection = HConnectionManagerMultiClusterWrapper.createConnection(config);
    
    System.out.println(" - Got HConnection: " + connection.getClass());
    
    System.out.println("Getting HTable");
    
    HTableInterface table = connection.getTable(tableName);
    
    System.out.println("Got HTable: " + table.getClass());
    
    BufferedWriter writer = new BufferedWriter(new FileWriter(outputCsvFile));
    
    HTableStats.printCSVHeaders(writer);
    
    for (int i = 1; i <= numberOfPuts; i++) {
      System.out.print("p");
      Put put = new Put(Bytes.toBytes(i%10 + ".key." + StringUtils.leftPad(String.valueOf(i), 12)));
      put.add(Bytes.toBytes(familyName), Bytes.toBytes("C"), Bytes.toBytes("Value:" + i));
      table.put(put);
      
      System.out.print("g");
      Get get = new Get(Bytes.toBytes(i%10 + ".key." + StringUtils.leftPad(String.valueOf(i), 12)));
      table.get(get);
      
      System.out.print("d");
      Delete delete = new Delete(Bytes.toBytes(i%10 + ".key." + StringUtils.leftPad(String.valueOf(i), 12)));
      table.delete(delete);
      
      System.out.print(".");
      if (i % 100 == 0) {
        System.out.println("|");
        HTableStats stats = ((HTableMultiCluster)table).getStats();
        stats.printPrettyStats();
        stats.printCSVStats(writer);
      }
      //secondsOfWait
      Thread.sleep(secondsOfWait);
    }
    
    writer.close();
    
    admin.disableTable(TableName.valueOf(tableName));
    admin.deleteTable(TableName.valueOf(tableName));
    
    connection.close();
    admin.close();
  } 
  
}
