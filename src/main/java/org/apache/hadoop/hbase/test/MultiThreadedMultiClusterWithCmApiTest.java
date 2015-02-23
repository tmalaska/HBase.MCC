package org.apache.hadoop.hbase.test;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadedMultiClusterWithCmApiTest {

  static Logger LOG = Logger.getLogger(MultiThreadedMultiClusterWithCmApiTest.class);

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {


      System.out.println("RunMultiClusterTest " +
              "<CM-Host-1> " +
              "<UserName> " +
              "<Password> " +
              "<Cluster-1> " +
              "<HBase-Service-1> " +
              "<CM-Host-2> " +
              "<UserName-2> " +
              "<Password-2> " +
              "<Cluster-2> " +
              "<HBase-Service-2> " +
              "<tableName> " +
              "<familyName> " +
              "<numberOfPuts> " +
              "<millisecond of wait> " +
              "<numberOfThreads> " +
              "<outputCsvFile>");
    }

    final String cmHost1 = args[0];
    final String username1 = args[1];
    final String password1 = args[2];
    final String cluster1 = args[3];
    final String hbaseService1 = args[4];
    final String cmHost2 = args[5];
    final String username2 = args[6];
    final String password2 = args[7];
    final String cluster2 = args[8];
    final String hbaseService2 = args[9];

    LOG.info("--Getting Configurations");

    Configuration config = HBaseMultiClusterConfigUtil.combineConfigurations(cmHost1, username1, password1,
            cluster1, hbaseService1,
            cmHost2, username2, password2,
            cluster2, hbaseService2);

    LOG.info("--Got Configuration");

    final String tableName = args[10];
    final String familyName = args[11];
    final int numberOfPuts = Integer.parseInt(args[12]);
    final int millisecondToWait = Integer.parseInt(args[13]);
    final int numberOfThreads = Integer.parseInt(args[14]);
    final String outputCsvFile = args[15];

    LOG.info("Getting HAdmin");

    LOG.info(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG + ": " + config.get(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG));
    LOG.info("hbase.zookeeper.quorum: " + config.get("hbase.zookeeper.quorum"));
    LOG.info("hbase.failover.cluster.fail1.hbase.hstore.compaction.max: " + config.get("hbase.failover.cluster.fail1.hbase.hstore.compaction.max"));

    HBaseAdmin admin = new HBaseAdminMultiCluster(config);

    try {
      if (admin.tableExists(TableName.valueOf(tableName))) {
        try {
          admin.disableTable(TableName.valueOf(tableName));
        } catch (Exception e) {
          //nothing
        }
        admin.deleteTable(TableName.valueOf(tableName));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    LOG.info(" - Got HAdmin:" + admin.getClass());

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

    LOG.info(" - About to create Table " + tableD.getName());

    admin.createTable(tableD, splitKeys);

    LOG.info(" - Created Table " + tableD.getName());

    LOG.info("Getting HConnection");

    config.set("hbase.client.retries.number", "1");
    config.set("hbase.client.pause", "1");

    final HConnection connection = HConnectionManagerMultiClusterWrapper.createConnection(config);

    LOG.info(" - Got HConnection: " + connection.getClass());

    LOG.info("Getting HTable");

    final AtomicInteger threadFinishCounter = new AtomicInteger(0);


    //Make sure output folder exist
    File outputFolder = new File(outputCsvFile);
    if (outputFolder.exists() == false) {
      outputFolder.mkdirs();
    }

    for (int threadNum = 0; threadNum < numberOfThreads; threadNum++) {

      final BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputCsvFile + "/thread-" + threadNum + ".csv")));

      final int threadFinalNum = threadNum;

      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Random r = new Random();
            HTableInterface table = connection.getTable(tableName);
            HTableStats stats = ((HTableMultiCluster) table).getStats();
            stats.printStats(writer, 5000);

            for (int i = 1; i <= numberOfPuts; i++) {

              int hash = r.nextInt(10);

              Put put = new Put(Bytes.toBytes(hash + ".key." + i + "." + StringUtils.leftPad(String.valueOf(i * threadFinalNum), 12)));
              put.add(Bytes.toBytes(familyName), Bytes.toBytes("C"), Bytes.toBytes("Value:" + i * threadFinalNum));
              table.put(put);

              Thread.sleep(millisecondToWait);

              Get get = new Get(Bytes.toBytes(hash + ".key." + StringUtils.leftPad(String.valueOf(i * threadFinalNum), 12)));
              table.get(get);

              Thread.sleep(millisecondToWait);

              //Delete delete = new Delete(Bytes.toBytes(hash + ".key." + StringUtils.leftPad(String.valueOf(i * threadFinalNum), 12)));
              //table.delete(delete);

              Thread.sleep(millisecondToWait);

              if (i % 10 == 0) {
                writeToSystemOut("{thread:" + threadFinalNum + ",count=" + i + "}", true);
              } else if (numberOfPuts % 1000 == 0) {
                writeToSystemOut(".", false);
              }
            }
            stats.stopPrintingStats();
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            threadFinishCounter.incrementAndGet();
            try {
              writer.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
          }

        }
      });
      t.start();
    }

    while (threadFinishCounter.get() < numberOfThreads) {
      Thread.sleep(millisecondToWait * 10);
    }

    //admin.disableTable(TableName.valueOf(tableName));
    //admin.deleteTable(TableName.valueOf(tableName));

    System.out.println("close connection");
    connection.close();
    System.out.println("close admin");
    admin.close();
    System.out.println("done");
    System.exit(0);
  }

  public static synchronized void writeToSystemOut(String str, boolean newLine) {
    if (newLine) {
      System.out.println();
      System.out.println(str);
    } else {
      System.out.print(str);
    }
  }

}
