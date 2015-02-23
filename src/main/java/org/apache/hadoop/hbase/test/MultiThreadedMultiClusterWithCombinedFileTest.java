package org.apache.hadoop.hbase.test;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadedMultiClusterWithCombinedFileTest {
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {


      System.out.println("RunMultiClusterTest " +
              "<combined file> " +
              "<tableName> " +
              "<familyName> " +
              "<numberOfPuts> " +
              "<millisecond of wait> " +
              "<numberOfThreads> " +
              "<outputCsvFile>");
    }

    final String combinedFilePath = args[0];

    System.out.println("--Getting Configurations");

    Configuration config = HBaseConfiguration.create();
    config.addResource(new FileInputStream(combinedFilePath));

    System.out.println("--Got Configuration");

    final String tableName = args[1];
    final String familyName = args[2];
    final int numberOfPuts = Integer.parseInt(args[3]);
    final int millisecondToWait = Integer.parseInt(args[4]);
    final int numberOfThreads = Integer.parseInt(args[5]);
    final String outputCsvFile = args[6];

    System.out.println("Getting HAdmin");

    System.out.println(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG + ": " + config.get(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG));
    System.out.println("hbase.zookeeper.quorum: " + config.get("hbase.zookeeper.quorum"));
    System.out.println("hbase.failover.cluster.fail1.hbase.hstore.compaction.max: " + config.get("hbase.failover.cluster.fail1.hbase.hstore.compaction.max"));

    HBaseAdmin admin = new HBaseAdminMultiCluster(config);

    try {
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
    } catch (Exception e) {
      e.printStackTrace();
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

    final HConnection connection = HConnectionManagerMultiClusterWrapper.createConnection(config);

    System.out.println(" - Got HConnection: " + connection.getClass());

    System.out.println("Getting HTable");

    final AtomicInteger threadFinishCounter = new AtomicInteger(0);

    for (int threadNum = 0; threadNum < numberOfThreads; threadNum++) {

      final BufferedWriter writer = new BufferedWriter(new FileWriter(outputCsvFile + "/thread-" + threadNum + ".csv"));

      final int threadFinalNum = threadNum;

      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Random r = new Random();
            for (int i = 1; i <= numberOfPuts; i++) {
              HTableInterface table = connection.getTable(tableName);
              HTableStats stats = ((HTableMultiCluster) table).getStats();
              stats.printStats(writer, 5000);

              int hash = r.nextInt(10);

              Put put = new Put(Bytes.toBytes(hash + ".key." + StringUtils.leftPad(String.valueOf(i * threadFinalNum), 12)));
              put.add(Bytes.toBytes(familyName), Bytes.toBytes("C"), Bytes.toBytes("Value:" + i * threadFinalNum));
              table.put(put);

              Thread.sleep(millisecondToWait);

              Get get = new Get(Bytes.toBytes(hash + ".key." + StringUtils.leftPad(String.valueOf(i * threadFinalNum), 12)));
              table.get(get);

              Thread.sleep(millisecondToWait);

              Delete delete = new Delete(Bytes.toBytes(hash + ".key." + StringUtils.leftPad(String.valueOf(i * threadFinalNum), 12)));
              table.delete(delete);

              Thread.sleep(millisecondToWait);

              if (numberOfPuts % 10000 == 0) {
                writeToSystemOut("{thread:" + threadFinalNum + ",count=" + i + "}", true);
              } else if (numberOfPuts % 1000 == 0) {
                writeToSystemOut(".", false);
              }
            }
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

    admin.disableTable(TableName.valueOf(tableName));
    admin.deleteTable(TableName.valueOf(tableName));

    connection.close();
    admin.close();
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
