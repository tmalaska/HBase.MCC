package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class HConnectionManagerMultiClusterWrapper {

  public static HConnection createConnection(Configuration conf)
      throws IOException {
    Collection<String> failoverClusters = conf
        .getStringCollection(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG);
    if (failoverClusters.size() == 0) {
      System.out.println(" -- Getting a signle cluster connection !!");
      return HConnectionManager.createConnection(conf);
    } else {

      Map<String, Configuration> configMap = HBaseMultiClusterConfigUtil
          .splitMultiConfigFile(conf);

      System.out.println(" -- Getting primary Connction");
      HConnection primaryConnection = HConnectionManager
          .createConnection(configMap
              .get(HBaseMultiClusterConfigUtil.PRIMARY_NAME));
      System.out.println(" --- Got primary Connction");

      ArrayList<HConnection> failoverConnections = new ArrayList<HConnection>();

      for (Entry<String, Configuration> entry : configMap.entrySet()) {
        if (!entry.getKey().equals(HBaseMultiClusterConfigUtil.PRIMARY_NAME)) {
          System.out.println(" -- Getting failure Connction");
          failoverConnections.add(HConnectionManager.createConnection(entry
              .getValue()));
          System.out.println(" --- Got failover Connction");
        }
      }
      
      return new HConnectionMultiCluster(conf, primaryConnection,
          failoverConnections.toArray(new HConnection[0]));
    }
  }
}
