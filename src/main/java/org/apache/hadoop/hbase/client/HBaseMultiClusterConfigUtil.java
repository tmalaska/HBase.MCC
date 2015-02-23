package org.apache.hadoop.hbase.client;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.*;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.*;
import com.cloudera.api.v1.*;
import com.cloudera.api.v8.RootResourceV8;
import com.cloudera.api.v8.ServicesResourceV8;
import org.apache.cxf.jaxrs.ext.multipart.InputStreamDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;

public class HBaseMultiClusterConfigUtil {
  
  static Logger LOG = Logger.getLogger(HBaseMultiClusterConfigUtil.class);
  static final String PRIMARY_NAME = "primary";
  
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("HBaseMultiClusterUtil <command> <args ....>");
      System.out.println("HBaseMultiClusterUtil combineConfigs <primary conf 1> <failover name> <failover conf 1> <optional failover name N> <optional failover conf N>");
      System.out.println("HBaseMultiClusterUtil splitConfigs <configuration file>");
      System.out.println("HBaseMultiClusterUtil combinConfigsFromCM <host1> <user1> <pwd1> <cluster1> <hbaseName1> <host2> <user2> <pwd2> <cluster2> <hbaseName2> <outputFile>");
      return;
    }
    System.out.println("Command: " + args[0]);
    if (args[0].equals("combineConfigs")) {
      String outputFile = args[1];
      
      Configuration primaryConfig = generateCombinedConfig(args);
      
      LOG.info("Writting Out New Primary");
      primaryConfig.writeXml(new BufferedWriter(new FileWriter(new File(outputFile))));
      LOG.info(" - Successful Written Out New Primary");
    } else if (args[0].equals("splitConfigs")) {
      
      Configuration config = HBaseConfiguration.create();
      config.addResource(new FileInputStream(new File(args[1])));
      
      OutputStream ops2 = new StringOutputStream();
      
      config.writeXml(ops2);
      //System.out.println(ops2.toString());
      
      splitMultiConfigFile(config);
    } else if (args[0].equals("splitConfigs")) {
      LOG.info("Unknown command: " + args[0]);
    } else if (args[0].equals("combinConfigsFromCM")) {

      //<host1> <user1> <pwd1> <cluster1> <hbaseName1> <host2> <user2> <pwd2> <cluster2> <hbaseName2> <outputFile>

      String host1 = args[1];
      String user1 = args[2];
      String pwd1  = args[3];
      String cluster1 = args[4];
      String hbaseName1 = args[5];
      String host2 = args[6];
      String user2 = args[7];
      String pwd2  = args[8];
      String cluster2 = args[9];
      String hbaseName2 = args[10];
      String outputFile = args[11];


      Configuration config = combineConfigurations(host1, user1, pwd1,
              cluster1, hbaseName1,
              host2, user2, pwd2,
              cluster2, hbaseName2);

      LOG.info("Writting Out New Primary");
      config.writeXml(new BufferedWriter(new FileWriter(new File(outputFile))));
      LOG.info(" - Successful Written Out New Primary");
    }
  }
  
  /**
   * This method will take a multi hbase config and produce all the single config files
   * @param config
   * @return
   */
  public static Map<String, Configuration> splitMultiConfigFile(Configuration config) {
    
    Map<String, Configuration> results = new HashMap<String, Configuration>();
    
    Collection<String> failoverNames = config.getStringCollection(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG);
    
    System.out.println("FailoverNames: " + failoverNames.size() + " " + config.get(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG));
    if (failoverNames.size() == 0) {
      results.put(PRIMARY_NAME, config);
    } else {
      
      // add failover configs
      for (String failoverName: failoverNames) {
        System.out.println("spliting: " + failoverName);
        Configuration failoverConfig = new Configuration(config);
        
        results.put(failoverName, failoverConfig);
        
        failoverConfig.unset(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG);
        
        Iterator<Entry<String, String>> it = failoverConfig.iterator();
        while (it.hasNext()) {
          Entry<String, String> keyValue = it.next();
          if (keyValue.getKey().startsWith(ConfigConst.HBASE_FAILOVER_CLUSTER_CONFIG)) {
            //System.out.println("adding: " + failoverName + " " + keyValue.getKey() + " " + keyValue.getValue());
            failoverConfig.set(keyValue.getKey().substring(ConfigConst.HBASE_FAILOVER_CLUSTER_CONFIG.length() + 2 + failoverName.length()), keyValue.getValue());
            failoverConfig.unset(keyValue.getKey());
          }
        }
      }
      
      //clean up primary config
      Iterator<Entry<String, String>> it = config.iterator();
      //config.unset(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG);
      while (it.hasNext()) {
        Entry<String, String> keyValue = it.next();
        if (keyValue.getKey().startsWith(ConfigConst.HBASE_FAILOVER_CLUSTER_CONFIG)) {
          //config.unset(keyValue.getKey());
        }
      }
      results.put(PRIMARY_NAME, config);
    }

    //print configs
    //
    /*
    for (Entry<String, Configuration> entry: results.entrySet()) {
      System.out.println("Config: " + entry.getKey());
      OutputStream ops = new StringOutputStream();
      try {
        entry.getValue().writeXml(ops);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      System.out.println(ops.toString());
    }
    */
    
    return results;
  }
  
  /**
   * This method will take args and up load config files to produce a single
   * multi hbase cluster config file.
   * 
   * The args should be like this.
   * 
   * args[0] commandName (not used here)
   * args[1] output file (not used here)
   * args[2] primary file
   * args[3] failover 1 name
   * args[4] failover 1 file
   * args[3+N*2] optional failover N name
   * args[4+N*2] optional failover N file
   * @param args
   * @return
   * @throws IOException
   */
  public static Configuration generateCombinedConfig(String[] args) throws IOException {
    String primaryConfigFile = args[2];
    HashMap<String, String> failoverConfigNameAndFiles = new HashMap<String, String>();
    for (int i = 3; i < args.length; i+=2) {
      failoverConfigNameAndFiles.put(args[i], args[i+1]);
    }
    return generateCombinedConfig(primaryConfigFile, failoverConfigNameAndFiles);
  }
  
  /**
   * This method will load config files to produce a single
   * multi hbase cluster config file.
   * 
   * The args should be like this.
   * 
   */
  public static Configuration generateCombinedConfig(String primaryConfigFile, Map<String, String> failoverConfigNameAndFiles) throws IOException {

    Configuration primaryConfig = HBaseConfiguration.create();
    primaryConfig.addResource(primaryConfigFile);

    HashMap<String, Configuration> failoverMap = new HashMap<String, Configuration>();
    for (Entry<String, String> entry: failoverConfigNameAndFiles.entrySet()) {
      Configuration failureConfig = HBaseConfiguration.create();
      failureConfig.addResource(entry.getValue());
      failoverMap.put(entry.getKey(), failureConfig);
    }
    return combineConfigurations(primaryConfig, failoverMap);
  }

  public static Configuration combineConfigurations(Configuration primary, Configuration failover ) {
    Map<String, Configuration> map = new HashMap<String, Configuration>();
    map.put("failover", failover);
    return combineConfigurations(primary, map);
  }

  // This method obtains the Hbase configuration from 2 clusters and combines
  // them
  public static Configuration combineConfigurations(String host1, String user1, String pwd1,
                                                    String cluster1, String hbaseName1,
                                                    String host2, String user2, String pwd2,
                                                    String cluster2, String hbaseName2) throws IOException {

    // Get the Cluster 1 Hbase Config
    LOG.info("Getting cluster 1 config");
    Configuration hbaseConf1 = getHbaseServiceConfigs(host1, user1, pwd1,
            cluster1, hbaseName1);

    // System.out.println(hbaseConf1);

    // Get the Cluster 2 Hbase Config
    LOG.info("Getting cluster 2 config");
    Configuration hbaseConf2 = getHbaseServiceConfigs(host2, user2, pwd2,
            cluster2, hbaseName2);

    // Call Combine Configuration
    return combineConfigurations(hbaseConf1, hbaseConf2);

  }

  // Method to obtain Hbase service config information using CM Rest API
  public static Configuration getHbaseServiceConfigs(String host,
                                                     String user, String pwd, String cluster, String hbaseName) throws IOException {


    System.out.println("host:" + host);
    System.out.println("user:" + user);
    System.out.println("pwd:" + pwd);
    // Obtain the Root Resource for the CM API
    RootResourceV8 apiRoot = new ClouderaManagerClientBuilder()
            .withHost(host).withUsernamePassword(user, pwd).build()
            .getRootV8();
    System.out.println("---");
    // Get the services for the cluster

    System.out.println("cluster:" + cluster);
    ServicesResourceV8 servicesResource = apiRoot.getClustersResource()
            .getServicesResource(cluster);

    // Get the Service Config
    System.out.println("hbaseName:" + hbaseName);

    for (ApiService apiService : servicesResource.readServices(DataView.FULL).getServices()) {
      System.out.println("Name:" + apiService.getName());
    }

    InputStreamDataSource clientConfig = servicesResource.getClientConfig(hbaseName.toLowerCase());
/*
    try {

      BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(new File("Config" + hbaseName + ".zip")));
      byte[] byteArray = new byte[1000];

      InputStream inputStream = clientConfig.getInputStream();

      int read = 0;
      while ((read = inputStream.read(byteArray)) > 0) {
        outputStream.write(byteArray, 0, read);
      }
      outputStream.close();
      inputStream.close();


    } catch (IOException e) {
      e.printStackTrace();
    }


    ZipFile zipFile = new ZipFile(new File("Config" + hbaseName + ".zip"));

    Configuration hbaseConf = new Configuration();

    hbaseConf.addResource(zipFile.getInputStream(zipFile.getEntry("hbase-conf/core-site.xml")));
    hbaseConf.addResource(zipFile.getInputStream(zipFile.getEntry("hbase-conf/hbase-site.xml")));
    hbaseConf.addResource(zipFile.getInputStream(zipFile.getEntry("hbase-conf/hdfs-site.xml")));
*/
    Configuration hbaseConf = new Configuration();

    final ZipInputStream inputStream = new ZipInputStream(clientConfig.getInputStream());
    ZipEntry zipEntry = null;
    while ((zipEntry = inputStream.getNextEntry()) != null) {
      if (zipEntry.getName().contains("core-site.xml") ||
              zipEntry.getName().contains("hbase-site.xml") ||
              zipEntry.getName().contains("hdfs-site.xml")) {
        System.out.println("Reading: " + zipEntry.getName());

        hbaseConf.addResource(new InputStream() {
          @Override
          public int read() throws IOException {
            return inputStream.read();
          }

          @Override
          public void close() throws IOException {
            inputStream.closeEntry();
          }
        });
        hbaseConf.get("hbase.zookeeper.quorum");
      } else {
        System.out.println("Not Reading: " + zipEntry.getName());
        inputStream.closeEntry();
      }

    }
    inputStream.close();
    System.out.println("hbase.zookeeper.quorum: " + hbaseConf.get("hbase.zookeeper.quorum"));
    return hbaseConf;

  } // end getHbaseServiceConfig()


  public static Configuration combineConfigurations(Configuration primary, Map<String, Configuration> failovers ) {

    Configuration resultingConfig = new Configuration();
    resultingConfig.clear();

    boolean isFirst = true;
    StringBuilder failOverClusterNames = new StringBuilder();


    Iterator<Entry<String, String>> primaryIt =  primary.iterator();

    while(primaryIt.hasNext()) {
      Entry<String, String> primaryKeyValue = primaryIt.next();
      resultingConfig.set(primaryKeyValue.getKey().replace('_', '.'), primaryKeyValue.getValue());
    }


    for (Entry<String, Configuration> failover: failovers.entrySet()) {
      if (isFirst) {
        isFirst = false;
      } else {
        failOverClusterNames.append(",");
      }
      failOverClusterNames.append(failover.getKey());

      Configuration failureConfig = failover.getValue();

      Iterator<Entry<String, String>> it = failureConfig.iterator();
      while (it.hasNext()) {
        Entry<String, String> keyValue = it.next();

        LOG.info(" -- Looking at : " + keyValue.getKey() + "=" + keyValue.getValue());

        String configKey = keyValue.getKey().replace('_', '.');

        if (configKey.startsWith("hbase.")) {
          resultingConfig.set(ConfigConst.HBASE_FAILOVER_CLUSTER_CONFIG + "." + failover.getKey() + "." + configKey , keyValue.getValue());
          LOG.info(" - Porting config: " + configKey + "=" + keyValue.getValue());
        }
      }
    }

    resultingConfig.set(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG, failOverClusterNames.toString());

    return resultingConfig;
  }
  
  public static class StringOutputStream extends OutputStream {

    private StringBuilder string = new StringBuilder();
    @Override
    public void write(int b) throws IOException {
        this.string.append((char) b );
    }

    //Netbeans IDE automatically overrides this toString()
    public String toString(){
        return this.string.toString();
    }    
  }
}
