package org.apache.hadoop.hbase.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;

public class HBaseMultiClusterConfigUtil {
  
  static Logger log = Logger.getLogger(HBaseMultiClusterConfigUtil.class);
  static final String PRIMARY_NAME = "primary";
  
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("HBaseMultiClusterUtil <command> <args ....>");
      System.out.println("HBaseMultiClusterUtil combineConfigs <primary conf 1> <failover name> <failover conf 1> <optional failover name N> <optional failover conf N>");
      System.out.println("HBaseMultiClusterUtil splitConfigs c<configuration file>");
      return;
    }
    
    if (args[0].equals("combineConfigs")) {
      String outputFile = args[1];
      
      Configuration primaryConfig = generateCombinedConfig(args);
      
      log.info("Writting Out New Primary");
      primaryConfig.writeXml(new BufferedWriter(new FileWriter(new File(outputFile))));
      log.info(" - Successful Written Out New Primary");
    } else if (args[0].equals("splitConfigs")) {
      
      Configuration config = HBaseConfiguration.create();
      config.addResource(new FileInputStream(new File(args[1])));
      
      OutputStream ops2 = new StringOutputStream();
      
      config.writeXml(ops2);
      //System.out.println(ops2.toString());
      
      splitMultiConfigFile(config);
    } else {
      log.info("Unknown command: " + args[0]);
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
    
    
    log.info("Opening primary Config");
    Configuration primaryConfig = HBaseConfiguration.create();
    primaryConfig.addResource(primaryConfigFile);
    log.info(" - Successfully Opened primary Config");
    
    StringBuilder failOverClusterNames = new StringBuilder();
    boolean isFirst = true;
    for (Entry<String, String> entry: failoverConfigNameAndFiles.entrySet()) {
      if (isFirst) {
        isFirst = false;
      } else {
        failOverClusterNames.append(",");
      }
      failOverClusterNames.append(entry.getKey());
      
      log.info("Opening failover config: " + entry.getKey() + " " + entry.getValue());
      
      Configuration failureConfig = HBaseConfiguration.create();
      failureConfig.addResource(entry.getValue());
      log.info(" - Successfully Opened primary Config: " + entry.getKey());
      
      Iterator<Entry<String, String>> it = failureConfig.iterator();
      while (it.hasNext()) {
        Entry<String, String> keyValue = it.next();
        
        log.info(" -- Looking at : " + keyValue.getKey() + "=" + keyValue.getValue());
        if (keyValue.getKey().startsWith("hbase.")) {
          primaryConfig.set(ConfigConst.HBASE_FAILOVER_CLUSTER_CONFIG + "." + entry.getKey() + "." + keyValue.getKey(), keyValue.getValue());
          log.info(" - Porting config: " + keyValue.getKey() + "=" + keyValue.getValue());
        }
      }
    }
    primaryConfig.set(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG, failOverClusterNames.toString());
    
    return primaryConfig;
    
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
