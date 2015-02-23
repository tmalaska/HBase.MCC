HBase.MCC
-----------------------------

### GoalThis project is to offer a solution for a multi-HBase client that will allow for the following benefits:* Be able to switch between single HBase clusters to Multi-HBase Client with limited or no code changes.  This means using the HConnectionManager, Connection, and Table, (Connection and Table hich are the new classes for HConnection, and HTableInterface in HBase version 0.99).* Offer thresholds to allow developers to decide between degrees of strongly consistent and eventually consistent.* Support N number of linked HBase Clusters

### Initial Results

![alt tag](https://raw.githubusercontent.com/tmalaska/HBase.MCC/master/AveragePutTimeWithMultiRestartsAndShutDowns.png)

The first graph is the break down of which cluster was being used.  The red areas are when I bounced the primary HBase cluster and it failed to deliver responses.

Note that the average response time is pretty much unaffected. 

Also note there is a hit when the cluster got down but the max delay is at 700 milliseconds.  

More testing will happen in the coming weeks.
### Design Doc
There is a design doc in the root folder named MultiHBaseClientDesignDoc.docx.  That document goes over the approach and the configuration options.

### Latest How to execute
java -cp cxf-rt-transports-http-2.7.5.jar:hbase.multicluster-0.0.1-SNAPSHOT.jar org.apache.hadoop.hbase.test.MultiThreadedMultiClusterWithCmApiTest tedmalaska-hbase-mcc-2-1.ent.cloudera.com admin admin PrimaryCluster HBase tedmalaska-hbase-mcc-2-1.ent.cloudera.com admin admin FailoverCluster HBase2 mcc c 100 100 2 stats.csv

