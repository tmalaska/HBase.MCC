HBase.MCC
-----------------------------

=== GoalThis design document is to offer a solution for a common implementation of this multi-HBase client that will allow for the following benefits:* Be able to switch between single HBase clusters to Multi-HBase Client with limited or no code changes.  This means using the HConnectionManager, Connection, and Table, (Connection and Table hich are the new classes for HConnection, and HTableInterface in HBase version 0.99).* Offer thresholds to allow developers to decide between degrees of strongly consistent and eventually consistent.* Support N number of linked HBase Clusters

=== Initial Results

![alt tag](https://raw.githubusercontent.com/tmalaska/HBase.MCC/master/AveragePutTimeWithMultiRestartsAndShutDowns.png)

The image above is the initial results of a 20k put test with two clusters.  The blue lines are the average time for the puts to commits to a Cluster.  Now the red lines are puts to the primary cluster and green lines are puts that are send to the failover cluster.  When you see green lines they are the results of the primary cluster being shut down or bounced. The cool thing here is we are keeping a pretty good average even in the case of extreme failure.  There is an increase in the time to put to the failover cluster but that is because I set the parameters to only go to the failover cluster after waiting 20 milliseconds.=== Design Doc
There is a design doc in the root folder named MultiHBaseClientDesignDoc.docx.  That document goes over the approach and the configuration options.

