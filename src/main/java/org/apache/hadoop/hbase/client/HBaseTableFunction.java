package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.client.HTableInterface;

public interface HBaseTableFunction<T> {
  public T call(HTableInterface table) throws Exception;
}
