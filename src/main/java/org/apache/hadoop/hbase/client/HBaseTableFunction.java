package org.apache.hadoop.hbase.client;

public interface HBaseTableFunction<T> {
  public T call(HTableInterface table) throws Exception;
}
