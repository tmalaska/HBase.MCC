package org.apache.hadoop.hbase.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SpeculativeRequester<T extends Object> {

  long waitTimeBeforeRequestingFailover;
  long waitTimeBeforeAcceptingResults;
  ExecutorService executor;
  static final Log LOG = LogFactory.getLog(SpeculativeRequester.class);

  public SpeculativeRequester(long waitTimeBeforeRequestingFailover,
      long waitTimeBeforeAcceptingResults,
      ExecutorService executor) {
    this.waitTimeBeforeRequestingFailover = waitTimeBeforeRequestingFailover;
    this.waitTimeBeforeAcceptingResults = waitTimeBeforeAcceptingResults;
    this.executor = executor;
  }

  public ResultWrapper<T> request(final Callable<T> primaryCallable,
      final List<Callable<T>> failoverCallables) {

    final AtomicBoolean isPrimarySuccess = new AtomicBoolean(false);
    final long startTime = System.currentTimeMillis();

    ArrayList<Callable<ResultWrapper<T>>> callables = new ArrayList<Callable<ResultWrapper<T>>>();

    callables.add(new Callable<ResultWrapper<T>>() {
      public ResultWrapper<T> call() throws Exception {
        T t = primaryCallable.call();
        isPrimarySuccess.set(true);
        return new ResultWrapper(true, t);
      }
    });

    for (final Callable<T> failoverCallable : failoverCallables) {
      callables.add(new Callable<ResultWrapper<T>>() {

        public ResultWrapper<T> call() throws Exception {
          long waitToRequest = waitTimeBeforeRequestingFailover
              - (System.currentTimeMillis() - startTime);
          if (waitToRequest > 0) {
            Thread.sleep(waitToRequest);
          }
          if (isPrimarySuccess.get() == false) {
            T t = failoverCallable.call();

            long waitToAccept = waitTimeBeforeAcceptingResults
                - (System.currentTimeMillis() - startTime);

            if (waitToAccept > 0) {
              Thread.sleep(waitToAccept);
            }

            return new ResultWrapper(false, t);
          } else {
            Thread.sleep(60000);
            return null;
          }

        }
      });
    }
    try {
      return executor.invokeAny(callables);
    } catch (InterruptedException e) {
      e.printStackTrace();
      LOG.error(e);
    } catch (ExecutionException e) {
      e.printStackTrace();
      LOG.error(e);
    }
    return null;
  }
  
  public static class ResultWrapper<T> {
    public Boolean isPrimary;
    public T t;
    
    public ResultWrapper(Boolean isPrimary, T t) {
      this.isPrimary = isPrimary;
      this.t = t;
    }
  }
}