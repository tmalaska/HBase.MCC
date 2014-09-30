package org.apache.hadoop.hbase.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SpeculativeRequester<T extends Object> {

  long waitTimeBeforeRequestingFailover;
  long waitTimeBeforeAcceptingResults;
  long lastPrimaryFail;
  static final Log LOG = LogFactory.getLog(SpeculativeRequester.class);

  static ExecutorService exe = Executors.newFixedThreadPool(200);
  
  public SpeculativeRequester(long waitTimeBeforeRequestingFailover,
      long waitTimeBeforeAcceptingResults, AtomicLong lastPrimaryFail) {
    this.waitTimeBeforeRequestingFailover = waitTimeBeforeRequestingFailover;
    this.waitTimeBeforeAcceptingResults = waitTimeBeforeAcceptingResults;
    this.lastPrimaryFail = lastPrimaryFail.get();
  }

  public ResultWrapper<T> request(final Callable<T> primaryCallable,
      final List<Callable<T>> failoverCallables) {

    ExecutorCompletionService<ResultWrapper<T>> exeS = new ExecutorCompletionService<ResultWrapper<T>>(exe);
    
    final AtomicBoolean isPrimarySuccess = new AtomicBoolean(false);
    final long startTime = System.currentTimeMillis();

    ArrayList<Callable<ResultWrapper<T>>> callables = new ArrayList<Callable<ResultWrapper<T>>>();

    if (System.currentTimeMillis() - lastPrimaryFail > 10000) {
      callables.add(new Callable<ResultWrapper<T>>() {
        public ResultWrapper<T> call() throws Exception {
          T t = primaryCallable.call();
          isPrimarySuccess.set(true);
          return new ResultWrapper(true, t);
        }
      });
    }

    for (final Callable<T> failoverCallable : failoverCallables) {
      callables.add(new Callable<ResultWrapper<T>>() {

        public ResultWrapper<T> call() throws Exception {
          
          long waitToRequest = (System.currentTimeMillis() - lastPrimaryFail > 10000)?
              waitTimeBeforeRequestingFailover - (System.currentTimeMillis() - startTime): 0;
              
          
          if (waitToRequest > 0) {
            Thread.sleep(waitToRequest);
          }
          if (isPrimarySuccess.get() == false) {
            T t = failoverCallable.call();

            long waitToAccept = (System.currentTimeMillis() - lastPrimaryFail > 10000)?
                waitTimeBeforeAcceptingResults - (System.currentTimeMillis() - startTime): 0;
            if (isPrimarySuccess.get() == false) {
              if (waitToAccept > 0) {
                Thread.sleep(waitToAccept);
              }
            }

            return new ResultWrapper(false, t);
          } else {
            throw new RuntimeException("Not needed");
          }

        }
      });
    }
    try {

      //ResultWrapper<T> t = exe.invokeAny(callables);
      for (Callable<ResultWrapper<T>> call: callables) {
        exeS.submit(call);
      }
      
      
      
      ResultWrapper<T> result = exeS.take().get();
      //exe.shutdownNow();
      
      return result; 
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