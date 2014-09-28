package org.apache.hadoop.hbase.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SpeculativeMutater {
  static final Log LOG = LogFactory.getLog(SpeculativeMutater.class);
  
  public static Boolean mutate(final long waitToSendFailover, final long waitToSendFailoverWithException, ExecutorService executor, final Callable<Void> primaryCallable,
      final List<Callable<Void>> failoverCallables, AtomicLong lastPrimaryFail) {
    
    ArrayList<Callable<Boolean>> callables = new ArrayList<Callable<Boolean>>();
    
    final AtomicBoolean isPrimarySuccess = new AtomicBoolean(false);
    final long startTime = System.currentTimeMillis();
    final long lastPrimaryFinalFail = lastPrimaryFail.get();
    
    callables.add(new Callable<Boolean>() {
      public Boolean call() throws Exception {
        primaryCallable.call();
        isPrimarySuccess.set(true);
        return true; 
      }
    });
    

    for (final Callable<Void> failoverCallable : failoverCallables) {
      callables.add(new Callable<Boolean>() {

        public Boolean call() throws Exception {
          long waitToRequest = (System.currentTimeMillis() - lastPrimaryFinalFail > 5000)?
              waitToSendFailover - (System.currentTimeMillis() - startTime): waitToSendFailoverWithException - (System.currentTimeMillis() - startTime);
              
              //System.out.println((System.currentTimeMillis() - lastPrimaryFinalFail > 5000) + ":" + lastPrimaryFinalFail + ":" + waitToRequest);
              
          if (waitToRequest > 0) {
            Thread.sleep(waitToRequest);
          }
          if (isPrimarySuccess.get() == false) {
            //System.out.print("x");
            failoverCallable.call();
            //System.out.print("X");
          }
          return false;
        }
      });
    }
    try {
      Boolean result = executor.invokeAny(callables);
      
      executor.shutdownNow();
      
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
}
