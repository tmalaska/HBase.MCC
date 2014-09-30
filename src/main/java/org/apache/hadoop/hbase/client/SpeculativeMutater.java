package org.apache.hadoop.hbase.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SpeculativeMutater {
  static final Log LOG = LogFactory.getLog(SpeculativeMutater.class);

  static ExecutorService exe = Executors.newFixedThreadPool(200);
  
  
  public static Boolean mutate(final long waitToSendFailover, final long waitToSendFailoverWithException, final Callable<Void> primaryCallable,
      final List<Callable<Void>> failoverCallables, AtomicLong lastPrimaryFail) {
    
    ExecutorCompletionService<Boolean> exeS = new ExecutorCompletionService<Boolean>(exe);
    
    ArrayList<Callable<Boolean>> callables = new ArrayList<Callable<Boolean>>();
    
    final AtomicBoolean isPrimarySuccess = new AtomicBoolean(false);
    final long startTime = System.currentTimeMillis();
    final long lastPrimaryFinalFail = lastPrimaryFail.get();
    
    if (System.currentTimeMillis() - lastPrimaryFinalFail > 5000) {
      callables.add(new Callable<Boolean>() {
        public Boolean call() throws Exception {
          
          primaryCallable.call();
          isPrimarySuccess.set(true);
          return true; 
        }
      });
    }
    

    for (final Callable<Void> failoverCallable : failoverCallables) {
      callables.add(new Callable<Boolean>() {

        public Boolean call() throws Exception {
          long waitToRequest = (System.currentTimeMillis() - lastPrimaryFinalFail > 5000)?
              waitToSendFailover - (System.currentTimeMillis() - startTime): waitToSendFailoverWithException - (System.currentTimeMillis() - startTime);
              
              
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
      //Boolean result = exe.invokeAny(callables);
      
      for (Callable<Boolean> call: callables) {
        exeS.submit(call);
      }
      Boolean result = exeS.take().get();
      
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
}
