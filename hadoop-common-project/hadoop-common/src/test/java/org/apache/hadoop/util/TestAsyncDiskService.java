/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.Collection;

/**
 * A test for AsyncDiskService.
 */
@RunWith(Parameterized.class)
public class TestAsyncDiskService {
  
  public static final Logger LOG =
      LoggerFactory.getLogger(TestAsyncDiskService.class);
  
  // Access by multiple threads from the ThreadPools in AsyncDiskService.
  volatile int count;
  
  /** An example task for incrementing a counter.  
   */
  class ExampleTask implements Runnable {

    ExampleTask() {
    }
    
    @Override
    public void run() {
      synchronized (TestAsyncDiskService.this) {
        count ++;
      }
    }
  };
  
  @Parameterized.Parameter(value = 0)
  public int total;

  @Parameterized.Parameters
      public static Collection<Object[]> data() {
        Object[][] data = new Object[][] { {100}, {0}, {250}, {3}, {-1}, {Integer.MAX_VALUE}, {Integer.MIN_VALUE}
        };

        return Arrays.asList(data);
      }

  /**
   * This test creates some ExampleTasks and runs them. 
   */
  // PUTs #64
  @Test(timeout = 1000)
  public void testAsyncDiskService() throws Throwable {
    Assume.assumeTrue(total >= 0 &&  total < 10000);
    String[] vols = new String[]{"/0", "/1"};
    AsyncDiskService service = new AsyncDiskService(vols);

    for (int i = 0; i < total; i++) {
      service.execute(vols[i%2], new ExampleTask());
    }

    Exception e = null;
    try {
      service.execute("no_such_volume", new ExampleTask());
    } catch (RuntimeException ex) {
      e = ex;
    }
    assertNotNull("Executing a task on a non-existing volume should throw an "
        + "Exception.", e);
    
    service.shutdown();
    if (!service.awaitTermination(5000)) {
      fail("AsyncDiskService didn't shutdown in 5 seconds.");
    }
    
    assertEquals(total, count); // used local variable
  }
}
