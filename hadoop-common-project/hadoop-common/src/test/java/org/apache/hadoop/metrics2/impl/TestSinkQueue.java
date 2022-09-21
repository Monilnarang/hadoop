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

package org.apache.hadoop.metrics2.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import static org.apache.hadoop.metrics2.impl.SinkQueue.*;

/**
 * Test the half-blocking metrics sink queue
 */
@RunWith(Parameterized.class)
public class TestSinkQueue {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSinkQueue.class);

  @Parameterized.Parameter(0)
  public int enqueueValue1;

  @Parameterized.Parameter(1)
  public int enqueueValue2;

  @Parameterized.Parameter(2)
  public int enqueueValue3;

  @Parameterized.Parameter(3)
    public int awhile1;

  @Parameterized.Parameter(4)
    public int awhile2;

  @Parameterized.Parameter(5)
    public int capacity;

  @Parameterized.Parameters
  public static Collection<Object> testData() {
    Object[][] data = new Object[][] { {1, 2, 3, 0, 100, 64},
                                       {5, 13, 19, 1, 7, 15},
                                       {2147483647, 0, -2147483648, 1100, -2147483648, 1000},
                                       {-1, -1, -1, -1, -1, -1},
                                       {0, 0, 0, 0, 0, 0},
        };
        return Arrays.asList(data);
    }

  /**
   * Test common use case
   * @throws Exception
   */
   // Class #1 PUT #1
  @Test public void testCommon() throws Exception {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(2);
    q.enqueue(enqueueValue1);
    assertEquals("queue front", enqueueValue1, (int) q.front()); // used parameter directly
    assertEquals("queue back", enqueueValue1, (int) q.back()); // used parameter directly
    assertEquals("element", enqueueValue1, (int) q.dequeue()); // used parameter directly

    assertTrue("should enqueue", q.enqueue(enqueueValue2)); // used parameter directly
    q.consume(new Consumer<Integer>() {
      @Override public void consume(Integer e) {
        assertEquals("element", enqueueValue2, (int) e);
      }
    });
    assertTrue("should enqueue", q.enqueue(enqueueValue3));     //can be removed // used parameter directly
    assertEquals("element", enqueueValue3, (int) q.dequeue());    //can be removed // used parameter directly
    assertEquals("queue size", 0, q.size());
    assertEquals("queue front", null, q.front());
    assertEquals("queue back", null, q.back());
  }

  /**
   * Test blocking when queue is empty
   * @throws Exception
   */
   // Class #1 PUT #2
  @Test(timeout = 2000)
  public void testEmptyBlocking() throws Exception {
    testEmptyBlocking(awhile1);
    testEmptyBlocking(awhile2); // remove it already parameterized
  }

  private void testEmptyBlocking(int awhile) throws Exception {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(2);
    final Runnable trigger = mock(Runnable.class);
    // try consuming emtpy equeue and blocking
    Thread t = new Thread() {
      @Override public void run() {
        try {
          assertEquals("element", enqueueValue1, (int) q.dequeue()); // used parameter directly
          q.consume(new Consumer<Integer>() {
            @Override public void consume(Integer e) {
              assertEquals("element", enqueueValue2, (int) e); // used parameter directly
              trigger.run();
            }
          });
        }
        catch (InterruptedException e) {
          LOG.warn("Interrupted", e);
        }
      }
    };
    t.start();
    // Should work with or without sleep
    if (awhile > 0) {
      Thread.sleep(awhile);
    }
    q.enqueue(enqueueValue1);
    q.enqueue(enqueueValue2);
    t.join();
    verify(trigger).run();
  }

  /**
   * Test nonblocking enqueue when queue is full
   * @throws Exception
   */
  // Class #1 PUT #3
  @Test public void testFull() throws Exception {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(1);
    q.enqueue(enqueueValue1);

    assertTrue("should drop", !q.enqueue(enqueueValue2)); // used parameter directly
    assertEquals("element", enqueueValue1, (int) q.dequeue()); // used parameter directly

    q.enqueue(enqueueValue3);
    q.consume(new Consumer<Integer>() {
      @Override public void consume(Integer e) {
        assertEquals("element", enqueueValue3, (int) e); // used parameter directly
      }
    });
    assertEquals("queue size", 0, q.size());
  }

  /**
   * Test the consumeAll method
   * @throws Exception
   */
  // Class #1 PUT #4
  @Test(timeout = 2000)
  public void testConsumeAll() throws Exception {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(capacity);
    assertTrue("should enqueue", q.enqueue(0));
    for (int i = 1; i < capacity; ++i) {
      assertTrue("should enqueue", q.enqueue(i)); // some manipulation of parameter
    }
    assertTrue("should not enqueue", !q.enqueue(capacity));

    final Runnable trigger = mock(Runnable.class);
    q.consumeAll(new Consumer<Integer>() {
      private int expected = 0;
      @Override public void consume(Integer e) {
        assertEquals("element", expected++, (int) e);
        trigger.run();
      }
    });

    verify(trigger, times(Math.max(capacity, 1))).run();
  }

  /**
   * Test the consumer throwing exceptions
   * @throws Exception
   */
  // Class #1 PUT #5
  @Test public void testConsumerException() throws Exception {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(1);
    final RuntimeException ex = new RuntimeException("expected");
    q.enqueue(enqueueValue1);

    try {
      q.consume(new Consumer<Integer>() {
        @Override public void consume(Integer e) {
          throw ex;
        }
      });
    }
    catch (Exception expected) {
      assertSame("consumer exception", ex, expected);
    }
    // The queue should be in consistent state after exception
    assertEquals("queue size", 1, q.size());
    assertEquals("element", enqueueValue1, (int) q.dequeue()); // used parameter directly
  }

  /**
   * Test the clear method
   */
  // Class #1 PUT #6
  @Test(timeout = 2000)
  public void testClear() {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(capacity);
    for (int i = 0; i < q.capacity() + 97; ++i) {
      q.enqueue(i);
    }
    assertEquals("queue size", q.capacity(), q.size());
    q.clear();
    assertEquals("queue size", 0, q.size());
  }

  /**
   * Test consumers that take their time.
   * @throws Exception
   */
  // Class #1 PUT #7
  @Test public void testHangingConsumer() throws Exception {
    SinkQueue<Integer> q = newSleepingConsumerQueue(2, enqueueValue1, enqueueValue2);
    assertEquals("queue back", enqueueValue2, (int) q.back()); // used parameter directly
    assertTrue("should drop", !q.enqueue(enqueueValue3)); // should not block // used parameter directly
    assertEquals("queue size", 2, q.size());
    assertEquals("queue head", enqueueValue1, (int) q.front()); // used parameter directly
    assertEquals("queue back", enqueueValue2, (int) q.back()); // used parameter directly
  }

  /**
   * Test concurrent consumer access, which is illegal
   * @throws Exception
   */
  // Class #1 PUT #8
  @Test public void testConcurrentConsumers() throws Exception {
    final SinkQueue<Integer> q = newSleepingConsumerQueue(2, enqueueValue1);
    assertTrue("should enqueue", q.enqueue(enqueueValue2)); // used parameter directly
    assertEquals("queue back", enqueueValue2, (int) q.back()); // used parameter directly
    assertTrue("should drop", !q.enqueue(enqueueValue3)); // should not block // used parameter directly
    shouldThrowCME(new Fun() {
      @Override public void run() {
        q.clear();
      }
    });
    shouldThrowCME(new Fun() {
      @Override public void run() throws Exception {
        q.consume(null);
      }
    });
    shouldThrowCME(new Fun() {
      @Override public void run() throws Exception {
        q.consumeAll(null);
      }
    });
    shouldThrowCME(new Fun() {
      @Override public void run() throws Exception {
        q.dequeue();
      }
    });
    // The queue should still be in consistent state after all the exceptions
    assertEquals("queue size", 2, q.size());
    assertEquals("queue front", enqueueValue1, (int) q.front()); // used parameter directly
    assertEquals("queue back", enqueueValue2, (int) q.back()); // used parameter directly
  }

  private void shouldThrowCME(Fun callback) throws Exception {
    try {
      callback.run();
    }
    catch (ConcurrentModificationException e) {
      LOG.info(e.toString());
      return;
    }
    LOG.error("should've thrown CME");
    fail("should've thrown CME");
  }

  private SinkQueue<Integer> newSleepingConsumerQueue(int capacity,
      int... values) throws Exception {
    final SinkQueue<Integer> q = new SinkQueue<Integer>(capacity);
    for (int i : values) {
      q.enqueue(i);
    }
    final CountDownLatch barrier = new CountDownLatch(1);
    Thread t = new Thread() {
      @Override public void run() {
        try {
          Thread.sleep(10); // causes failure without barrier
          q.consume(new Consumer<Integer>() {
            @Override
            public void consume(Integer e) throws InterruptedException {
              LOG.info("sleeping");
              barrier.countDown();
              Thread.sleep(1000 * 86400); // a long time
            }
          });
        }
        catch (InterruptedException ex) {
          LOG.warn("Interrupted", ex);
        }
      }
    };
    t.setName("Sleeping consumer");
    t.setDaemon(true);  // so jvm can exit
    t.start();
    barrier.await();
    LOG.debug("Returning new sleeping consumer queue");
    return q;
  }

  static interface Fun {
    void run() throws Exception;
  }
}
