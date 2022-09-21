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
package org.apache.hadoop.io.compress;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

@RunWith(JUnitParamsRunner.class)
public class TestCodecPool {
  private final String LEASE_COUNT_ERR =
      "Incorrect number of leased (de)compressors";
  DefaultCodec codec;

  @Before
  public void setup() {
    this.codec = new DefaultCodec();
    this.codec.setConf(new Configuration());
  }

  @Test(timeout = 10000)
  @Parameters({
    "2, 2",
    "3, -5",
    "0, 0",
    "-1, -1",
    "150, -2"})
  public void testCompressorPoolCountsAndCompressorNotReturnSameInstance(int compressorCount,
                                                                            int checkGetCompressorWhenEmptyCount) {
    // Get #compressorCount compressors and return them
    Set<Compressor> compressors = new HashSet<>();
    for (int i = 0; i < compressorCount; i++) {
        compressors.add(CodecPool.getCompressor(codec));
        assertEquals(LEASE_COUNT_ERR, i + 1,
            CodecPool.getLeasedCompressorsCount(codec)); // some manipulation of parameter
    }
    assertEquals(Math.max(compressorCount, 0), compressors.size()); // formula

    int i = 0;
    for (Compressor compressor : compressors) {
        CodecPool.returnCompressor(compressor);
        assertEquals(LEASE_COUNT_ERR, compressorCount - i - 1, // some manipulation of parameter
            CodecPool.getLeasedCompressorsCount(codec));
        i++;
    }

    Compressor comp = CodecPool.getCompressor(codec);
    CodecPool.returnCompressor(comp);
    for (i = 0; i < checkGetCompressorWhenEmptyCount - 1; i++) {
        CodecPool.returnCompressor(comp);
        assertEquals(LEASE_COUNT_ERR, 0,
            CodecPool.getLeasedCompressorsCount(codec));
    }
  }

  @Test(timeout = 10000)
  @Parameters({
    "2, 2",
    "3, -5",
    "0, 0",
    "-1, -1",
    "150, -2"})
  public void testDecompressorPoolCountsAndNotReturnSameInstance(int decompressorCount,
                                                                    int checkGetDecompressorWhenEmptyCount) {
    // Get #decompressorCount decompressors and return them
    Set<Decompressor> decompressors = new HashSet<>();
    for (int i = 0; i < decompressorCount; i++) {
        decompressors.add(CodecPool.getDecompressor(codec));
        assertEquals(LEASE_COUNT_ERR, i + 1,
            CodecPool.getLeasedDecompressorsCount(codec)); // some manipulation of parameter
    }
    assertEquals(Math.max(decompressorCount, 0), decompressors.size());  // formula

    int i = 0;
    for (Decompressor decompressor : decompressors) {
        CodecPool.returnDecompressor(decompressor);
        assertEquals(LEASE_COUNT_ERR, decompressorCount - i - 1, // some manipulation of parameter
            CodecPool.getLeasedDecompressorsCount(codec));
        i++;
    }

    Decompressor decomp = CodecPool.getDecompressor(codec);
    CodecPool.returnDecompressor(decomp);
    for (i = 0; i < checkGetDecompressorWhenEmptyCount - 1; i++) {
        CodecPool.returnDecompressor(decomp);
        assertEquals(LEASE_COUNT_ERR, 0,
            CodecPool.getLeasedDecompressorsCount(codec));
    }
  }

  @Test(timeout = 10000)
  @Parameters({
    "4, 3",
    "10, 1",
    "10, 2",
    "1, 10",
    "1, 1",
    "-3, -6",
    "0, 0",
    "1, 0",
    "5, -2",
    "1, 2" })
  public void testMultiThreadedCompressorPool(int iterations, int nThreads) throws InterruptedException {
    Assume.assumeTrue(nThreads > 1 && iterations > 0);
    ExecutorService threadpool = Executors.newFixedThreadPool(nThreads);
    final LinkedBlockingDeque<Compressor> queue = new LinkedBlockingDeque<Compressor>(
        iterations);

    Callable<Boolean> consumer = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Compressor c = queue.take();
        CodecPool.returnCompressor(c);
        return c != null;
      }
    };

    Callable<Boolean> producer = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Compressor c = CodecPool.getCompressor(codec);
        queue.put(c);
        return c != null;
      }
    };

    for (int i = 0; i < iterations; i++) {
      threadpool.submit(consumer);
      threadpool.submit(producer);
    }

    // wait for completion
    threadpool.shutdown();
    threadpool.awaitTermination(1000, TimeUnit.SECONDS);

    assertEquals(LEASE_COUNT_ERR, 0, CodecPool.getLeasedCompressorsCount(codec));
  }

  @Test(timeout = 10000)
  @Parameters({
    "4, 3",
    "10, 1",
    "10, 2",
    "1, 10",
    "1, 1",
    "-3, -6",
    "0, 0",
    "1, 0",
    "5, -2",
    "1, 2" })
  public void testMultiThreadedDecompressorPool(int iterations, int nThreads) throws InterruptedException {
    Assume.assumeTrue(nThreads > 1 && iterations > 0);
    ExecutorService threadpool = Executors.newFixedThreadPool(nThreads);
    final LinkedBlockingDeque<Decompressor> queue = new LinkedBlockingDeque<Decompressor>(
        iterations);

    Callable<Boolean> consumer = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Decompressor dc = queue.take();
        CodecPool.returnDecompressor(dc);
        return dc != null;
      }
    };

    Callable<Boolean> producer = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Decompressor c = CodecPool.getDecompressor(codec);
        queue.put(c);
        return c != null;
      }
    };

    for (int i = 0; i < iterations; i++) {
      threadpool.submit(consumer);
      threadpool.submit(producer);
    }

    // wait for completion
    threadpool.shutdown();
    threadpool.awaitTermination(1000, TimeUnit.SECONDS);

    assertEquals(LEASE_COUNT_ERR, 0,
        CodecPool.getLeasedDecompressorsCount(codec));
  }
}
