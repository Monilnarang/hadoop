/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode.rawcoder;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests for the raw erasure coder benchmark tool.
 */
@RunWith(JUnitParamsRunner.class)
public class TestRawErasureCoderBenchmark {

  private Object[] testParameters() {
    return new Object[] {
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.DUMMY_CODER, 2, 100, 1024},
        new Object[] { "decode", RawErasureCoderBenchmark.CODER.DUMMY_CODER, 5, 150, 100},
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.LEGACY_RS_CODER, 2, 80, 200},
        new Object[] { "decode", RawErasureCoderBenchmark.CODER.LEGACY_RS_CODER, 5, 300, 350},
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.RS_CODER, 3, 200, 200},
        new Object[] { "decode", RawErasureCoderBenchmark.CODER.RS_CODER, 4, 135, 20},
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.ISAL_CODER, 5, 300, 64},
        new Object[] { "decode", RawErasureCoderBenchmark.CODER.ISAL_CODER, 6, 200, 128},
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.DUMMY_CODER, 1, -99999, 100},
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.DUMMY_CODER, 1, 10, 0},
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.DUMMY_CODER, 0, 100, 10},
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.DUMMY_CODER, -1, 50, 300},
        new Object[] { "decode", RawErasureCoderBenchmark.CODER.DUMMY_CODER, 5, -10000, 100},
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.LEGACY_RS_CODER, 2, -80, 200},
        new Object[] { "decode", RawErasureCoderBenchmark.CODER.LEGACY_RS_CODER, 5, -300, 350},
        new Object[] { "decode", RawErasureCoderBenchmark.CODER.LEGACY_RS_CODER, 0, 100, 350},
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.RS_CODER, 3, -200, 200},
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.RS_CODER, -1, 250, 100},
        new Object[] { "decode", RawErasureCoderBenchmark.CODER.RS_CODER, 4, -135, 20},
        new Object[] { "encode", RawErasureCoderBenchmark.CODER.ISAL_CODER, 5, -300, 64},
        new Object[] { "decode", RawErasureCoderBenchmark.CODER.ISAL_CODER, 6, -200, 128},
    };
  }

  @Test
  @Parameters(method = "testParameters")
  public void testAllCoders(String opType, RawErasureCoderBenchmark.CODER coder,
                                 int numThreads, int dataSizeMB, int chunkSizeKB) throws Exception {
    Assume.assumeTrue(numThreads > 0 && chunkSizeKB > 0 && dataSizeMB > 0);
    if (coder == RawErasureCoderBenchmark.CODER.ISAL_CODER) {
        Assume.assumeTrue(ErasureCodeNative.isNativeCodeLoaded());
    }
    RawErasureCoderBenchmark.performBench(opType, coder, numThreads, dataSizeMB, chunkSizeKB);
  }

}
