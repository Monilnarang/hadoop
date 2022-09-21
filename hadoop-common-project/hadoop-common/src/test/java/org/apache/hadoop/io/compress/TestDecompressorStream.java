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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assume;import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDecompressorStream {

  @Parameterized.Parameter(0)
  public String TEST_STRING;

  @Parameterized.Parameters
  public static Collection<Object> testData() {
    Object[][] data = new Object[][] { {"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"},
                                       {"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"},
                                       {"!@#$%^&*()_+-=[]}{';:<>?/.,1234567890qwertyuioplkjhgfdsazxcvbnm"},
                                       {"qwertyuioplkjhgfdsazxcvbnm"},
                                       {"1234567890"},
                                       {"                                                                "},
                                       {"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                                       {"1234567890123456789012345"},
                                       {"12345678901234567890123456"},
                                       {"=-_+)(*&^%$#@!`~[]{}|';:/.,<>?!@#$%^&*()_"},
                                       {" 1 1 1 1 1 1 1 1 1 1 1 1 1 1"},
                                       {""},
                                       {null},
        };
    return Arrays.asList(data);
    }

  private ByteArrayInputStream bytesIn;
  private Decompressor decompressor;
  private DecompressorStream decompressorStream;

  @Before
  public void setUp() throws IOException {
    Assume.assumeTrue(TEST_STRING != null);
    bytesIn = new ByteArrayInputStream(TEST_STRING.getBytes());
    decompressor = new FakeDecompressor();
    decompressorStream =
        new DecompressorStream(bytesIn, decompressor, 20, 13);
  }

  @Test
  public void testReadOneByte() throws IOException {
    for (int i = 0; i < TEST_STRING.length(); ++i) {
      assertThat(decompressorStream.read()).isEqualTo(TEST_STRING.charAt(i));
    }
    try {
      int ret = decompressorStream.read();
      fail("Not reachable but got ret " + ret);
    } catch (EOFException e) {
      // Expect EOF exception
    }
  }

  @Test
  public void testReadBuffer() throws IOException {
    // 32 buf.length < 52 TEST_STRING.length()
    byte[] buf = new byte[32];
    int bytesToRead = TEST_STRING.length();
    int i = 0;
    while (bytesToRead > 0) {
      int n = Math.min(bytesToRead, buf.length);
      int bytesRead = decompressorStream.read(buf, 0, n);
      assertTrue(bytesRead > 0 && bytesRead <= n);
      assertThat(new String(buf, 0, bytesRead))
          .isEqualTo(TEST_STRING.substring(i, i + bytesRead));
      bytesToRead = bytesToRead - bytesRead;
      i = i + bytesRead;
    }
    try {
      int ret = decompressorStream.read(buf, 0, buf.length);
      fail("Not reachable but got ret " + ret);
    } catch (EOFException e) {
      // Expect EOF exception
    }
  }

  @Test
  public void testSkip() throws IOException {
    Assume.assumeTrue(TEST_STRING.length() > 25);
    assertThat(decompressorStream.skip(12)).isEqualTo(12L);
    assertThat(decompressorStream.read()).isEqualTo(TEST_STRING.charAt(12));
    assertThat(decompressorStream.read()).isEqualTo(TEST_STRING.charAt(13));
    assertThat(decompressorStream.read()).isEqualTo(TEST_STRING.charAt(14));
    assertThat(decompressorStream.skip(10)).isEqualTo(10L);
    assertThat(decompressorStream.read()).isEqualTo(TEST_STRING.charAt(25));
    try {
      long ret = decompressorStream.skip(1000);
      fail("Not reachable but got ret " + ret);
    } catch (EOFException e) {
      // Expect EOF exception
    }
  }
}