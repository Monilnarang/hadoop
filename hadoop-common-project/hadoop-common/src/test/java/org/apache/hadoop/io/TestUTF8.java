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

package org.apache.hadoop.io;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Unit tests for UTF8. */
@SuppressWarnings("deprecation")
@RunWith(Enclosed.class)
public class TestUTF8 {

  @RunWith(Parameterized.class)
  public static class FirstParameterizedPart {
      private static final Random RANDOM = new Random();

      @Parameterized.Parameter(0)
      public static int randomStringBound;

      @Parameterized.Parameter(1)
      public int iterations;

      @Parameterized.Parameters
      public static Collection<Object> testData() {
        Object[] data = new Object[][] { {100 , 10000},
                                         {1000 , 1000},
                                         {10000 , 100},
                                         {100000 , 10},
                                         {-10 , -10},
                                         {-10 , 10},
                                         {0 , 1},
                                         {10 , -10},
                                         {1 , 0},
        };
        return Arrays.asList(data);
      }

      public static String getTestString() throws Exception {
        StringBuilder buffer = new StringBuilder();
        Assume.assumeTrue(randomStringBound > 0);
        int length = RANDOM.nextInt(randomStringBound);
        for (int i = 0; i < length; i++) {
          buffer.append((char)(RANDOM.nextInt(Character.MAX_VALUE)));
        }
        return buffer.toString();
      }

      @Test
      public void testWritable() throws Exception {
        for (int i = 0; i < iterations; i++) {
          TestWritable.testWritable(new UTF8(getTestString()));
        }
      }

      @Test
      public void testGetBytes() throws Exception {
        for (int i = 0; i < iterations; i++) {

          // generate a random string
          String before = getTestString();
          if (before.length() >= 21845) {
              continue;
          }
          // Check that the bytes are stored correctly in Modified-UTF8 format.
          // Note that the DataInput and DataOutput interfaces convert between
          // bytes and Strings using the Modified-UTF8 format.
          assertEquals(before, readModifiedUTF(UTF8.getBytes(before)));
        }
      }

      private String readModifiedUTF(byte[] bytes) throws IOException {
        final short lengthBytes = (short)2;
        ByteBuffer bb = ByteBuffer.allocate(bytes.length + lengthBytes);
        bb.putShort((short)bytes.length).put(bytes);
        ByteArrayInputStream bis = new ByteArrayInputStream(bb.array());
        DataInputStream dis = new DataInputStream(bis);
        return dis.readUTF();
      }

      @Test
      public void testIO() throws Exception {
        DataOutputBuffer out = new DataOutputBuffer();
        DataInputBuffer in = new DataInputBuffer();

        for (int i = 0; i < iterations; i++) {
          // generate a random string
          String before = getTestString();
          if (before.length() >= 21845) {
              continue;
          }

          // write it
          out.reset();
          UTF8.writeString(out, before);

          // test that it reads correctly
          in.reset(out.getData(), out.getLength());
          String after = UTF8.readString(in);
          assertEquals(before, after);

          // test that it reads correctly with DataInput
          in.reset(out.getData(), out.getLength());
          String after2 = in.readUTF();
          assertEquals(before, after2);
        }

      }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class SecondParameterizedPart {

      private Object[] valueSetForNullEncoding() {
        return new Object[] {
                    new Object[] {new String(new char[] { 0 })},
                    new Object[] {new String(new char[] { 0, 1, 2, 4 })},
                    new Object[] {new String(new char[] { 'a', 'v', 'c', 'a', 't', 'V', 'Q'})},
                    new Object[] {new String(new char[] { 'a', 'v', 'c', 'a', 't', 'V', 'Q', 0, 1, 2, 4})},
                    new Object[] {new String(new char[] { })},
        };
      }

      @Test
      @Parameters(method = "valueSetForNullEncoding")
      public void testNullEncoding(String s) throws Exception {
        DataOutputBuffer dob = new DataOutputBuffer();
        new UTF8(s).write(dob);

        assertEquals(s, new String(dob.getData(), 2, dob.getLength()-2, "UTF-8")); // used parameter directly
      }

      /**
       * Test encoding and decoding of UTF8 outside the basic multilingual plane.
       *
       * This is a regression test for HADOOP-9103.
       */
      @Test
      @Parameters ({
      "\uD83D\uDC31",  // Test using the "CAT FACE" character (U+1F431)
      "\uD83C\uDF92",  // 'SCHOOL SATCHEL' (U+1F392)
      "\uD83C\uDFD3", // 'TABLE TENNIS PADDLE AND BALL' (U+1F3D3)
      "\uD83C\uDFEE", // 'IZAKAYA LANTERN' (U+1F3EE)
      "\uD83D\uDC15", // 'DOG' (U+1F415)
      "\uD83D\uDC25", // 'FRONT-FACING BABY CHICK' (U+1F425)
      "\uD83D\uDC4D", // 'THUMBS UP SIGN' (U+1F44D)
      "\uD83D\uDD1D", // 'TOP WITH UPWARDS ARROW ABOVE' (U+1F51D)

      })
      public void testNonBasicMultilingualPlane(String symbolString) throws Exception {
        // See http://www.fileformat.info/info/unicode/char/1f431/index.htm
        String hexString = Hex.encodeHexString(symbolString.getBytes());
        byte[] bytes = Hex.decodeHex(hexString.toCharArray());

        // This encodes to 4 bytes in UTF-8:
        byte[] encoded = symbolString.getBytes("UTF-8");
        assertEquals(bytes.length, encoded.length); // some manipulation of parameter
        assertEquals(hexString, StringUtils.byteToHexString(encoded)); // some manipulation of parameter

        // Decode back to String using our own decoder
        String roundTrip = UTF8.fromBytes(encoded);
        assertEquals(symbolString, roundTrip); // used parameter directly
      }

      private Object[] valueSetForInvalidOrTruncated() {
        return new Object[] {
                    new Object[] {new byte[] { 0x01, 0x02, (byte)0xff, (byte)0xff, 0x01, 0x02, 0x03, 0x04, 0x05 },
                        "Invalid UTF8 at ffff01020304"}, // invalid UTF8
                    new Object[] {new byte[] { 0x01, 0x02, (byte)0xf8, (byte)0x88, (byte)0x80, (byte)0x80, (byte)0x80,
                        0x04, 0x05 }, "Invalid UTF8 at f88880808004"}, // 5-byte UTF8 sequence, now considered illegal.
                    new Object[] {new byte[] { (byte)0xF0, (byte)0x9F, (byte)0x90 },
                        "Truncated UTF8 at f09f90" }, // Truncated CAT FACE character (3 out of 4 bytes)
                    new Object[] {new byte[] { 0x05, 0x03, (byte)0xff, 0x01, 0x02, 0x03, 0x04, (byte)0xff, 0x05 },
                        "Invalid UTF8 at ff01020304ff"}, // invalid UTF8
                    new Object[] {new byte[] { 0x01, 0x02, (byte)0xf9, (byte)0x33, (byte)0x78, (byte)0x55, (byte)0x22,
                        0x04, 0x05 }, "Invalid UTF8 at f93378552204"}, // 5-byte UTF8 sequence, now considered illegal.
                    new Object[] {new byte[] { (byte)0xF0, (byte)0x9F, (byte)0x94 },
                        "Truncated UTF8 at f09f94" } // Truncated TOP WITH UPWARDS ARROW character (3 out of 4 bytes)
        };
      }

      /**
       * Test invalid and truncated UTF8 throws an appropriate error message.
       */
      @Test
      @Parameters(method = "valueSetForInvalidOrTruncated")
      public void testInvalidAndTruncatedUTF8(byte[] invalidOrTruncated, String expectedText) throws Exception {
        try {
          UTF8.fromBytes(invalidOrTruncated);
          fail("did not throw an exception");
        } catch (UTFDataFormatException utfde) {
          GenericTestUtils.assertExceptionContains(
              expectedText, utfde); // used parameter directly
        }
      }
  }
}
