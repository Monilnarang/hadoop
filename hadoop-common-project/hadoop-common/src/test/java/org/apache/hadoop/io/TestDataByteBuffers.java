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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestDataByteBuffers {

  @Parameterized.Parameter(0)
  public static long seed;

  @Parameterized.Parameter(1)
  public int iter;

  @Parameterized.Parameter(2)
  public static int intBound;

  private static final Random RAND = new Random(seed);

  @Parameterized.Parameters
  public static Collection<Object> testData() {
    Object[][] data = new Object[][] { {31L, 1000, 1024},
                                       {100L, 100, 100009},
                                       {1L, 10000, Integer.MAX_VALUE},
                                       {17, -200, Integer.MIN_VALUE},
                                       {-19, 20, -57},
                                       {0, 1500, 3000},
                                       {Long.MAX_VALUE, Integer.MAX_VALUE, 0},
    };
    return Arrays.asList(data);
  }

  private static void readJunk(DataInput in, int iter)
      throws IOException {
    RAND.setSeed(seed);
    for (int i = 0; i < iter; ++i) {
      switch (RAND.nextInt(7)) {
      case 0:
        assertEquals((byte)(RAND.nextInt() & 0xFF), in.readByte()); break;
      case 1:
        assertEquals((short)(RAND.nextInt() & 0xFFFF), in.readShort()); break;
      case 2:
        assertEquals(RAND.nextInt(), in.readInt()); break;
      case 3:
        assertEquals(RAND.nextLong(), in.readLong()); break;
      case 4:
        assertEquals(Double.doubleToLongBits(RAND.nextDouble()),
            Double.doubleToLongBits(in.readDouble()));
        break;
      case 5:
        assertEquals(Float.floatToIntBits(RAND.nextFloat()),
            Float.floatToIntBits(in.readFloat()));
        break;
      case 6:
        int len = RAND.nextInt(intBound);
        byte[] vb = new byte[len];
        RAND.nextBytes(vb);
        byte[] b = new byte[len];
        in.readFully(b, 0, len);
        assertArrayEquals(vb, b);
        break;
      default:
        throw new IOException();
      }
    }
  }

  private static void writeJunk(DataOutput out, int iter)
      throws IOException {
    RAND.setSeed(seed);
    for (int i = 0; i < iter; ++i) {
      switch (RAND.nextInt(7)) {
      case 0:
        out.writeByte(RAND.nextInt()); break;
      case 1:
        out.writeShort((short)(RAND.nextInt() & 0xFFFF)); break;
      case 2:
        out.writeInt(RAND.nextInt()); break;
      case 3:
        out.writeLong(RAND.nextLong()); break;
      case 4:
        out.writeDouble(RAND.nextDouble()); break;
      case 5:
        out.writeFloat(RAND.nextFloat()); break;
      case 6:
        byte[] b = new byte[RAND.nextInt(intBound)];
        RAND.nextBytes(b);
        out.write(b);
        break;
      default:
        throw new IOException();
      }
    }
  }

  @Test
  public void testBaseBuffers() throws IOException {
    Assume.assumeTrue(iter <= 1000 * 1000);
    Assume.assumeTrue(intBound > 0 && intBound <= 1024 * 1024);
    DataOutputBuffer dob = new DataOutputBuffer();
    writeJunk(dob, iter);
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), 0, dob.getLength());
    readJunk(dib, iter);

    dob.reset();
    writeJunk(dob, iter);
    dib.reset(dob.getData(), 0, dob.getLength());
    readJunk(dib, iter);
  }

  @Test
  public void testDataInputByteBufferCompatibility() throws IOException {
    Assume.assumeTrue(iter <= 1000 * 1000);
    Assume.assumeTrue(intBound > 0 && intBound <= 1024 * 1024);
    DataOutputBuffer dob = new DataOutputBuffer();
    writeJunk(dob, iter);
    ByteBuffer buf = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    DataInputByteBuffer dib = new DataInputByteBuffer();
    dib.reset(buf);
    readJunk(dib, iter);
  }

}
