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
package org.apache.hadoop.fs;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.runner.RunWith;

/** This test makes sure that "DU" does not get to run on each call to getUsed */
@RunWith(JUnitParamsRunner.class)
public class TestDU {
  final static private File DU_DIR = GenericTestUtils.getTestDir("dutmp");
  final static int BLOCK_SIZE = 4096; // kindly update as per system (for mac -> diskutil info / | grep "Block Size" )

  @Before
  public void setUp() {
    assumeFalse(Shell.WINDOWS);
    FileUtil.fullyDelete(DU_DIR);
    assertTrue(DU_DIR.mkdirs());
  }

  @After
  public void tearDown() throws IOException {
      FileUtil.fullyDelete(DU_DIR);
  }

  private void createFile(File newFile, int size) throws IOException {
    // write random data so that filesystems with compression enabled (e.g., ZFS)
    // can't compress the file
    Random random = new Random();
    byte[] data = new byte[size];
    random.nextBytes(data);

    newFile.createNewFile();
    RandomAccessFile file = new RandomAccessFile(newFile, "rws");

    file.write(data);

    file.getFD().sync();
    file.close();
  }

  private Object[] valueSetForWrittenSize() {
    return new Object[] {
                new Object[] {32*1024},
                new Object[] {32*102},
                new Object[] {32*4},
                new Object[] {17*(-4)},
                new Object[] {17*17},
                new Object[] {19},
                new Object[] {29*(-17)},
                new Object[] {0},
                new Object[] {1},
                new Object[] {Integer.MAX_VALUE},
                new Object[] {Integer.MIN_VALUE},
    };
  }

  /**
   * Verify that du returns expected used space for a file.
   * We assume here that if a file system crates a file of size
   * that is a multiple of the block size in this file system,
   * then the used size for the file will be exactly that size.
   * This is true for most file systems.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  @Parameters(method = "valueSetForWrittenSize")
  public void testDU(int writtenSize) throws IOException, InterruptedException {
    // Allow for extra 4K on-disk slack for local file systems
    // that may store additional file metadata (eg ext attrs).
    Assume.assumeTrue(writtenSize >= 0 && writtenSize <= 32*1024*100);
    final int slack = 4*1024;
    File file = new File(DU_DIR, "data");
    createFile(file, writtenSize);

    Thread.sleep(5000); // let the metadata updater catch up

    DU du = new DU(file, 10000, 0, -1);
    du.init();
    long duSize = du.getUsed();
    du.close();

    assertTrue("Invalid on-disk size",
        duSize >= writtenSize &&
        writtenSize <= (duSize + slack));

    //test with 0 interval, will not launch thread
    du = new DU(file, 0, 1, -1);
    du.init();
    duSize = du.getUsed();
    du.close();

    assertTrue("Invalid on-disk size",
        duSize >= writtenSize &&
        writtenSize <= (duSize + slack));

    //test without launching thread
    du = new DU(file, 10000, 0, -1);
    du.init();
    duSize = du.getUsed();

    assertTrue("Invalid on-disk size",
        duSize >= writtenSize &&
        writtenSize <= (duSize + slack));
  }

  private Object[] valueSetForDfsUsedValue() {
    return new Object[] {
                new Object[] {-Long.MAX_VALUE},
                new Object[] {Long.MAX_VALUE},
                new Object[] {32*1024},
                new Object[] {Long.MIN_VALUE * -1},
                new Object[] {Long.MIN_VALUE},
                new Object[] {17*(-17)},
                new Object[] {0}
    };
  }

  @Test
  @Parameters(method = "valueSetForDfsUsedValue")
  public void testDUGetUsedWillNotReturnNegative(long dfsUsedValue) throws IOException {
    File file = new File(DU_DIR, "data");
    assertTrue(file.createNewFile());
    Configuration conf = new Configuration();
    conf.setLong(CommonConfigurationKeys.FS_DU_INTERVAL_KEY, 10000L);
    DU du = new DU(file, 10000L, 0, -1);
    du.incDfsUsed(dfsUsedValue);
    long duSize = du.getUsed();
    assertTrue(String.valueOf(duSize), duSize >= 0L);
  }

  private Object[] valueSetsForTestSetInitialValue() {
    return new Object[] {
                new Object[] {8192, 1024, 3000, 0, 5000},
                new Object[] {8192, 1024, 3000, 2000, 4000},
                new Object[] {8192, 1024, 3000, 1001, 4001},
                new Object[] {4096, 1024, 3000, 1001, 4001},
                new Object[] {16384, 1024, 3000, 1001, 4001},
                new Object[] {8192, 10, 3000, 1001, 4001},
                new Object[] {-8192, 10, 3000, 1001, 4001},
                new Object[] {8192, -100, 3000, 1001, 4001},
                new Object[] {0, 10, 3000, 1001, 4001},
                new Object[] {8192, 0, 3000, 1001, 4001},
                new Object[] {10, 10, 3000, 1001, 4001},
                new Object[] {12, 10, 3000, 1001, 4001},
    };
  }

  @Test
  @Parameters(method = "valueSetsForTestSetInitialValue")
  public void testDUSetInitialValue(int fileSize, long initialUse, long interval, long jitter, long sleep)
        throws IOException {
    Assume.assumeTrue(interval + jitter <= sleep);
    Assume.assumeTrue(fileSize >= 0 && initialUse >= 0);
    Assume.assumeTrue(fileSize % BLOCK_SIZE == 0);
    File file = new File(DU_DIR, "dataX");
    createFile(file, fileSize);
    DU du = new DU(file, interval, jitter, initialUse);
    du.init();
    assertTrue("Initial usage setting not honored", du.getUsed() == initialUse); // used parameter directly

    // wait until the first du runs.
    try {
      Thread.sleep(sleep);
    } catch (InterruptedException ie) {}

    assertTrue("Usage didn't get updated", du.getUsed() == fileSize); // used parameter directly
  }



}
