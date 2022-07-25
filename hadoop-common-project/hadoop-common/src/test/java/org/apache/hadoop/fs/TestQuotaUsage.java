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

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Enclosed.class)
public class TestQuotaUsage {

  @RunWith(Parameterized.class)
  public static class TheParameterizedPart {
    @Parameterized.Parameter(value = 0)
    public long fileAndDirCount;
    @Parameterized.Parameter(value = 1)
    public long quota;
    @Parameterized.Parameter(value = 2)
    public long spaceConsumed;
    @Parameterized.Parameter(value = 3)
    public long spaceQuota;
    @Parameterized.Parameter(value = 4)
    public long SSDQuota;

    @Parameterized.Parameters
    public static Collection<Object> testData() {
      Object[][] data = new Object[][] { {22222, 44444, 55555, 66666, 300000},
                                         {22222, 3, 11111, 7, 444444},
                                         {-1, 1, -1, 1, -1},
                                         {222255555, 222256578, 1073741825, 1, 5}
       };
      return Arrays.asList(data);
    }

  // PUTs #19
  // check the full constructor with quota information
  @Test
  public void testConstructorWithQuota() {
    QuotaUsage quotaUsage = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    assertEquals("getFileAndDirectoryCount", fileAndDirCount,
        quotaUsage.getFileAndDirectoryCount());
    assertEquals("getQuota", quota, quotaUsage.getQuota());
    assertEquals("getSpaceConsumed", spaceConsumed,
        quotaUsage.getSpaceConsumed());
    assertEquals("getSpaceQuota", spaceQuota, quotaUsage.getSpaceQuota());
  }

  // PUTs #20
  // check the constructor with quota information
  @Test
  public void testConstructorNoQuota() {
    QuotaUsage quotaUsage = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).
        spaceConsumed(spaceConsumed).build();
    assertEquals("getFileAndDirectoryCount", fileAndDirCount,
        quotaUsage.getFileAndDirectoryCount());
    assertEquals("getQuota", -1, quotaUsage.getQuota());
    assertEquals("getSpaceConsumed", spaceConsumed,
        quotaUsage.getSpaceConsumed());
    assertEquals("getSpaceQuota", -1, quotaUsage.getSpaceQuota());
  }

  // PUTs #21
  // check the toString method with quotas
  @Test
  public void testToStringWithQuota() {
    QuotaUsage quotaUsage = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    String expected = String.format(QuotaUsage.QUOTA_STRING_FORMAT + QuotaUsage.SPACE_QUOTA_STRING_FORMAT,
              quota, (quota-fileAndDirCount), spaceQuota, (spaceQuota - spaceConsumed));
    assertEquals(expected, quotaUsage.toString());
  }

  // PUTs #22
  // check the toString method with quotas
  @Test
  public void testToStringNoQuota() {
    QuotaUsage quotaUsage = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).build();
    String expected = "        none             inf            none"
        + "             inf ";
    assertEquals(expected, quotaUsage.toString());
  }

  // PUTs #23
  // check the equality
  @Test
  public void testCompareQuotaUsage() {
    QuotaUsage quotaUsage1 = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).
        typeConsumed(StorageType.SSD, SSDQuota).
        typeQuota(StorageType.SSD, SSDQuota).
        build();

    QuotaUsage quotaUsage2 = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).
        typeConsumed(StorageType.SSD, SSDQuota).
        typeQuota(StorageType.SSD, SSDQuota).
        build();

    assertEquals(quotaUsage1, quotaUsage2);
  }
  }


  public static class NotParameterizedPart {
  // check the empty constructor correctly initialises the object
  @Test
  public void testConstructorEmpty() {
    QuotaUsage quotaUsage = new QuotaUsage.Builder().build();
    assertEquals("getQuota", -1, quotaUsage.getQuota());
    assertEquals("getSpaceConsumed", 0, quotaUsage.getSpaceConsumed());
    assertEquals("getSpaceQuota", -1, quotaUsage.getSpaceQuota());
  }

  // check the header
  @Test
  public void testGetHeader() {
    String header = "       QUOTA       REM_QUOTA     SPACE_QUOTA "
        + "REM_SPACE_QUOTA ";
    assertEquals(header, QuotaUsage.getHeader());
  }

  // check the toString method with quotas
  // Can be removed #1, covered in PUTs #21
  @Test
  public void testToStringHumanWithQuota() {
    long fileAndDirCount = 222255555;
    long quota = 222256578;
    long spaceConsumed = 1073741825;
    long spaceQuota = 1;

    QuotaUsage quotaUsage = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    String expected = "     212.0 M            1023               1 "
        + "           -1 G ";
    assertEquals(expected, quotaUsage.toString(true));
  }
  }
}
