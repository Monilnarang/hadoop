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

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
@RunWith(Enclosed.class)
public class TestSysInfoWindows {


  static class SysInfoWindowsMock extends SysInfoWindows {
    private long time = SysInfoWindows.REFRESH_INTERVAL_MS + 1;
    private String infoStr = null;
    void setSysinfoString(String infoStr) {
      this.infoStr = infoStr;
    }
    void advance(long dur) {
      time += dur;
    }
    @Override
    String getSystemInfoInfoFromShell() {
      return infoStr;
    }
    @Override
    long now() {
      return time;
    }
  }

  @RunWith(Parameterized.class)
  public static class TheParameterizedPart {
  static final String COMMA = ",";

  @Parameterized.Parameter(value = 0)
  public long virtualMemorySize;
  @Parameterized.Parameter(value = 1)
  public long physicalMemorySize;
  @Parameterized.Parameter(value = 2)
  public long availableVirtualMemorySize;
  @Parameterized.Parameter(value = 3)
  public long availablePhysicalMemorySize;
  @Parameterized.Parameter(value = 4)
  public int numProcessors;
  @Parameterized.Parameter(value = 5)
  public long cpuFrequency;
  @Parameterized.Parameter(value = 6)
  public long cumulativeCpuTime;
  @Parameterized.Parameter(value = 7)
  public long storageBytesRead;
  @Parameterized.Parameter(value = 8)
  public long storageBytesWritten;
  @Parameterized.Parameter(value = 9)
  public long networkBytesRead;
  @Parameterized.Parameter(value = 10)
  public long networkBytesWritten;

  @Parameterized.Parameters
  public static Collection<Object> testData() {
    Object[][] data = new Object[][] { {17177038848L,8589467648L,15232745472L,6400417792L,1,2805000L,6261812L,1234567L,2345678L,3456789L,4567890L},
            {17177038848L,8589467648L,15232745472L,6400417792L,12,2805000L,6261812L,1234567L,2345678L,3456789L,4567890L},
    };
    return Arrays.asList(data);
  }

  // PUTs #29
  @Test(timeout = 10000)
  public void parseSystemInfoString() {
    SysInfoWindowsMock tester = new SysInfoWindowsMock();
    tester.setSysinfoString(
            virtualMemorySize + COMMA + physicalMemorySize + COMMA + availableVirtualMemorySize + COMMA +
             availablePhysicalMemorySize + COMMA + numProcessors + COMMA + cpuFrequency + COMMA + cumulativeCpuTime +
             COMMA + storageBytesRead + COMMA + storageBytesWritten + COMMA + networkBytesRead + COMMA +
             networkBytesWritten + "\r\n");
    // info str derived from windows shell command has \r\n termination
    assertEquals(virtualMemorySize, tester.getVirtualMemorySize());
    assertEquals(physicalMemorySize, tester.getPhysicalMemorySize());
    assertEquals(availableVirtualMemorySize, tester.getAvailableVirtualMemorySize());
    assertEquals(availablePhysicalMemorySize, tester.getAvailablePhysicalMemorySize());
    assertEquals(numProcessors, tester.getNumProcessors());
    assertEquals(numProcessors, tester.getNumCores());
    assertEquals(cpuFrequency, tester.getCpuFrequency());
    assertEquals(cumulativeCpuTime, tester.getCumulativeCpuTime());
    assertEquals(storageBytesRead, tester.getStorageBytesRead());
    assertEquals(storageBytesWritten, tester.getStorageBytesWritten());
    assertEquals(networkBytesRead, tester.getNetworkBytesRead());
    assertEquals(networkBytesWritten, tester.getNetworkBytesWritten());
    // undef on first call
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getCpuUsagePercentage(), 0.0);
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getNumVCoresUsed(), 0.0);
  }

  // PUTs #30
  @Test(timeout = 10000)
  public void refreshAndCpuUsage() throws InterruptedException {
    SysInfoWindowsMock tester = new SysInfoWindowsMock();
    tester.setSysinfoString(
            virtualMemorySize + COMMA + physicalMemorySize + COMMA + availableVirtualMemorySize + COMMA +
            availablePhysicalMemorySize + COMMA + numProcessors + COMMA + cpuFrequency + COMMA + cumulativeCpuTime +
            COMMA + storageBytesRead + COMMA + storageBytesWritten + COMMA + networkBytesRead + COMMA +
            networkBytesWritten + "\r\n");
    // info str derived from windows shell command has \r\n termination
    tester.getAvailablePhysicalMemorySize();
    // verify information has been refreshed
    assertEquals(availablePhysicalMemorySize, tester.getAvailablePhysicalMemorySize());
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getCpuUsagePercentage(), 0.0);
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getNumVCoresUsed(), 0.0);

    tester.setSysinfoString(
            virtualMemorySize + COMMA + physicalMemorySize + COMMA + availableVirtualMemorySize + COMMA +
            (availablePhysicalMemorySize - 1000000000) + COMMA + numProcessors + COMMA + cpuFrequency + COMMA +
            (cumulativeCpuTime + 1200) + COMMA + storageBytesRead + COMMA + storageBytesWritten + COMMA +
            networkBytesRead + COMMA + networkBytesWritten + "\r\n");
    tester.getAvailablePhysicalMemorySize();
    // verify information has not been refreshed
    assertEquals(availablePhysicalMemorySize, tester.getAvailablePhysicalMemorySize());
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getCpuUsagePercentage(), 0.0);
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getNumVCoresUsed(), 0.0);

    // advance clock
    tester.advance(SysInfoWindows.REFRESH_INTERVAL_MS + 1);

    // verify information has been refreshed
    assertEquals(availablePhysicalMemorySize - 1000000000, tester.getAvailablePhysicalMemorySize());
    assertEquals((1200) * 100F /
                 (SysInfoWindows.REFRESH_INTERVAL_MS + 1f) / numProcessors,
                 tester.getCpuUsagePercentage(), 0.0);
    assertEquals((1200) /
                 (SysInfoWindows.REFRESH_INTERVAL_MS + 1f) / 1,
                 tester.getNumVCoresUsed(), 0.0);
  }
  }

  public static class NotParameterizedPart {
  // Can Be Removed #2, covered in PUTs #30
  @Test(timeout = 10000)
  public void refreshAndCpuUsageMulticore() throws InterruptedException {
    // test with 12 cores
    SysInfoWindowsMock tester = new SysInfoWindowsMock();
    tester.setSysinfoString(
        "17177038848,8589467648,15232745472,6400417792,12,2805000,6261812," +
        "1234567,2345678,3456789,4567890\r\n");
    // verify information has been refreshed
    assertEquals(6400417792L, tester.getAvailablePhysicalMemorySize());

    tester.setSysinfoString(
        "17177038848,8589467648,15232745472,5400417792,12,2805000,6263012," +
        "1234567,2345678,3456789,4567890\r\n");
    // verify information has not been refreshed
    assertEquals(6400417792L, tester.getAvailablePhysicalMemorySize());

    // advance clock
    tester.advance(SysInfoWindows.REFRESH_INTERVAL_MS + 1);

    // verify information has been refreshed
    assertEquals(5400417792L, tester.getAvailablePhysicalMemorySize());
    // verify information has been refreshed
    assertEquals((6263012 - 6261812) * 100F /
                 (SysInfoWindows.REFRESH_INTERVAL_MS + 1f) / 12,
                 tester.getCpuUsagePercentage(), 0.0);
    assertEquals((6263012 - 6261812) /
                 (SysInfoWindows.REFRESH_INTERVAL_MS + 1f),
                 tester.getNumVCoresUsed(), 0.0);
  }

  @Test(timeout = 10000)
  public void errorInGetSystemInfo() {
    SysInfoWindowsMock tester = new SysInfoWindowsMock();
    // info str derived from windows shell command is null
    tester.setSysinfoString(null);
    // call a method to refresh values
    tester.getAvailablePhysicalMemorySize();

    // info str derived from windows shell command with no \r\n termination
    tester.setSysinfoString("");
    // call a method to refresh values
    tester.getAvailablePhysicalMemorySize();
  }
  }
}
