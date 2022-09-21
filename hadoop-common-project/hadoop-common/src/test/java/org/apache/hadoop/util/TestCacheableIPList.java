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

import java.io.IOException;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class TestCacheableIPList {

  private String[] getIp1Array() {
    String[] ips1 = {"10.119.103.112", "10.221.102.0/23", "10.113.221.221"};
    return ips1;
  }

  private String[] getIp2Array() {
    String[]ips2 = {"10.119.103.112", "10.221.102.0/23",
                        "10.222.0.0/16", "10.113.221.221", "10.113.221.222"};
    return ips2;
  }

  private Object[] testParameters() {
    return new Object[] {
        new Object[] { getIp1Array(), getIp2Array(), "10.113.221.222", false, true, true, 100 }, // default test 1
        new Object[] { getIp1Array(), getIp2Array(), "10.222.103.121", false, true, true ,100 }, // default test 1
        new Object[] { getIp2Array(), getIp1Array(), "10.113.221.222", true, true, false, 100 }, // default test 2
        new Object[] { getIp2Array(), getIp1Array(), "10.222.103.121", true, true, false ,100 }, // default test 2
        new Object[] { getIp1Array(), getIp2Array(), "10.113.221.222", false, false, true, 100 }, // default test 3
        new Object[] { getIp1Array(), getIp2Array(), "10.222.103.121", false, false, true ,100 }, // default test 3
        new Object[] { getIp2Array(), getIp1Array(), "10.113.221.222", true, false, false, 100 }, // default test 4
        new Object[] { getIp2Array(), getIp1Array(), "10.222.103.121", true, false, false ,100 }, // default test 4

    };
  }

  /**
   * Add a bunch of subnets and IPSs to the file
   * setup a low cache refresh
   * test for inclusion
   * Check for exclusion
   * Add a bunch of subnets and Ips
   * wait for cache timeout.
   * test for inclusion
   * Check for exclusion
   */
  @Test
  @Parameters(method = "testParameters")
  // ips1[], ips2[], ipToCheck, existBool, ipToCheckAfterSleep ???, existAfterSleepBool, chooseMethod, sleep time
  public void testAddWithSleepForCacheTimeout(String[] ips, String[] ips2, String ipToCHeck, boolean expectBool,
        boolean useSleepToRefresh, boolean expectBoolAfter, int timeout) throws IOException, InterruptedException {
        // decide (how?) proper indent of line break
    TestFileBasedIPList.createFileWithEntries ("ips.txt", ips);

    CacheableIPList cipl = new CacheableIPList(
        new FileBasedIPList("ips.txt"),timeout);

    assertEquals(ipToCHeck + " is" + (expectBool ? " not " : " " ) + "in the list",
        expectBool, cipl.isIn(ipToCHeck)); // used parameter directly

    TestFileBasedIPList.removeFile("ips.txt");

    TestFileBasedIPList.createFileWithEntries ("ips.txt", ips2);

    if (useSleepToRefresh) {
        Thread.sleep(timeout+1);
    } else {
        cipl.refresh();
    }

    assertEquals(ipToCHeck + " is" + (expectBoolAfter ? " not " : " " ) + "in the list",
        expectBoolAfter, cipl.isIn(ipToCHeck)); // used parameter directly

    TestFileBasedIPList.removeFile("ips.txt");
  }

}
