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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class TestBlockLocation {

  private static final String[] EMPTY_STR_ARRAY = new String[0];
  private static final StorageType[] EMPTY_STORAGE_TYPE_ARRAY =
      StorageType.EMPTY_ARRAY;

  private static void checkBlockLocation(final BlockLocation loc)
      throws Exception {
    checkBlockLocation(loc, 0, 0, false);
  }

  private static void checkBlockLocation(final BlockLocation loc,
      final long offset, final long length, final boolean corrupt)
      throws Exception {
    checkBlockLocation(loc, EMPTY_STR_ARRAY, EMPTY_STR_ARRAY, EMPTY_STR_ARRAY,
        EMPTY_STR_ARRAY, EMPTY_STR_ARRAY, EMPTY_STORAGE_TYPE_ARRAY, offset,
        length, corrupt);
  }

  private static void checkBlockLocation(final BlockLocation loc,
      String[] names, String[] hosts, String[] cachedHosts,
      String[] topologyPaths,
      String[] storageIds, StorageType[] storageTypes,
      final long offset, final long length,
      final boolean corrupt) throws Exception {
    assertNotNull(loc.getHosts());
    assertNotNull(loc.getCachedHosts());
    assertNotNull(loc.getNames());
    assertNotNull(loc.getTopologyPaths());
    assertNotNull(loc.getStorageIds());
    assertNotNull(loc.getStorageTypes());

    assertArrayEquals(hosts, loc.getHosts());
    assertArrayEquals(cachedHosts, loc.getCachedHosts());
    assertArrayEquals(names, loc.getNames());
    assertArrayEquals(topologyPaths, loc.getTopologyPaths());
    assertArrayEquals(storageIds, loc.getStorageIds());
    assertArrayEquals(storageTypes, loc.getStorageTypes());

    assertEquals(offset, loc.getOffset());
    assertEquals(length, loc.getLength());
    assertEquals(corrupt, loc.isCorrupt());
  }

  /**
   * Call all the constructors and verify the delegation is working properly
   */
  @Test(timeout = 5000)
  @Parameters({
  "1, 2",
  "2, 4",
  "8, 4",
  "0,0",
  "-1 , -1",
  "-99999, -9999"
  })
  public void testBlockLocationConstructors(long offset, long length) throws Exception {
    //
    BlockLocation loc;
    loc = new BlockLocation();
    checkBlockLocation(loc);
    loc = new BlockLocation(null, null, offset, length);
    checkBlockLocation(loc, offset, length, false);
    loc = new BlockLocation(null, null, null, offset, length);
    checkBlockLocation(loc, offset, length, false);
    loc = new BlockLocation(null, null, null, offset, length, true);
    checkBlockLocation(loc, offset, length, true);
    loc = new BlockLocation(null, null, null, null, offset, length, true);
    checkBlockLocation(loc, offset, length, true);
    loc = new BlockLocation(null, null, null, null, null, null, offset, length, true);
    checkBlockLocation(loc, offset, length, true);
  }

  private Object[] testParameters() {
    return new Object[] {
        new Object[] {1, 2, new String[] { "name" }, new String[] { "host" }, new String[] { "cachedHost" },
                new String[] { "path" }, new String[] { "storageId" }, new StorageType[] { StorageType.DISK }},
        new Object[] {2, 4, new String[] { "n@3", "na me" }, new String[] { "h11.2.2", "ho st" },
                new String[] { "c@#432", "cach ed" }, new String[] { "p123@", "spaced value" },
                new String[] { "s@567", "val val " }, new StorageType[] { StorageType.RAM_DISK, StorageType.SSD }},
        new Object[] {8, 4, new String[] { " ", "1 2", "!@#$%^&*()" }, new String[] { " ", "1 2", "!@#$%^&*()" },
                new String[] { " ", "1 2", "!@#$%^&*()" }, new String[] { " ", "1 2", "!@#$%^&*()" },
                new String[] { " ", "1 2", "!@#$%^&*()" },
                new StorageType[] { StorageType.ARCHIVE, StorageType.DEFAULT, StorageType.NVDIMM }},
        new Object[] {0, 0, new String[] { "" }, new String[] { "" }, new String[] { "" },
                new String[] { "" }, new String[] { "" }, new StorageType[] { StorageType.PROVIDED }},
        new Object[] {-1, -1, null, null, null, null, null, null},
        new Object[] {-99999, -9999, new String[] { "name" }, new String[] { "h11.2.2", "ho st" },
                new String[] { "!@#$%^&*()" , "    "}, new String[] { "" }, new String[] { "" },
                new StorageType[] { StorageType.PROVIDED, StorageType.NVDIMM, StorageType.ARCHIVE, StorageType.SSD,
                    StorageType.DEFAULT, StorageType.DISK, StorageType.RAM_DISK} },
        new Object[] {-79, 1231, EMPTY_STR_ARRAY, EMPTY_STR_ARRAY, EMPTY_STR_ARRAY, EMPTY_STR_ARRAY, EMPTY_STR_ARRAY,
                EMPTY_STORAGE_TYPE_ARRAY},
    };
  }

  /**
   * Call each of the setters and verify
   */
  @Test(timeout = 5000)
  @Parameters(method = "testParameters")
  public void testBlockLocationSetters(long offset, long length, String[] names, String[] hosts, String[] cachedHosts,
        String[] topologyPaths, String[] storageIds, StorageType[] storageTypes) throws Exception {
    Assume.assumeTrue(names != null && hosts != null && cachedHosts != null && topologyPaths != null &&
        storageIds != null && storageTypes != null);
    BlockLocation loc;
    loc = new BlockLocation();
    // Test that null sets the empty array
    loc.setHosts(null);
    loc.setCachedHosts(null);
    loc.setNames(null);
    loc.setTopologyPaths(null);
    checkBlockLocation(loc);
    // Test that not-null gets set properly
    loc.setNames(names);
    loc.setHosts(hosts);
    loc.setCachedHosts(cachedHosts);
    loc.setTopologyPaths(topologyPaths);
    loc.setStorageIds(storageIds);
    loc.setStorageTypes(storageTypes);
    loc.setOffset(offset);
    loc.setLength(length);
    loc.setCorrupt(true);
    checkBlockLocation(loc, names, hosts, cachedHosts, topologyPaths,
        storageIds, storageTypes, offset, length, true);
  }
}
