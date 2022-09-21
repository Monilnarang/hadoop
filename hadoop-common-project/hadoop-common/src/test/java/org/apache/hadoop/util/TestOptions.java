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

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class TestOptions {

  private Object[] valueSetForTestAppend() {
    return new Object[] {
                new Object[] {new String[]{"hi", "there"}, new String[]{"Dr.", "Who"}},
                new Object[] {new String[]{"dd", "ee", "ff"}, new String[]{"aa", "bb", "cc"}},
                new Object[] {new String[]{"7", "8", "9"}, new String[]{"1", "2", "3", "4", "5", "6"}},
                new Object[] {new String[]{"      ", "       ", "          "}, new String[]{" ", "    ", "    "}},
                new Object[] {new String[]{""}, new String[]{""}},
                new Object[] {new String[]{}, new String[]{"aa", "bb", "cc"}},
                new Object[] {new String[]{}, new String[]{"Dr.", "Who"}},
                new Object[] {new String[]{}, new String[]{}},
                new Object[] {new String[]{null, null, null, null}, new String[]{null, null}},
                new Object[] {new String[]{}, new String[]{null}},
                new Object[] {new String[]{null}, new String[]{}},
                new Object[] {new String[]{"!@#$%^&*()_+=-{}][|;',./?><"}, new String[]{"!@#$%^&*()_+=-{}][|;',./?><"}},
    };
  }

  @Test
  @Parameters(method = "valueSetForTestAppend")
  public void testAppend(String[] oldOpts, String[] newOpts) throws Exception {
    assertArrayEquals("append failure",
                      ArrayUtils.addAll(newOpts, oldOpts),
                      Options.prependOptions(oldOpts,
                                             newOpts)); // formula // some manipulation of parameter
  }

  private Object[] valueSetForTestFind() {
    return new Object[] {
            new Object[] {new Object[]{1, "hi", true, "bye", 'x'}, 0, 1, 2},
            new Object[] {new Object[]{"hi", 12345, 123, false}, 1, 0, 3},
            new Object[] {new Object[]{"hi", "", "123", "false"}, -1, 0, -1},
            new Object[] {new Object[]{1}, 0, -1, -1},
            new Object[] {new Object[]{true, false}, -1, -1, 0},
            new Object[] {new Object[]{}, -1, -1, -1},
            new Object[] {new Object[]{1, "hi", true, "bye", 'x'}, 0, 1, 7},
    };
  }
  /*
  * intInd -> index of first occurrence of integer in array
  * stringInd -> index of first occurrence of string in array
  * boolInd -> index of first occurrence of bool in array
  * if any of the above is absent from the array, any -ve value could be provided.
  * */

  @Test
  @Parameters(method = "valueSetForTestFind")
  public void testFind(Object[] objects, int intInd, int stringInd, int boolInd) throws Exception {
     Assume.assumeTrue(Math.max(Math.max(intInd, stringInd), boolInd) < objects.length);
     if(intInd >= 0) {
        assertEquals(objects[intInd], Options.getOption(Integer.class, objects).intValue()); // some manipulation of parameter
     }
     if(stringInd >= 0) {
        assertEquals(objects[stringInd], Options.getOption(String.class, objects)); // some manipulation of parameter
     }
     if(boolInd >= 0) {
        assertEquals(objects[boolInd], Options.getOption(Boolean.class, objects).booleanValue()); // some manipulation of parameter
     }
  }
}
