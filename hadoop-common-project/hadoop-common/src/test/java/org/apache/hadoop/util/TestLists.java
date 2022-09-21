/*
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
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Simple tests for utility class Lists.
 */
@RunWith(JUnitParamsRunner.class)
public class TestLists {

  private Object[] valueSetToProvideStringArray() {
    return new Object[] {
                new Object[] {new String[]{"record1"}},
                new Object[] {new String[]{"record1", "record2", "record3"}},
                new Object[] {new String[]{"r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r",
                    "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r"}},
                new Object[] {new String[]{"", "", "", ""}},
                new Object[] {new String[]{"    ", "   ", "          ", " "}},
                new Object[] {new String[]{"123", "345", "!@#$%", ")(*&", null}},
                new Object[] {new String[]{"!@#$%^&*()_+-={}][|:';,./?><qwertyuioplkjhgfdsazxcvbnm1234567890"}},
                new Object[] {new String[]{}},
                new Object[] {new String[]{null, null, null, null, null}},
    };
  }

  @Test
  @Parameters(method = "valueSetToProvideStringArray")
  public void testAddToEmptyArrayList(String [] stringArr) {
    List<String> list = Lists.newArrayList();
    for (int i = 0; i < stringArr.length; i++) {
        list.add(stringArr[i]);
    }
    Assert.assertEquals(stringArr.length, list.size()); // some manipulation of parameter
    for (int i = 0; i < stringArr.length; i++) {
        Assert.assertEquals(stringArr[i], list.get(i)); // some manipulation of parameter
    }
  }

  @Test
  @Parameters(method = "valueSetToProvideStringArray")
  public void testAddToEmptyLinkedList(String [] stringArr) {
    List<String> list = Lists.newLinkedList();
    for (int i = 0; i < stringArr.length; i++) {
        list.add(stringArr[i]);
    }
    Assert.assertEquals(stringArr.length, list.size()); // some manipulation of parameter
    for (int i = 0; i < stringArr.length; i++) {
        Assert.assertEquals(stringArr[i], list.get(i)); // some manipulation of parameter
    }
  }

  private Object[] valueSetToProvideStringElementsAndCountToAddMore() {
    return new Object[] {
                new Object[] {1, "record1", "record2", "record3"},
                new Object[] {0, "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r",
                    "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r", "r"},
                new Object[] {100, "", "", "", ""},
                new Object[] {200, "    ", "   ", "          ", " "},
                new Object[] {7, "123", "345", "!@#$%", ")(*&", null},
                new Object[] {-4, "!@#$%^&*()_+-={}][|:';,./?><qwertyuioplkjhgfdsazxcvbnm1234567890"},
                new Object[] {-100, "", "       ", null, null},
    };
  }

  @Test
  @Parameters(method = "valueSetToProvideStringElementsAndCountToAddMore")
  public void testVarArgArrayLists(int nMore, String... elements) {
    List<String> list = Lists.newArrayList(elements);
    for (int i = 0; i < nMore; i++) {
        list.add("record" + (i + elements.length + 1));
    }
    Assert.assertEquals(elements.length + Math.max(nMore, 0), list.size()); // formula // some manipulation of parameter
    int i = 0;
    for (String s : elements) {
        Assert.assertEquals(s, list.get(i++)); // some manipulation of parameter
    }
    int j = 0;
    while (i < nMore) {
        Assert.assertEquals("record" + (j + elements.length + 1), list.get(i)); // formula // some manipulation of parameter
        j++;
        i++;
    }
  }

  @Test
  @Parameters(method = "valueSetToProvideStringElementsAndCountToAddMore")
  public void testItrArrayLists(int nMore, String... elements) {
    Set<String> set = new HashSet<>();
    for (String s : elements) {
        set.add(s);
    }
    List<String> list = Lists.newArrayList(set);
    for (int i = 0; i < nMore; i++) {
        list.add("record" + (i + elements.length + 1));
    }
    Assert.assertEquals(set.size() + Math.max(nMore, 0), list.size()); // formula // some manipulation of parameter
  }

  @Test
  @Parameters(method = "valueSetToProvideStringElementsAndCountToAddMore")
  public void testItrLinkedLists(int nMore, String... elements) {
    Set<String> set = new HashSet<>();
    for (String s : elements) {
        set.add(s);
    }
    List<String> list = Lists.newLinkedList(set);
    for (int i = 0; i < nMore; i++) {
        list.add("record" + (i + elements.length + 1));
    }
    Assert.assertEquals(set.size() + Math.max(nMore, 0), list.size()); // formula // some manipulation of parameter
  }

  private Object[] valueSetToProvideStringArrayAndPageSize() {
    return new Object[] {
                new Object[] {new String[]{"a", "b", "c", "d", "e"}, 2},
                new Object[] {new String[]{"a", "b", "c", "d", "e"}, 1},
                new Object[] {new String[]{"a", "b", "c", "d", "e"}, 6},
                new Object[] {new String[]{"a", "b", "c", "d", "e"}, 13},
                new Object[] {new String[]{"a", "b", "c"}, 3},
    };
  }

  @Test
  @Parameters(method = "valueSetToProvideStringArrayAndPageSize")
  public void testListsPartition(String[] stringList, int pageSize) {
    List<String> list = new ArrayList<>();
    for(int i=0;i<stringList.length;i++) {
        list.add(stringList[i]);
    }
    List<List<String>> res = Lists.
            partition(list, pageSize);
    Assertions.assertThat(res)
            .describedAs("Number of partitions post partition")
            .hasSize((int) Math.ceil(stringList.length/(pageSize*1.0))); // formula
    Assertions.assertThat(res.get(0))
            .describedAs("Number of elements in first partition")
            .hasSize(Math.min(stringList.length, pageSize)); // formula
    Assertions.assertThat(res.get((int) Math.ceil(stringList.length/(pageSize*1.0))-1))
            .describedAs("Number of elements in last partition")
            .hasSize(stringList.length%pageSize==0 ? Math.min(stringList.length, pageSize): stringList.length%pageSize); // formula
  }

  private Object[] valueSetToProvideTwoArrays() {
    return new Object[] {
                new Object[] {new String[]{"record1", "record2", "record3"},
                    (Object) new String[]{"record1", "record2", "record3"}},
                new Object[] {new String[]{"record4", "record5", "record6", "record7", "record8"},
                    (Object) new String[]{"record1"}},
                new Object[] {new String[]{"R!", "R@", "R#", "R$", "R %"},
                    (Object) new String[]{"record2", "record3"}},
                new Object[] {new String[]{"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
                    (Object) new String[]{"     ", "    ", "\"\", \"\", \"\" "}},
                new Object[] {new String[]{null, null, null, null, null, null, null, null, null, null, null, null },
                    (Object) new String[]{}},

    };
  }

  @Test
  @Parameters(method = "valueSetToProvideTwoArrays")
  public void testArrayListWithSize(String[] list1, String[] list2) {
    List<String> list = Lists.newArrayListWithCapacity(list1.length);
    for(int i=0;i<list1.length;i++) {
        list.add(list1[i]);
    }
    Assert.assertEquals(list1.length, list.size()); // some manipulation of parameter
    for(int i=0;i<list1.length;i++) {
        Assert.assertEquals(list1[i], list.get(i)); // some manipulation of parameter
    }
    list = Lists.newArrayListWithCapacity(list2.length);
    for(int i=0;i<list2.length;i++) {
            list.add(list2[i]);
    }
    Assert.assertEquals(list2.length, list.size()); // some manipulation of parameter
    for(int i=0;i<list2.length;i++) {
        Assert.assertEquals(list2[i], list.get(i)); // some manipulation of parameter
    }
  }

}
