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

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Simple tests for utility class Lists.
 */
public class TestLists {

  @Test
  public void testAddToEmptyArrayList() {
    List<String> list = Lists.newArrayList();
    list.add("record1");
    Assert.assertEquals(1, list.size());
    Assert.assertEquals("record1", list.get(0));
  }

  @Test
  public void testAddToEmptyLinkedList() {
    List<String> list = Lists.newLinkedList();
    list.add("record1");
    Assert.assertEquals(1, list.size());
    Assert.assertEquals("record1", list.get(0));
  }

  private static Stream<Arguments> valuesSetsForTestVarArgArrayLists() {
      return Stream.of(
          Arguments.of((Object) new String[]{"record1", "record2", "record3"}),
          Arguments.of((Object) new String[]{"record4", "record5", "record6", "record7", "record8"}),
          Arguments.of((Object) new String[]{"R!", "R@", "R#", "R$", "R %"})
      );
  }
   // PUTs #65
  @ParameterizedTest
  @MethodSource("valuesSetsForTestVarArgArrayLists")
  public void testVarArgArrayLists(String[] recordList) {
    List<String> list = Lists.newArrayList(recordList);
    list.add("record4");
    Assert.assertEquals(recordList.length + 1, list.size()); // basic manipulation of parameter
    for (int i = 0;i < recordList.length; i++) {
        Assert.assertEquals(recordList[i], list.get(i)); // basic manipulation of parameter
    }
    Assert.assertEquals("record4", list.get(recordList.length)); // no change in assertion
  }

  private static Stream<Arguments> valuesSetsForTestItrArrayLists() {
      return Stream.of(
          Arguments.of(new HashSet<>(Arrays.asList("record1", "record2", "record3"))),
          Arguments.of(new HashSet<>(Arrays.asList("record1"))),
          Arguments.of(new HashSet<>(Arrays.asList("record1123", "record54", "record13")))
      );
  }
  // PUTs #68
  @ParameterizedTest
  @MethodSource("valuesSetsForTestItrArrayLists")
  public void testItrArrayLists(Set<String> stringHash) {
    Set<String> set = new HashSet<>(stringHash);
    List<String> list = Lists.newArrayList(set);
    list.add("record4");
    Assert.assertEquals(stringHash.size()+1, list.size()); // basic manipulation of parameter
  }

  private static Stream<Arguments> valuesSetsForTestItrLinkedLists() {
      return Stream.of(
          Arguments.of(new HashSet<>(Arrays.asList("record1", "record2", "record3"))),
          Arguments.of(new HashSet<>(Arrays.asList("record1"))),
          Arguments.of(new HashSet<>(Arrays.asList("record1123", "record54", "record13")))
      );
  }
  // PUTs #69
  @ParameterizedTest
  @MethodSource("valuesSetsForTestItrLinkedLists")
  public void testItrLinkedLists(Set<String> stringHash) {
    Set<String> set = new HashSet<>(stringHash);
    List<String> list = Lists.newLinkedList(set);
    list.add("record4");
    Assert.assertEquals(stringHash.size() + 1, list.size()); // basic manipulation of parameter
  }

  private static Stream<Arguments> valuesSetsForTestListsPartition() {
      return Stream.of(
          Arguments.of((Object) new String[]{"a", "b", "c", "d", "e"}, 2), //  5, 2 -> 1
          Arguments.of((Object) new String[]{"a", "b", "c", "d", "e"}, 1), // 5,1 -> 1
          Arguments.of((Object) new String[]{"a", "b", "c", "d", "e"}, 6), // 5, 6 -> 5
          Arguments.of((Object) new String[]{"a", "b", "c", "d", "e"}, 13), // 5, 13 -> 5
          Arguments.of((Object) new String[]{"a", "b", "c"}, 3) // 3, 3 -> 3
      );
  }
   // PUTs #66
  @ParameterizedTest
  @MethodSource("valuesSetsForTestListsPartition")
  public void testListsPartition(String[] stringList, int pageSize) {
    List<String> list = new ArrayList<>();
    for (int i = 0;i < stringList.length; i++) {
        list.add(stringList[i]);
    }
    List<List<String>> res = Lists.
            partition(list, pageSize);
    Assertions.assertThat(res)
            .describedAs("Number of partitions post partition")
            .hasSize((int) Math.ceil(stringList.length / (pageSize * 1.0))); // formula
    Assertions.assertThat(res.get(0))
            .describedAs("Number of elements in first partition")
            .hasSize(Math.min(stringList.length, pageSize)); // formula
    Assertions.assertThat(res.get((int) Math.ceil(stringList.length / (pageSize * 1.0)) - 1))
            .describedAs("Number of elements in last partition")
            .hasSize(stringList.length%pageSize == 0 ? Math.min(stringList.length, pageSize) : stringList.length % pageSize); // formula
  }

  private static Stream<Arguments> valuesSetsForTestArrayListWithSize() {
      return Stream.of(
          Arguments.of((Object) new String[]{"record1", "record2", "record3"}, (Object) new String[]{"record1", "record2", "record3"}),
          Arguments.of((Object) new String[]{"record4", "record5", "record6", "record7", "record8"}, (Object) new String[]{"record1"}),
          Arguments.of((Object) new String[]{"R!", "R@", "R#", "R$", "R %"}, (Object) new String[]{"record2", "record3"})
      );
  }
   // PUTs #67
  @ParameterizedTest
  @MethodSource("valuesSetsForTestArrayListWithSize")
  public void testArrayListWithSize(String[] list1, String[] list2) {
    List<String> list = Lists.newArrayListWithCapacity(list1.length);
    for (int i = 0; i < list1.length; i++) {
        list.add(list1[i]);
    }
    Assert.assertEquals(list1.length, list.size()); // basic manipulation of parameter
    for (int i = 0; i < list1.length; i++) {
        Assert.assertEquals(list1[i], list.get(i)); // basic manipulation of parameter
    }
    list = Lists.newArrayListWithCapacity(list2.length);
    for (int i = 0; i < list2.length; i++) {
            list.add(list2[i]);
    }
    Assert.assertEquals(list2.length, list.size()); // basic manipulation of parameter
    for (int i = 0; i < list2.length; i++) {
        Assert.assertEquals(list2[i], list.get(i)); // basic manipulation of parameter
    }
  }
}
