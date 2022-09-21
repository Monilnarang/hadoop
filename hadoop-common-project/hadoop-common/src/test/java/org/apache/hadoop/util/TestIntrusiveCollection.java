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

/**
 * <pre>
 * Story 1
 * As a software developer,
 *  I want to use the IntrusiveCollection class;
 * So that I can save on memory usage during execution.
 * </pre>
 */
package org.apache.hadoop.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.test.HadoopTestBase;
import org.apache.hadoop.util.IntrusiveCollection.Element;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class TestIntrusiveCollection extends HadoopTestBase {
  static class SimpleElement implements IntrusiveCollection.Element {
    private Map<IntrusiveCollection<? extends Element>, Element>
        prevMap, nextMap;
    private Map<IntrusiveCollection<? extends Element>, Boolean> isMemberMap;

    public SimpleElement() {
      prevMap = new HashMap<>();
      nextMap = new HashMap<>();
      isMemberMap = new HashMap<>();
    }

    @Override
    public void insertInternal(IntrusiveCollection<? extends Element> list,
        Element prev, Element next) {
      isMemberMap.put(list, true);
      prevMap.put(list, prev);
      nextMap.put(list, next);
    }

    @Override
    public void setPrev(IntrusiveCollection<? extends Element> list,
        Element prev) {
      prevMap.put(list, prev);
    }

    @Override
    public void setNext(IntrusiveCollection<? extends Element> list,
        Element next) {
      nextMap.put(list, next);
    }

    @Override
    public void removeInternal(IntrusiveCollection<? extends Element> list) {
      prevMap.remove(list);
      nextMap.remove(list);
      isMemberMap.remove(list);
    }

    @Override
    public Element getPrev(IntrusiveCollection<? extends Element> list) {
      return prevMap.getOrDefault(list, null);
    }

    @Override
    public Element getNext(IntrusiveCollection<? extends Element> list) {
      return nextMap.getOrDefault(list, null);
    }

    @Override
    public boolean isInList(IntrusiveCollection<? extends Element> list) {
      return isMemberMap.getOrDefault(list, false);
    }
  }

  private Object[] valueSetForNumberOfElements() {
    return new Object[] {
                new Object[] {1},
                new Object[] {3},
                new Object[] {0},
                new Object[] {-1},
                new Object[] {-10},
                new Object[] {10},
    };
  }

  /**
   * <pre>
   * Scenario S1.1: Adding and removing elements
   * Given  an IntrusiveCollection has been created
   *  and    the IntrusiveCollection is empty
   * When    I insert elements
   *  and    then remove them
   * Then    The IntrusiveCollection contains the newly added elements
   *  and    doesn't find them once removed
   *  and    the IntrusiveCollection is empty at last
   * </pre>
   */
  @Test
  @Parameters(method = "valueSetForNumberOfElements")
  public void testShouldAddAndRemoveElements(int numberOfElementsToAddAndRemove) {
    Assume.assumeTrue(numberOfElementsToAddAndRemove >= 0);
    IntrusiveCollection<SimpleElement> intrusiveCollection =
      new IntrusiveCollection<>();
    SimpleElement[] elements = new SimpleElement[numberOfElementsToAddAndRemove];
    for (int i = 0; i < numberOfElementsToAddAndRemove; i++) {
        elements[i] = new SimpleElement();
        intrusiveCollection.add(elements[i]);
        assertFalse("Collection should not be empty",
            intrusiveCollection.isEmpty());
    }
    for (int i = 0; i < numberOfElementsToAddAndRemove; i++) {
        assertTrue("Collection should contain added element",
            intrusiveCollection.contains(elements[i])); // some manipulation of parameter
    }
    for (int i = 0; i < numberOfElementsToAddAndRemove; i++) {
        intrusiveCollection.remove(elements[i]);
        assertFalse("Collection should not contain removed element",
            intrusiveCollection.contains(elements[i])); // some manipulation of parameter
    }
    assertTrue("Collection should be empty", intrusiveCollection.isEmpty());
  }


  /**
   * <pre>
   * Scenario S1.2: Removing all elements
   * Given  an IntrusiveCollection has been created
   *  and    the IntrusiveCollection contains multiple elements
   * When    I remove all elements
   * Then    the IntrusiveCollection is empty.
   * </pre>
   */
  @Test
  @Parameters(method = "valueSetForNumberOfElements")
  public void testShouldRemoveAllElements(int numberOfElements) {
    IntrusiveCollection<SimpleElement> intrusiveCollection =
      new IntrusiveCollection<>();
    for (int i = 0; i < numberOfElements; i++) {
        intrusiveCollection.add(new SimpleElement());
    }
    intrusiveCollection.clear();

    assertTrue("Collection should be empty", intrusiveCollection.isEmpty());
  }

  /**
   * <pre>
   * Scenario S1.3: Iterating through elements
   * Given  an IntrusiveCollection has been created
   *  and    the IntrusiveCollection contains multiple elements
   * When    I iterate through the IntrusiveCollection
   * Then    I get each element in the collection, successively.
   * </pre>
   */
  @Test
  @Parameters(method = "valueSetForNumberOfElements")
  public void testIterateShouldReturnAllElements(int numberOfElements) {
    Assume.assumeTrue(numberOfElements >= 0);
    IntrusiveCollection<SimpleElement> intrusiveCollection =
      new IntrusiveCollection<>();
    SimpleElement[] elements = new SimpleElement[numberOfElements];
    for (int i = 0; i < numberOfElements; i++) {
        elements[i] = new SimpleElement();
        intrusiveCollection.add(elements[i]);
    }

    Iterator<SimpleElement> iterator = intrusiveCollection.iterator();

    for (int i = 0; i < numberOfElements; i++) {
        assertEquals((i + 1) + " element returned is incorrect", elements[i], iterator.next()); // some manipulation of parameter
    }
    assertFalse("Iterator should not have next element", iterator.hasNext());
  }
}
