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

package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.fs.shell.PathData;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class TestAnd {

  @Rule
  public Timeout globalTimeout = new Timeout(10000, TimeUnit.MILLISECONDS);

  private Object[] valueSetsForDifferentCases() {
    return new Object[] {
                // Test 1 testPass ->  test all expressions passing
                new Object[] {Result.PASS, Result.PASS, Result.PASS},
                // Test 2 testFailFirst -> test the first expression failing
                new Object[] {Result.FAIL, Result.PASS, Result.FAIL},
                // Test 3 testFailSecond -> test the second expression failing
                new Object[] {Result.PASS, Result.FAIL, Result.FAIL},
                // Test 4 testFailBoth -> test both expressions failing
                new Object[] {Result.FAIL, Result.FAIL, Result.FAIL},
                // Test 5 testStopFirst -> test the first expression stopping
                new Object[] {Result.STOP, Result.PASS, Result.STOP},
                // Test 6 testStopSecond -> test the second expression stopping
                new Object[] {Result.PASS, Result.STOP, Result.STOP},
                // Test 7 testStopFail -> test first expression stopping and second failing
                new Object[] {Result.STOP, Result.FAIL, Result.STOP.combine(Result.FAIL)},
    };
  }


  @Test
  @Parameters(method = "valueSetsForDifferentCases")
  public void testPass(Result firstResult, Result secondResult, Result expectedResult) throws IOException {
    And and = new And();

    PathData pathData = mock(PathData.class);

    Expression first = mock(Expression.class);
    when(first.apply(pathData, -1)).thenReturn(firstResult);

    Expression second = mock(Expression.class);
    when(second.apply(pathData, -1)).thenReturn(secondResult);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    assertEquals(expectedResult, and.apply(pathData, -1)); // used parameter directly
    verify(first).apply(pathData, -1);
    if (firstResult == Result.PASS || firstResult == Result.STOP) {
        verify(second).apply(pathData, -1);
    }
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  @Test
  @Parameters({
  "setOptions", // Test 8: testSetOptions :- test setOptions is called on child
  "prepare", // Test 9: testPrepare :- test prepare is called on child
  "finish", // Test 10: testFinish :- test finish is called on child
  "randomTestName", // check ignore on random function name
  "", // check ignore on random function name
  })
  public void testCallFunctionOnChild(String function) throws IOException {
    And and = new And();
    Expression first = mock(Expression.class);
    Expression second = mock(Expression.class);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    switch (function) {
        case "setOptions": {
            FindOptions options = mock(FindOptions.class);
            and.setOptions(options);
            verify(first).setOptions(options);
            verify(second).setOptions(options);
            break;
        }
        case "prepare": {
            and.prepare();
            verify(first).prepare();
            verify(second).prepare();
            break;
        }
        case "finish": {
            and.finish();
            verify(first).finish();
            verify(second).finish();
            break;
        }
        default: {
            Assume.assumeTrue(false);
        }
    }
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }
}
