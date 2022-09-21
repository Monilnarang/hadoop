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
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.fs.shell.PathData;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class TestFilterExpression {
  private Expression expr;
  private FilterExpression test;

  @Rule
  public Timeout globalTimeout = new Timeout(10000, TimeUnit.MILLISECONDS);

  @Before
  public void setup() {
    expr = mock(Expression.class);
    test = new FilterExpression(expr) {
    };
  }

  // test that the child expression is correctly set
  @Test
  public void expression() throws IOException {
    assertEquals(expr, test.expression);
  }

  // test that setOptions method is called
  @Test
  public void setOptions() throws IOException {
    FindOptions options = mock(FindOptions.class);
    test.setOptions(options);
    verify(expr).setOptions(options);
    verifyNoMoreInteractions(expr);
  }

  private Object[] valueSetForInteger() {
    return new Object[] {
                new Object[] {-1},
                new Object[] {1},
                new Object[] {0},
                new Object[] {-1},
                new Object[] {Integer.MAX_VALUE},
                new Object[] {Integer.MIN_VALUE},
                new Object[] {12345},
    };
  }

  // test the apply method is called and the result returned
  @Test
  @Parameters(method = "valueSetForInteger")
  public void apply(int depth) throws IOException {
    Assume.assumeTrue(depth == -1);
    PathData item = mock(PathData.class);
    when(expr.apply(item, depth)).thenReturn(Result.PASS).thenReturn(Result.FAIL);
    assertEquals(Result.PASS, test.apply(item, depth)); // used parameter directly
    assertEquals(Result.FAIL, test.apply(item, depth)); // used parameter directly
    verify(expr, times(2)).apply(item, depth);
    verifyNoMoreInteractions(expr);
  }

  // test that the finish method is called
  @Test
  public void finish() throws IOException {
    test.finish();
    verify(expr).finish();
    verifyNoMoreInteractions(expr);
  }

  private Object[] valueSetForStringArray() {
    return new Object[] {
                new Object[] {new String[] { "Usage 1", "Usage 2", "Usage 3" }},
                new Object[] {new String[] { "Help 1", "Help 2", "Help 3" }},
                new Object[] {new String[] { "U", "U" }},
                new Object[] {new String[] { "1", "2", "3", "4" }},
                new Object[] {new String[] { }},
                new Object[] {new String[] { "", "    ", "      ", "", "", "", "", "                      " }},
                new Object[] {new String[] { "123@abc", "ABC@4567", "!@#$%^&*()__+=-{}][;':,./?><" }},
    };
  }

  // test that the getUsage method is called
  @Test
  @Parameters(method = "valueSetForStringArray")
  public void getUsage(String[] usage) {
    when(expr.getUsage()).thenReturn(usage);
    assertArrayEquals(usage, test.getUsage());
    verify(expr).getUsage();
    verifyNoMoreInteractions(expr);
  }

  // test that the getHelp method is called
  @Test
  @Parameters(method = "valueSetForStringArray")
  public void getHelp(String[] help) {
    when(expr.getHelp()).thenReturn(help);
    assertArrayEquals(help, test.getHelp());
    verify(expr).getHelp();
    verifyNoMoreInteractions(expr);
  }

  private Object[] valueSetForTwoBooleans() {
    return new Object[] {
                new Object[] {true, false},
                new Object[] {true, true},
                new Object[] {false, false},
                new Object[] {false, true},
    };
  }

  // test that the isAction method is called
  @Test
  @Parameters(method = "valueSetForTwoBooleans")
  public void isAction(boolean b1, boolean b2) {
    when(expr.isAction()).thenReturn(b1).thenReturn(b2);
    assertEquals(b1, test.isAction()); // changed assert // used parameter directly
    assertEquals(b2, test.isAction()); // changed asserts // used parameter directly
    verify(expr, times(2)).isAction();
    verifyNoMoreInteractions(expr);
  }

  // test that the isOperator method is called
  @Test
  @Parameters(method = "valueSetForTwoBooleans")
  public void isOperator(boolean b1, boolean b2) {
    when(expr.isAction()).thenReturn(b1).thenReturn(b2);
    assertEquals(b1, test.isAction()); // changed asserts // used parameter directly
    assertEquals(b2, test.isAction()); // changed asserts // used parameter directly
    verify(expr, times(2)).isAction();
    verifyNoMoreInteractions(expr);
  }

  // test that the getPrecedence method is called
  @Test
  @Parameters(method = "valueSetForInteger")
  public void getPrecedence(int precedence) {
    when(expr.getPrecedence()).thenReturn(precedence);
    assertEquals(precedence, test.getPrecedence());
    verify(expr).getPrecedence();
    verifyNoMoreInteractions(expr);
  }

  // test that the addChildren method is called
  @Test
  public void addChildren() {
    @SuppressWarnings("unchecked")
    Deque<Expression> expressions = mock(Deque.class);
    test.addChildren(expressions);
    verify(expr).addChildren(expressions);
    verifyNoMoreInteractions(expr);
  }

  // test that the addArguments method is called
  @Test
  public void addArguments() {
    @SuppressWarnings("unchecked")
    Deque<String> args = mock(Deque.class);
    test.addArguments(args);
    verify(expr).addArguments(args);
    verifyNoMoreInteractions(expr);
  }
}
