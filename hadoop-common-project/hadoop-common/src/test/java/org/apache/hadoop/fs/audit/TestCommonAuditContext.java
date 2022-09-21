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

package org.apache.hadoop.fs.audit;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.assertj.core.api.AbstractStringAssert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_COMMAND;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_PROCESS;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_THREAD1;
import static org.apache.hadoop.fs.audit.CommonAuditContext.PROCESS_ID;
import static org.apache.hadoop.fs.audit.CommonAuditContext.removeGlobalContextEntry;
import static org.apache.hadoop.fs.audit.CommonAuditContext.currentAuditContext;
import static org.apache.hadoop.fs.audit.CommonAuditContext.getGlobalContextEntry;
import static org.apache.hadoop.fs.audit.CommonAuditContext.getGlobalContextEntries;
import static org.apache.hadoop.fs.audit.CommonAuditContext.noteEntryPoint;
import static org.apache.hadoop.fs.audit.CommonAuditContext.setGlobalContextEntry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of the common audit context.
 */
@RunWith(JUnitParamsRunner.class)
public class TestCommonAuditContext extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestCommonAuditContext.class);

  private final CommonAuditContext context = currentAuditContext();
  /**
   * We can set, get and enumerate global context values.
   */
  @Test
  @Parameters({
    "command",
    "truth 123",
    "space !@#$%^&*() space ",
    "",
    "  " })
  public void testGlobalSetGetEnum(String s) throws Throwable {

    setGlobalContextEntry(PARAM_COMMAND, s);
    assertGlobalEntry(PARAM_COMMAND)
        .isEqualTo(s);
    // and the iterators.
    List<Map.Entry<String, String>> list = StreamSupport
        .stream(getGlobalContextEntries().spliterator(),
            false)
        .filter(e -> e.getKey().equals(PARAM_COMMAND))
        .collect(Collectors.toList());
    assertThat(list)
        .hasSize(1)
        .allMatch(e -> e.getValue().equals(s));
  }

  @Test
  public void testVerifyProcessID() throws Throwable {
    assertThat(
        getGlobalContextEntry(PARAM_PROCESS))
        .describedAs("global context value of %s", PARAM_PROCESS)
        .isEqualTo(PROCESS_ID);
  }


  @Test
  public void testNullValue() throws Throwable {
    assertThat(context.get(PARAM_PROCESS))
        .describedAs("Value of context element %s", PARAM_PROCESS)
        .isNull();
  }

  @Test
  public void testThreadId() throws Throwable {
    String t1 = getContextValue(PARAM_THREAD1);
    Long tid = Long.valueOf(t1);
    assertThat(tid).describedAs("thread ID")
        .isEqualTo(Thread.currentThread().getId());
  }

  /**
   * Verify functions are dynamically evaluated.
   */
  @Test
  @Parameters({
    "key, false, false",
    "Key 123, true, true",
    ", false, true",
    "      , true, false",
    "key !@#$%^&*(), false, true" })
  public void testDynamicEval(String key, Boolean boolToSet, Boolean boolInit) throws Throwable {
    context.reset();
    final AtomicBoolean ab = new AtomicBoolean(boolInit);
    context.put(key, () ->
        Boolean.toString(ab.get()));
    assertContextValue(key) // used parameter directly
        .isEqualTo(boolInit.toString());
    // update the reference and the next get call will
    // pick up the new value.
    ab.set(boolToSet);
    assertContextValue(key) // used parameter directly
        .isEqualTo(boolToSet.toString()); // some manipulation of parameter
  }

  private String getContextValue(final String key) {
    String val = context.get(key);
    assertThat(val).isNotBlank();
    return val;
  }

  /**
   * Start an assertion on a context value.
   * @param key key to look up
   * @return an assert which can be extended call
   */
  private AbstractStringAssert<?> assertContextValue(final String key) {
    String val = context.get(key);
    return assertThat(val)
        .describedAs("Value of context element %s", key)
        .isNotBlank();
  }
  /**
   * Assert a context value is null.
   * @param key key to look up
   */
  private void assertContextValueIsNull(final String key) {
    assertThat(context.get(key))
        .describedAs("Value of context element %s", key)
        .isNull();
  }

  @Test
  public void testNoteEntryPoint() throws Throwable {
    setAndAssertEntryPoint(this).isEqualTo("TestCommonAuditContext");

  }

  @Test
  public void testNoteNullEntryPoint() throws Throwable {
    setAndAssertEntryPoint(null).isNull();
  }

  private AbstractStringAssert<?> setAndAssertEntryPoint(final Object tool) {
    removeGlobalContextEntry(PARAM_COMMAND);
    noteEntryPoint(tool);
    AbstractStringAssert<?> anAssert = assertGlobalEntry(
        PARAM_COMMAND);
    return anAssert;
  }

  private AbstractStringAssert<?> assertGlobalEntry(final String key) {
    AbstractStringAssert<?> anAssert = assertThat(getGlobalContextEntry(key))
        .describedAs("Global context value %s", key);
    return anAssert;
  }

  @Test
  @Parameters({
    "command",
    "truth 123",
    "space !@#$%^&*() space ",
    "",
    "    " })
  public void testAddRemove(String key) throws Throwable {
    Assume.assumeTrue(key.trim().length() > 0);
    assertContextValueIsNull(key);
    context.put(key, key);
    assertContextValue(key).isEqualTo(key);
    context.remove(key);
    assertContextValueIsNull(key);
  }
}
