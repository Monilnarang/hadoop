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

package org.apache.hadoop.fs.statistics;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;

import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.junit.runner.RunWith;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticCounterIsTracked;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticCounterIsUntracked;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.emptyStatistics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test handling of the empty IO statistics class.
 */
@RunWith(JUnitParamsRunner.class)
public class TestEmptyIOStatistics extends AbstractHadoopTestBase {

  private final IOStatistics empty = emptyStatistics();

  private Object[] testParametersForKey() {
    return new Object[] {
        new Object[] {"anything"},
        new Object[] {"12345sas@##%^"},
        new Object[] {"1234"},
        new Object[] {"      "},
        new Object[] {""},
        new Object[] {" .?// | \\  "}
        };
  }

  @Test
  @Parameters(method = "testParametersForKey")
  public void testUnknownStatistic(String key) throws Throwable {
    assertStatisticCounterIsUntracked(empty, key); // used parameter directly
  }

  @Test
  @Parameters(method = "testParametersForKey")
  public void testStatisticsTrackedAssertion(String key) throws Throwable {
    // expect an exception to be raised when an assertion
    // is made that an unknown statistic is tracked,.
    assertThatThrownBy(() ->
        assertStatisticCounterIsTracked(empty, key)) // used parameter directly
        .isInstanceOf(AssertionError.class);
  }

  private Object[] testParametersForKeyAndValue() {
    return new Object[] {
        new Object[] {"anything", 0},
        new Object[] {"12345sas@##%^", -1},
        new Object[] {"1234", 100},
        new Object[] {"      ", Long.MIN_VALUE},
        new Object[] {"", Long.MAX_VALUE},
        new Object[] {" .?// | \\  ", 999999999}
    };
  }

  @Test
  @Parameters(method = "testParametersForKeyAndValue")
  public void testStatisticsValueAssertion(String key, long value) throws Throwable {
    // expect an exception to be raised when
    // an assertion is made about the value of an unknown statistics
    assertThatThrownBy(() ->
        verifyStatisticCounterValue(empty, key, value)) // used parameter directly
        .isInstanceOf(AssertionError.class);
  }

  @Test
  public void testEmptySnapshot() throws Throwable {
    final IOStatistics stat = IOStatisticsSupport.snapshotIOStatistics(empty);
    assertThat(stat.counters().keySet())
        .describedAs("keys of snapshot")
        .isEmpty();
    IOStatistics deser = IOStatisticAssertions.statisticsJavaRoundTrip(stat);
    assertThat(deser.counters().keySet())
        .describedAs("keys of deserialized snapshot")
        .isEmpty();
  }

  @Test
  public void testStringification() throws Throwable {
    assertThat(ioStatisticsToString(empty))
        .isNotBlank();
  }

  @Test
  public void testWrap() throws Throwable {
    IOStatisticsSource statisticsSource = IOStatisticsBinding.wrap(empty);
    assertThat(statisticsSource.getIOStatistics())
        .isSameAs(empty);
  }

  private Object[] testParametersForStringifyNulls() {
    return new Object[] {
        new Object[] {IOStatisticsLogging.ioStatisticsSourceToString(null)},
        new Object[] {IOStatisticsLogging.ioStatisticsSourceToString(IOStatisticsBinding.wrap(null))},
        new Object[] {ioStatisticsToString(null)},
    };
  }

  @Test
  @Parameters(method = "testParametersForStringifyNulls")
  public void testStringifyNulls(String str) throws Throwable {
    assertThat(str) // used parameter directly
        .describedAs("Null statistics should stringify to \"\"")
        .isEmpty();
  }

}
