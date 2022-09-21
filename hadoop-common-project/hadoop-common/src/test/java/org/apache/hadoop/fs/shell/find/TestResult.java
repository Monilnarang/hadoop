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

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(JUnitParamsRunner.class)
public class TestResult {

  @Rule
  public Timeout globalTimeout = new Timeout(10000, TimeUnit.MILLISECONDS);

  private Object[] valueSetForTestingSingleResult() {
    return new Object[] {
        new Object[] {Result.PASS, true, true}, // test the PASS value
        new Object[] {Result.FAIL, false, true}, // test the FAIL value
        new Object[] {Result.STOP, true, false}, // test the STOP value
        new Object[] {Result.PASS.combine(Result.PASS), true, true}, // test combine method with two PASSes
        new Object[] {Result.PASS.combine(Result.FAIL), false, true}, // test the combine method with a PASS and a FAIL
        new Object[] {Result.FAIL.combine(Result.PASS), false, true}, // test the combine method with a FAIL and a PASS
        new Object[] {Result.FAIL.combine(Result.FAIL), false, true}, // test the combine method with two FAILs
        new Object[] {Result.PASS.combine(Result.STOP), true, false}, // test the combine method with a PASS and STOP
        new Object[] {Result.STOP.combine(Result.FAIL), false, false}, // test the combine method with a STOP and FAIL
        new Object[] {Result.STOP.combine(Result.PASS), true, false}, // test the combine method with a STOP and a PASS
        new Object[] {Result.FAIL.combine(Result.STOP), false, false}, // test the combine method with a FAIL and a STOP
        new Object[] {Result.PASS.negate(), false, true}, // test the negation of PASS
        new Object[] {Result.FAIL.negate(), true, true}, // test the negation of FAIL
        new Object[] {Result.STOP.negate(), false, false}, // test the negation of STOP
        new Object[] {Result.STOP.combine(Result.STOP), true, false}, // test the combine method with a STOP and a STOP
    };
  }

  @Test
  @Parameters(method = "valueSetForTestingSingleResult")
  public void testSingleResult(Result result, boolean isPass, boolean isDescend) {
    assertEquals(isPass, result.isPass()); // changed assert // used parameter directly
    assertEquals(isDescend, result.isDescend()); // changed assert // used parameter directly
  }

  private Object[] valueSetForTestingTwoEqualResults() {
    return new Object[] {
        new Object[] {Result.PASS, Result.PASS.combine(Result.PASS)}, // test equals with two PASSes
        new Object[] {Result.FAIL, Result.FAIL.combine(Result.FAIL)}, // test equals with two FAILs
        new Object[] {Result.STOP, Result.STOP.combine(Result.STOP)}, // test equals with two STOPs
        new Object[] {Result.STOP.combine(Result.STOP), Result.STOP.combine(Result.STOP)},
        new Object[] {Result.PASS.combine(Result.PASS), Result.PASS.combine(Result.PASS)},
        new Object[] {Result.FAIL.combine(Result.FAIL), Result.FAIL.combine(Result.FAIL)},
        new Object[] {Result.STOP.combine(Result.PASS), Result.PASS.combine(Result.STOP)},
        new Object[] {Result.STOP.combine(Result.FAIL), Result.FAIL.combine(Result.STOP)},
        new Object[] {Result.FAIL.combine(Result.PASS), Result.PASS.combine(Result.FAIL)},
        new Object[] {Result.STOP.combine(Result.STOP), Result.PASS.combine(Result.STOP)},
        new Object[] {Result.FAIL, Result.FAIL.combine(Result.FAIL)},
        new Object[] {Result.FAIL, Result.PASS.combine(Result.FAIL)},
        new Object[] {Result.STOP, Result.STOP.combine(Result.PASS)},
        new Object[] {Result.PASS, Result.PASS.combine(Result.PASS)},
        new Object[] {Result.FAIL, Result.FAIL.combine(Result.FAIL)},
        new Object[] {Result.FAIL, Result.PASS.combine(Result.FAIL)},
    };
  }

  @Test
  @Parameters(method = "valueSetForTestingTwoEqualResults")
  public void testTwoEqualsResults(Result one, Result two) {
    assertEquals(one, two);
  }

  private Object[] valueSetForTestingUnequalCombinations() {
    return new Object[] {
        new Object[] {Result.PASS, Result.FAIL},
        new Object[] {Result.PASS, Result.STOP},
        new Object[] {Result.FAIL, Result.PASS},
        new Object[] {Result.FAIL, Result.STOP},
        new Object[] {Result.STOP, Result.PASS},
        new Object[] {Result.STOP, Result.FAIL},
        new Object[] {Result.STOP, Result.PASS.combine(Result.PASS)},
        new Object[] {Result.STOP, Result.PASS.combine(Result.FAIL)},
        new Object[] {Result.STOP, Result.FAIL.combine(Result.FAIL)},
        new Object[] {Result.STOP, Result.STOP.combine(Result.FAIL)},
        new Object[] {Result.PASS, Result.STOP.combine(Result.STOP)},
        new Object[] {Result.PASS, Result.FAIL.combine(Result.FAIL)},
        new Object[] {Result.PASS, Result.PASS.combine(Result.FAIL)},
        new Object[] {Result.PASS, Result.STOP.combine(Result.FAIL)},
        new Object[] {Result.PASS, Result.STOP.combine(Result.PASS)},
        new Object[] {Result.FAIL, Result.PASS.combine(Result.PASS)},
        new Object[] {Result.FAIL, Result.STOP.combine(Result.STOP)},
        new Object[] {Result.FAIL, Result.STOP.combine(Result.FAIL)},
        new Object[] {Result.FAIL, Result.STOP.combine(Result.PASS)},
    };
  }

  // test all combinations of not equals
  @Test
  @Parameters(method = "valueSetForTestingUnequalCombinations")
  public void notEquals(Result one, Result two) {
    assertFalse(one.equals(two));
  } // used parameter directly
}
