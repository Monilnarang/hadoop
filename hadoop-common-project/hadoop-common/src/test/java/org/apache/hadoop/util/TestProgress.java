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

import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.stream.Stream;

public class TestProgress {
  private static Stream<Arguments> valuesSetsForTestSetPattern() {
        return Stream.of(
            Arguments.of(Float.NaN),
            Arguments.of(Float.NEGATIVE_INFINITY),
            Arguments.of((float)-1),
            Arguments.of((float)1.1),
            Arguments.of(Float.POSITIVE_INFINITY),
            Arguments.of((float)0),
            Arguments.of((float)0.000009),
            Arguments.of((float)0.1),
            Arguments.of((float)0.2),
            Arguments.of((float)0.3),
            Arguments.of((float)0.4),
            Arguments.of((float)0.5),
            Arguments.of((float)0.6),
            Arguments.of(0.7f),
            Arguments.of((float)0.8),
            Arguments.of((float)0.9),
            Arguments.of((float)1.00001),
            Arguments.of((float)-0.000009)
        );
    }
     // PUTs #70
    @ParameterizedTest
    @MethodSource("valuesSetsForTestSetPattern")
  public void testSet(Float value){
    Progress progress = new Progress();
    progress.set(value);
    Assert.assertEquals(value >= 1 ? 1 : (value > 0 ? value : 0), progress.getProgress(), 0.001); // formula
  }
}
