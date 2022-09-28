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

package org.apache.hadoop.security;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestHttpCrossOriginFilterInitializer {

  @Parameterized.Parameter(value = 0)
  public String param;
  @Parameterized.Parameter(value = 1)
  public String nestedParam;
  @Parameterized.Parameter(value = 2)
  public String value1;
  @Parameterized.Parameter(value = 3)
  public String value2;

  @Parameterized.Parameters
    public static Collection<Object[]> testData() {
      Object[][] data = new Object[][] { {"rootparam", "nested.param", "rootvalue", "nestedvalue"},
                                         {"random123@", "nested.param.nested.again", "garbage Value", "some 123.value"}
      };

      return Arrays.asList(data);
    }

  // PUTs #49
  @Test
  public void testGetFilterParameters() {

    // Initialize configuration object
    Configuration conf = new Configuration();
    conf.set(HttpCrossOriginFilterInitializer.PREFIX + param,
        value1);
    conf.set(HttpCrossOriginFilterInitializer.PREFIX + nestedParam,
        value2);
    conf.set("outofscopeparam", "outofscopevalue");

    // call function under test
    Map<String, String> filterParameters = HttpCrossOriginFilterInitializer
        .getFilterParameters(conf, HttpCrossOriginFilterInitializer.PREFIX);

    // retrieve values
    String rootvalue = filterParameters.get(param);
    String nestedvalue = filterParameters.get(nestedParam);
    String outofscopeparam = filterParameters.get("outofscopeparam");

    // verify expected values are in place
    Assert.assertEquals("Could not find filter parameter", value1,
        rootvalue); // used parameter directly
    Assert.assertEquals("Could not find filter parameter", value2,
        nestedvalue); // used parameter directly
    Assert.assertNull("Found unexpected value in filter parameters",
        outofscopeparam); // no change in assertion
  }
}
