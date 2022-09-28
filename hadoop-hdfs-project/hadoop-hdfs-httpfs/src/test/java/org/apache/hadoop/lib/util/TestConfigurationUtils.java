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

package org.apache.hadoop.lib.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestConfigurationUtils {

   @Parameterized.Parameter(value = 0)
   public String key1;
   @Parameterized.Parameter(value = 1)
   public String value1;
   @Parameterized.Parameter(value = 2)
   public String key2;
   @Parameterized.Parameter(value = 3)
   public String value2;
   @Parameterized.Parameter(value = 4)
   public String key3;
   @Parameterized.Parameter(value = 5)
   public String value3;

   @Parameterized.Parameters
     public static Collection<Object> testData() {
       Object[][] data = new Object[][] { {"testParameter1", "valueFromSource", "testParameter2", "valueFromTarget",
                    "testParameter3", "originalValueFromTarget"},
                                          {"test.31@", "1111", "test# 32*34 ", ")(*^%$#", " t e s t K e y",
                    "test @ value * #"},
                                          {"testParameter1", " ", "testParameter2", " ", "testParameter3", "dcx"},
       };
       return Arrays.asList(data);
     }

  // PUTs #36
  @Test
  public void constructors() throws Exception {
    Configuration conf = new Configuration(false);
    assertEquals(conf.size(), 0); // no change in assertion

    byte[] bytes = ("<configuration><property><name>" + key1 + "</name><value>" + value1 +
                    "</value></property></configuration>").getBytes();
    InputStream is = new ByteArrayInputStream(bytes);
    conf = new Configuration(false);
    ConfigurationUtils.load(conf, is);
    assertEquals(conf.size(), 1); // no change in assertion
    assertEquals(conf.get(key1), value1); // used parameter directly
  }


  @Test
  public void constructors3() throws Exception {
    InputStream is = new ByteArrayInputStream(
        "<xxx><property name=\"key1\" value=\"val1\"/></xxx>".getBytes());
    Configuration conf = new Configuration(false);
    ConfigurationUtils.load(conf, is);
    assertEquals("val1", conf.get("key1"));
  }

  // PUTs #37
  @Test
  public void copy() throws Exception {
    Configuration srcConf = new Configuration(false);
    Configuration targetConf = new Configuration(false);

    srcConf.set(key1, value1);
    srcConf.set(key2, value1);

    targetConf.set(key2, value2);
    targetConf.set(key3, value2);

    ConfigurationUtils.copy(srcConf, targetConf);

    assertEquals(value1, targetConf.get(key1)); // used parameter directly
    assertEquals(value1, targetConf.get(key2)); // used parameter directly
    assertEquals(value2, targetConf.get(key3)); // used parameter directly
  }

  // PUTs #38
  @Test
  public void injectDefaults() throws Exception {
    Configuration srcConf = new Configuration(false);
    Configuration targetConf = new Configuration(false);

    srcConf.set(key1, value1);
    srcConf.set(key2, value1);

    targetConf.set(key2, value3);
    targetConf.set(key3, value3);

    ConfigurationUtils.injectDefaults(srcConf, targetConf);

    assertEquals(value1, targetConf.get(key1)); // used parameter directly
    assertEquals(value3, targetConf.get(key2)); // used parameter directly
    assertEquals(value3, targetConf.get(key3)); // used parameter directly

    assertEquals(value1, srcConf.get(key1)); // used parameter directly
    assertEquals(value1, srcConf.get(key2)); // used parameter directly
    assertNull(srcConf.get(key3)); // used parameter directly
  }


  // PUTs #39
  @Test
  public void resolve() {
    Configuration conf = new Configuration(false);
    conf.set(key1, value1);
    conf.set(key2, "${" + key1 + "}");
    assertEquals(conf.getRaw(key1), value1); // used parameter directly
    assertEquals(conf.getRaw(key2), "${" + key1 + "}"); // basic manipulation of parameter
    conf = ConfigurationUtils.resolve(conf);
    assertEquals(conf.getRaw(key1), value1); // used parameter directly
    assertEquals(conf.getRaw(key2), value1); // used parameter directly
  }

  // PUTs #40
  @Test
  public void testVarResolutionAndSysProps() {
    String userName = System.getProperty("user.name");
    Configuration conf = new Configuration(false);
    conf.set(key1, value1);
    conf.set(key2, "${"+key1+"}");
    conf.set(key3, "${user.name}");
    conf.set("d", "${aaa}");
    assertEquals(conf.getRaw(key1), value1); // used parameter directly
    assertEquals(conf.getRaw(key2), "${"+key1+"}"); // basic manipulation of parameter
    assertEquals(conf.getRaw(key3), "${user.name}"); // used parameter directly
    assertEquals(conf.get(key1), value1); // used parameter directly
    assertEquals(conf.get(key2), value1); // used parameter directly
    assertEquals(conf.get(key3), userName); // used parameter directly
    assertEquals(conf.get("d"), "${aaa}"); // no change in assertion

    conf.set("user.name", value3);
    assertEquals(conf.get("user.name"), value3); // used parameter directly
  }

  @Test
  public void testCompactFormatProperty() throws IOException {
    final String testfile = "test-compact-format-property.xml";
    Configuration conf = new Configuration(false);
    assertEquals(0, conf.size());
    ConfigurationUtils.load(conf,
        Thread.currentThread()
            .getContextClassLoader().getResource(testfile).openStream());
    assertEquals(2, conf.size());
    assertEquals("val1", conf.get("key.1"));
    assertEquals("val2", conf.get("key.2"));
  }
}
