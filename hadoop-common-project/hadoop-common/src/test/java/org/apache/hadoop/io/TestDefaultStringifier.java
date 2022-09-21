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

package org.apache.hadoop.io;

import java.io.IOException;
import java.util.Random;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
@RunWith(JUnitParamsRunner.class)
public class TestDefaultStringifier {

  private static Configuration conf = new Configuration();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDefaultStringifier.class);
  private Object[] valueSetForTestWithWritable() {
    return new Object[] {
                new Object[] {10, 40, "abcdefghijklmnopqrstuvwxyz".toCharArray()},
                new Object[] {100, 100, "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray()},
                new Object[] {1, 1000, "1234567890".toCharArray()},
                new Object[] {50, 400, "!@#$%^&*()_[]{}-=_+|;':,./<>?`~".toCharArray()},
                new Object[] {1000, 10000, "abcdefghijklmnopqrstuvwxyz1234567890!@#$%^&*()".toCharArray()},
                new Object[] {0, 0, "abcdefghijklmnopqrstuvwxyz1234567890!@#$%^&*()".toCharArray()},
                new Object[] {-1, 5, "abcdefghijklmnopqrstuvwxyz1234567890!@#$%^&*()".toCharArray()},
                new Object[] {5, -5, "abcdefghijklmnopqrstuvwxyz1234567890!@#$%^&*()".toCharArray()},
                new Object[] {5, 0, "".toCharArray()},
                new Object[] {5, 10, "".toCharArray()},
    };
  }

  @Test
  @Parameters(method = "valueSetForTestWithWritable")
  public void testWithWritable(int iterations, int bound, char[] characters) throws Exception {
    Assume.assumeTrue(bound > 0 && characters.length > 0);
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization");

    LOG.info("Testing DefaultStringifier with Text");

    Random random = new Random();

    //test with a Text
    for(int i=0;i<iterations;i++) {
      //generate a random string
      StringBuilder builder = new StringBuilder();
      int strLen = random.nextInt(bound);
      for(int j=0; j< strLen; j++) {
        builder.append(characters[random.nextInt(characters.length)]);
      }
      Text text = new Text(builder.toString());
      DefaultStringifier<Text> stringifier = new DefaultStringifier<Text>(conf, Text.class);

      String str = stringifier.toString(text);
      Text claimedText = stringifier.fromString(str);
      LOG.info("Object: " + text);
      LOG.info("String representation of the object: " + str);
      assertEquals(text, claimedText);
    }
  }

  private Object[] valueSetForIntegerValue() {
    return new Object[] {
                new Object[] {48},
                new Object[] {0},
                new Object[] {-1},
                new Object[] {Integer.MAX_VALUE},
                new Object[] {Integer.MIN_VALUE},

    };
  }

  @Test
  @Parameters(method = "valueSetForIntegerValue")
  public void testWithJavaSerialization(int value) throws Exception {
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization");

    LOG.info("Testing DefaultStringifier with Serializable Integer");

    //Integer implements Serializable
    Integer testInt = Integer.valueOf(value);
    DefaultStringifier<Integer> stringifier = new DefaultStringifier<Integer>(conf, Integer.class);

    String str = stringifier.toString(testInt);
    Integer claimedInt = stringifier.fromString(str);
    LOG.info("String representation of the object: " + str);

    assertEquals(testInt, claimedInt);
  }

  @Test
  @Parameters({
  "uninteresting test string,test.defaultstringifier.key1",
  "uninterestingteststring,someKEY",
  ",",
  "   ,     ",
  "!@#$%^&*()_1234567890,1234567890!@#$%^&*()",
  ".,."
  })
  public void testStoreLoad(String testString, String keyName) throws IOException {

    LOG.info("Testing DefaultStringifier#store() and #load()");
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization");
    Text text = new Text(testString);

    DefaultStringifier.store(conf,text, keyName);

    Text claimedText = DefaultStringifier.load(conf, keyName, Text.class);
    assertEquals("DefaultStringifier#load() or #store() might be flawed"
        , text, claimedText);

  }

  private Object[] valueSetForTestStoreLoadArray() {
    return new Object[] {
                new Object[] { "test.defaultstringifier.key2", new Integer[] {1,2,3,4,5} },
                new Object[] { "   ", new Integer[] {} },
                new Object[] { "", new Integer[] {Integer.MIN_VALUE, Integer.MAX_VALUE, 0} },
                new Object[] { "!@#$%^&*()_+-={}][|;':,.<>?", new Integer[] {Integer.MIN_VALUE} },
    };
  }

  @Test
  @Parameters(method = "valueSetForTestStoreLoadArray")
  public void testStoreLoadArray(String keyName, Integer[] array) throws IOException {
    LOG.info("Testing DefaultStringifier#storeArray() and #loadArray()");
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization");

    DefaultStringifier.storeArray(conf, array, keyName);

    Integer[] claimedArray = DefaultStringifier.<Integer>loadArray(conf, keyName, Integer.class);
    for (int i = 0; i < array.length; i++) {
      assertEquals("two arrays are not equal", array[i], claimedArray[i]);
    }

  }

}
