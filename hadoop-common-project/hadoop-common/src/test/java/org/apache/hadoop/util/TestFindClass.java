/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.apache.hadoop.util.FindClass;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the find class logic
 */

@RunWith(JUnitParamsRunner.class)
public class TestFindClass extends Assert {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFindClass.class);

  public static final String LOG4J_PROPERTIES = "log4j.properties";

  /**
   * Run the tool runner instance
   * @param expected expected return code
   * @param args a list of arguments
   * @throws Exception on any falure that is not handled earlier
   */
  private void run(int expected, String... args) throws Exception {
    int result = ToolRunner.run(new FindClass(), args);
    assertEquals(expected, result);
  }

  private Object[] valueSetForRun() {
    return new Object[] {
                new Object[] {FindClass.E_USAGE, "org.apache.hadoop.util.TestFindClass"}, // testUsage
                new Object[] {FindClass.SUCCESS,    // testFindsResource
                                      FindClass.A_RESOURCE, "org/apache/hadoop/util/TestFindClass.class"},
                new Object[] {FindClass.E_NOT_FOUND, // testFailsNoSuchResource
                                      FindClass.A_RESOURCE,
                                      "org/apache/hadoop/util/ThereIsNoSuchClass.class"},
                new Object[] {FindClass.SUCCESS, // testLoadFindsSelf
                                      FindClass.A_LOAD, "org.apache.hadoop.util.TestFindClass"},
                new Object[] {FindClass.E_NOT_FOUND, // testLoadFailsNoSuchClass
                                      FindClass.A_LOAD, "org.apache.hadoop.util.ThereIsNoSuchClass"},
                new Object[] {FindClass.E_LOAD_FAILED, // testLoadWithErrorInStaticInit
                                      FindClass.A_LOAD,
                                      "org.apache.hadoop.util.TestFindClass$FailInStaticInit"},
                new Object[] {FindClass.SUCCESS, // testCreateHandlesBadToString
                                      FindClass.A_CREATE,
                                      "org.apache.hadoop.util.TestFindClass$BadToStringClass"},
                new Object[] {FindClass.SUCCESS, // testCreatesClass
                                      FindClass.A_CREATE, "org.apache.hadoop.util.TestFindClass"},
                new Object[] {FindClass.E_LOAD_FAILED, // testCreateFailsInStaticInit
                                      FindClass.A_CREATE,
                                      "org.apache.hadoop.util.TestFindClass$FailInStaticInit"},
                new Object[] {FindClass.E_CREATE_FAILED, // testCreateFailsInConstructor
                                      FindClass.A_CREATE,
                                      "org.apache.hadoop.util.TestFindClass$FailInConstructor"},
                new Object[] {FindClass.E_CREATE_FAILED, // testCreateFailsNoEmptyConstructor
                                      FindClass.A_CREATE,
                                      "org.apache.hadoop.util.TestFindClass$NoEmptyConstructor"},
                new Object[] {FindClass.SUCCESS, // testLoadPrivateClass
                                      FindClass.A_LOAD, "org.apache.hadoop.util.TestFindClass$PrivateClass"},
                new Object[] {FindClass.E_CREATE_FAILED, // testCreateFailsPrivateClass
                                      FindClass.A_CREATE,
                                      "org.apache.hadoop.util.TestFindClass$PrivateClass"},
                new Object[] {FindClass.E_CREATE_FAILED, // testCreateFailsInPrivateConstructor
                                      FindClass.A_CREATE,
                                      "org.apache.hadoop.util.TestFindClass$PrivateConstructor"},
                new Object[] {FindClass.SUCCESS, // testLoadFindsLog4J
                                     FindClass.A_RESOURCE, LOG4J_PROPERTIES},
                new Object[] {FindClass.E_NOT_FOUND, // testPrintFailsNoSuchClass
                                     FindClass.A_PRINTRESOURCE, "org.apache.hadoop.util.ThereIsNoSuchClass"},
                new Object[] {FindClass.SUCCESS, // testPrintClass
                                     FindClass.A_PRINTRESOURCE, LOG4J_PROPERTIES},
    };
  }

  @Test
  @Parameters(method = "valueSetForRun")
  public void testRun(int expected, String... args) throws Throwable {
    run(expected, args);
  }

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  @Test
  public void testPrintLog4J() throws Throwable {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    FindClass.setOutputStreams(out, System.err);
    run(FindClass.SUCCESS, FindClass.A_PRINTRESOURCE, LOG4J_PROPERTIES);
    //here the content should be done
    out.flush();
    String body = baos.toString("UTF8");
    LOG.info(LOG4J_PROPERTIES + " =\n" + body);
    assertTrue(body.contains("Apache"));
  }
  
  
  /**
   * trigger a divide by zero fault in the static init
   */
  public static class FailInStaticInit {
    static {
      int x = 0;
      int y = 1 / x;
    }
  }

  /**
   * trigger a divide by zero fault in the constructor
   */
  public static class FailInConstructor {
    public FailInConstructor() {
      int x = 0;
      int y = 1 / x;
    }
  }

  /**
   * A class with no parameterless constructor -expect creation to fail
   */
  public static class NoEmptyConstructor {
    public NoEmptyConstructor(String text) {
    }
  }

  /**
   * This has triggers an NPE in the toString() method; checks the logging
   * code handles this.
   */
  public static class BadToStringClass {
    public BadToStringClass() {
    }

    @Override
    public String toString() {
      throw new NullPointerException("oops");
    }
  }

  /**
   * This has a private constructor
   * -creating it will trigger an IllegalAccessException
   */
  public static class PrivateClass {
    private PrivateClass() {
    }
  }

  /**
   * This has a private constructor
   * -creating it will trigger an IllegalAccessException
   */
  public static class PrivateConstructor {
    private PrivateConstructor() {
    }
  }
}
