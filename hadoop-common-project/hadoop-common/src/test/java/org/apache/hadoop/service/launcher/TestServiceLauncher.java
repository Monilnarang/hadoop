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

package org.apache.hadoop.service.launcher;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.BreakableService;
import org.apache.hadoop.service.launcher.testservices.FailingStopInStartService;
import org.apache.hadoop.service.launcher.testservices.InitInConstructorLaunchableService;
import org.apache.hadoop.service.launcher.testservices.LaunchableRunningService;
import org.apache.hadoop.service.launcher.testservices.NoArgsAllowedService;
import org.apache.hadoop.service.launcher.testservices.NullBindLaunchableService;
import org.apache.hadoop.service.launcher.testservices.RunningService;
import org.apache.hadoop.service.launcher.testservices.StoppingInStartLaunchableService;
import org.apache.hadoop.service.launcher.testservices.StringConstructorOnlyService;

import static org.apache.hadoop.test.GenericTestUtils.*;
import static org.apache.hadoop.service.launcher.testservices.ExceptionInExecuteLaunchableService.*;

import org.junit.Assume;import org.junit.Test;
import org.junit.runner.RunWith;
@RunWith(JUnitParamsRunner.class)
public class TestServiceLauncher extends AbstractServiceLauncherTestBase {

  private Object[] valueSetForTestingServicesAndConstructors()throws Throwable {
    return new Object[] {
        new Object[] {RunningService.NAME},
        new Object[] {NullBindLaunchableService.NAME},
        new Object[] {StringConstructorOnlyService.NAME},
        new Object[] {InitInConstructorLaunchableService.NAME},
        new Object[] {NoArgsAllowedService.NAME},
        new Object[] {NoArgsAllowedService.NAME, LauncherArguments.ARG_CONF_PREFIXED, configFile(newConf())},
        new Object[] {LaunchableRunningService.NAME},
        new Object[] {StoppingInStartLaunchableService.NAME}
    };
  }

  @Test
  @Parameters(method = "valueSetForTestingServicesAndConstructors")
  public void testRunServicesAndConstructors(String... args) throws Throwable {
    assertRuns(args); // used parameter directly
  }

  /**
   * Test the behaviour of service stop logic.
   */
  @Test
  public void testStopInStartup() throws Throwable {
    FailingStopInStartService svc = new FailingStopInStartService();
    svc.init(new Configuration());
    svc.start();
    assertStopped(svc);
    Throwable cause = svc.getFailureCause();
    assertNotNull(cause);
    assertTrue(cause instanceof ServiceLaunchException);
    assertTrue(svc.waitForServiceToStop(0));
    ServiceLaunchException e = (ServiceLaunchException) cause;
    assertEquals(FailingStopInStartService.EXIT_CODE, e.getExitCode());
  }

  private Object[] valueSetForTestingExceptionAndExits()throws Throwable {
    return new Object[] {
        new Object[] {EXIT_EXCEPTION_THROWN, OTHER_EXCEPTION_TEXT, NAME},
        new Object[] {EXIT_OTHER_FAILURE, SLE_TEXT, NAME, ARG_THROW_SLE},
        new Object[] {IOE_EXIT_CODE, EXIT_IN_IOE_TEXT, NAME, ARG_THROW_IOE},
        new Object[] {EXIT_EXCEPTION_THROWN, "java.lang.OutOfMemoryError", NAME, ARG_THROWABLE},
        new Object[] {EXIT_COMMAND_ARGUMENT_ERROR, "1", NoArgsAllowedService.NAME, "one"},
        new Object[] {EXIT_OTHER_FAILURE, "", LaunchableRunningService.NAME, LaunchableRunningService.ARG_FAILING},
    };
  }

  /**
   * Value set 2 verifies that exceptions in the
   * {@link LaunchableService#execute()} method are relayed if an instance of
   * an exit exceptions, and forwarded if not.
   */
  @Test
  @Parameters(method = "valueSetForTestingExceptionAndExits")
  public void testExceptionsAndExits(int expected, String text, String... args) throws Throwable {
    assertLaunchOutcome(expected, text, args); // used parameter directly
  }

  /**
   * As the exception is doing some formatting tricks, these
   * tests verify that exception arguments are being correctly
   * used as initializers.
   */
  @Test
  @Parameters({
  "32,020",
  "64,040",
  "96,060",
  "128,080",
  "16,010"
  })
  public void testBasicExceptionFormatting(int argValue, String expectedText) throws Throwable {
    ServiceLaunchException ex = new ServiceLaunchException(0, "%03x", argValue);
    assertExceptionContains(expectedText, ex); // used parameter directly
  }

  @Test
  @Parameters({
  "%03x",
  "%04x",
  "%12x",
  "%03y",
  "@03y",
  "@@@",
  "format",
  "213",
  ""
  })
  public void testNotEnoughArgsExceptionFormatting(String format) throws Throwable {
    ServiceLaunchException ex = new ServiceLaunchException(0, format);
    assertExceptionContains(format, ex); // used parameter directly
  }

  private Object[] valueSetForCause() {
    return new Object[] {
                new Object[] {"cause"},
                new Object[] {""},
                new Object[] {"   "},
                new Object[] {"!@#$%^&*()"},
                new Object[] {"123@abc"},
                new Object[] {"abc@123"},
    };
  }

  @Test
  @Parameters(method = "valueSetForCause")
  public void testInnerCause(String causeStr) throws Throwable {

    Exception cause = new Exception(causeStr);
    ServiceLaunchException ex =
        new ServiceLaunchException(0, "%03x: %s", 32, cause);
    assertExceptionContains("020", ex);
    assertExceptionContains(causeStr, ex); // used parameter directly
    assertSame(cause, ex.getCause());
  }

  @Test
  @Parameters(method = "valueSetForCause")
  public void testInnerCauseNotInFormat(String causeStr) throws Throwable {
    Assume.assumeTrue(causeStr != "");
    Exception cause = new Exception(causeStr);
    ServiceLaunchException ex =
        new ServiceLaunchException(0, "%03x:", 32, cause);
    assertExceptionContains("020", ex);
    assertFalse(ex.getMessage().contains(causeStr)); // used parameter directly
    assertSame(cause, ex.getCause());
  }

  @Test
  public void testShutdownHookNullReference() throws Throwable {
    new ServiceShutdownHook(null).run();
  }

  @Test
  public void testShutdownHook() throws Throwable {
    BreakableService service = new BreakableService();
    setServiceToTeardown(service);
    ServiceShutdownHook hook = new ServiceShutdownHook(service);
    hook.run();
    assertStopped(service);
  }

  @Test
  @Parameters({
  "false, false, true",
  "false, false, false",
  "false, true, true",
  "false, true, false",
  "true, false, true",
  "true, false, false",
  "true, true, true",
  "true, true, false",
  })
  public void testFailingHookCaught(boolean failOnInit, boolean failOnStart, boolean failOnStop) throws Throwable {
    BreakableService service = new BreakableService(failOnInit, failOnStart, failOnStop);
    setServiceToTeardown(service);
    ServiceShutdownHook hook = new ServiceShutdownHook(service);
    hook.run();
    assertStopped(service);
  }

}
