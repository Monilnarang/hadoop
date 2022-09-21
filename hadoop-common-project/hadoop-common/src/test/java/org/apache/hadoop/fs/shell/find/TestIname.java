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
import static org.apache.hadoop.fs.shell.find.TestHelper.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.shell.PathData;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class TestIname {
  private FileSystem mockFs;
  private Name.Iname name;

  @Rule
  public Timeout globalTimeout = new Timeout(10000, TimeUnit.MILLISECONDS);

  @Before
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();
  }

  private void setup(String arg) throws IOException {
    name = new Name.Iname();
    addArgument(name, arg);
    name.setOptions(new FindOptions());
    name.prepare();
  }

  // test a matching name (same case)
  @Test
  @Parameters({
  "name",
  "nameasdf",
  "NAME",
  "name@123",
  "!@#$%^&*()-=+_",
  "123123",
  "",
  "     ",
  })
  public void applyMatch(final String matchingName) throws IOException {
    setup(matchingName);
    PathData item = new PathData("/directory/path/" + matchingName, mockFs.getConf());
    assertEquals(Result.PASS, name.apply(item, -1));
  }

  // test a non-matching name
  @Test
  @Parameters({
  "name,notname",
  "nameasdf,knjsbahdsa",
  "NAME,KJILHJKH",
  "name@123,@123",
  "!@#$%^&*()-=+_,)(*&^%$#@!",
  "123123,123 123",
  ",null",
  "      ,    ",
  "      ,",
  "name,name",
  })
  public void applyNotMatch(String name, final String nonMatchingName) throws IOException {
    Assume.assumeTrue(!name.equals(nonMatchingName));
    setup(name);
    PathData item = new PathData("/directory/path/" + nonMatchingName, mockFs.getConf());
    assertEquals(Result.FAIL, this.name.apply(item, -1));
  }

  // test a matching name (different case)
  @Test
  @Parameters({
  "name,NaMe",
  "nAmE,NaMe",
  "NAME,namE",
  "NAME,name",
  "name,NAME",
  "name@123,NAME@123",
  " A , a    ",
  "n*e,name", // test a matching glob pattern (same case)
  "n*,name",
  "*,name@123",
  "*,",
  "*,     ",
  "*,   a  ",
  "a*,   a  ",
  "n*e,NaMe", // test a matching glob pattern (different case)
  "n*,NAMe",
  "*,NaME@12345",
  "*,   AA  ",
  "***, name@NAME ",
  })
  public void applyGlobAndMixedCases(String name, final String matchingName) throws IOException {
    setup(name);
    PathData item = new PathData("/directory/path/" + matchingName, mockFs.getConf());
    assertEquals(Result.PASS, this.name.apply(item, -1));
  }

  // test a non-matching glob pattern
  @Test
  @Parameters({
  "n*e,notmatch",
  "*e,naM@123",
  "***name,namename123",
  "name**,amename",
  })
  public void applyGlobNotMatch(String arg, final String nonMatchName) throws IOException {
    setup(arg);
    PathData item = new PathData("/directory/path/" + nonMatchName, mockFs.getConf());
    assertEquals(Result.FAIL, name.apply(item, -1));
  }
}
