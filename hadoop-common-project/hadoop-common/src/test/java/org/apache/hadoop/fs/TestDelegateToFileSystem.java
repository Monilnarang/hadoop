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
package org.apache.hadoop.fs;

import java.net.URI;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.net.ftp.FTP;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Assume;import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class TestDelegateToFileSystem {

  private static final String FTP_DUMMYHOST = "ftp://dummyhost";
  private static final URI FTP_URI_NO_PORT = URI.create(FTP_DUMMYHOST);
  private static final URI FTP_URI_WITH_PORT = URI.create(FTP_DUMMYHOST + ":"
      + FTP.DEFAULT_PORT);

  private void testDefaultUriInternal(String defaultUri)
      throws UnsupportedFileSystemException {
    final Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, defaultUri);
    final AbstractFileSystem ftpFs =
        AbstractFileSystem.get(FTP_URI_NO_PORT, conf);
    Assert.assertEquals(FTP_URI_WITH_PORT, ftpFs.getUri());
  }

  @Test
  @Parameters({
  "hdfs://dummyhost",
  "",
  "    ",
  "hdfs://someText@123",
  "hdfs://someText",
  "http://localhost",
  "http://local host",
  "abcd.//",
  "abcdaa/",
  "abcdaaa",
  "a",
  })
  public void testDefaultURIwithOutPort(String defaultUri) throws Exception {
    Assume.assumeTrue(!defaultUri.contains(" ") && !defaultUri.isEmpty());
    testDefaultUriInternal(defaultUri);
  }

  @Test
  @Parameters({
  "hdfs://dummyhost:8020",
  "",
  "    ",
  "8020",
  "8080",
  "hdfs://someText@123:123",
  "hdfs://someText:12312",
  "http://localhost:8080",
  "http://local host:9090",
  "abcd.//:2012",
  "abcdaa/:123",
  "abcdaaa:6666",
  "a",
  })
  public void testDefaultURIwithPort(String defaultUri) throws Exception {
    Assume.assumeTrue(!defaultUri.contains(" ") && !defaultUri.isEmpty());
    testDefaultUriInternal(defaultUri);
  }
}
