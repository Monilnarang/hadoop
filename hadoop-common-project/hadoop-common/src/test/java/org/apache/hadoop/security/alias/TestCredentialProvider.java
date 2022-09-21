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
package org.apache.hadoop.security.alias;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

@RunWith(JUnitParamsRunner.class)
public class TestCredentialProvider {

  private Object[] valueSetForTestCredentialEntry() {
    return new Object[] {
        new Object[] {new char[]{1, 2, 3, 4}, "cred1"},
        new Object[] {new char[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, "cred123456789"},
        new Object[] {new char[]{1}, "00"},
        new Object[] {new char[]{'a', 'b', 'c'}, "    "},
        new Object[] {new char[]{1, 'b', '@', '#'}, "123@abc"},
        new Object[] {new char[]{}, ""},
    };
  }

  @Test
  @Parameters(method = "valueSetForTestCredentialEntry")
  public void testCredentialEntry(char [] key1, String alias) throws Exception {
    CredentialProvider.CredentialEntry obj =
        new CredentialProvider.CredentialEntry(alias, key1);
    assertEquals(alias, obj.getAlias()); // used parameter directly
    assertArrayEquals(key1, obj.getCredential()); // used parameter directly
  }

  @Test
  @Parameters({
  "hdfs://nn.example.com/my/path, myscheme://hdfs@nn.example.com/my/path",
  "hdfs://nn/my/path?foo=bar&baz=bat#yyy, myscheme://hdfs@nn/my/path?foo=bar&baz=bat#yyy",
  "inner://hdfs@nn1.example.com/my/path, outer://inner@hdfs@nn1.example.com/my/path",
  "user:///, outer://user/",
  "rootuser:///, outer://rootuser/",
  "inner://rootuser?a=b&c=123, outer://inner@rootuser?a=b&c=123",
  "https://rootuser#abcxy23, outer://https@rootuser#abcxy23"
  })
  public void testUnnestUri(String pathString, String uriString) throws Exception {
    assertEquals(new Path(pathString), ProviderUtils.unnestUri(new URI(uriString))); // used parameter directly
    // give time to thinking about generalizing above assert (unnestUri found to be too complex for a one liner)
  }
}
