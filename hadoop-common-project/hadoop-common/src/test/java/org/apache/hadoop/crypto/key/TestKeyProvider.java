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
package org.apache.hadoop.crypto.key;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

@RunWith(JUnitParamsRunner.class)
public class TestKeyProvider {

  private static final String CIPHER = "AES";

  private Object[] valueSetForNameAndVersion() {
    return new Object[] {
                new Object[] {"/a/b", 3},
                new Object[] { "/aaa", 12 },
                new Object[] { "", 0 },
                new Object[] { "@#!&^^&$@", -1 },
                new Object[] { "@@", -99999 },
                new Object[] { "qwertyuioplkjhgfdsazxcvbnm,,./<>?;'[]}{|+_)(*&^%$#@!-=", Integer.MIN_VALUE },
                new Object[] { null, 9999},
    };
  }

  @Test
  @Parameters(method = "valueSetForNameAndVersion")
  public void testBuildVersionName(final String name, final Integer version) throws Exception {
    assertEquals(name + "@" + version, KeyProvider.buildVersionName(name, version)); // formula
  }

  private Object[] valueSetForValidAndInvalidValues() {
    return new Object[] {
                new Object[] {"/a/b@3", "/a/b"},
                new Object[] { "/aaa@12", null},
                new Object[] { "@", "" },
                new Object[] { "@#!&^^&$@", -1 },
                new Object[] { "@@", -99999 },
                new Object[] { "qwertyuioplkjhgfdsazxcvbnm,,./<>?;'[]}{|+_)(*&^%$#@!-=", Integer.MIN_VALUE },
                new Object[] { "123456789@0987654321", "1234567890987654321"},
    };
  }

  @Test
  @Parameters(method = "valueSetForValidAndInvalidValues")
  public void testParseVersionName(String validVersionname, String invalidVersionname) throws Exception {
    assertEquals(validVersionname.substring(0, validVersionname.lastIndexOf('@')), KeyProvider.getBaseName(validVersionname)); // formula
    try {
      KeyProvider.getBaseName(invalidVersionname);
      assertTrue("should have thrown", false);
    } catch (IOException e) {
      assertTrue(true);
    }
  }

  private Object[] valueSetForKeyMaterial() {
    return new Object[] {
                new Object[] {new byte[]{1,2,3,4}, "key1", "1"},
                new Object[] {new byte[]{'a','b','c','d'}, "key@@@@", "1123123"},
                new Object[] {new byte[]{1,'a',3,'4',' ',' '}, "!@#$%^&*()", "asfjkbs"},
                new Object[] {new byte[]{}, "", ""},
                new Object[] {new byte[]{'@','*','!','#','$','%','^','&','@','(',')'}, ":';<>?/.,[]}{|", "!@#$%^)("},

    };
  }

  @Test
  @Parameters(method = "valueSetForKeyMaterial")
  public void testKeyMaterial(byte[] material, final String name, final String version) throws Exception {
    KeyProvider.KeyVersion obj = new KeyProvider.KeyVersion(name, name + "@" + version, material);
    assertEquals(name + "@" + version, obj.getVersionName()); // formula
    assertArrayEquals(material, obj.getMaterial()); // used parameter directly
  }

  @Test
  public void testMetadata() throws Exception {
    //Metadata without description
    DateFormat format = new SimpleDateFormat("y/m/d");
    Date date = format.parse("2013/12/25");
    KeyProvider.Metadata meta = new KeyProvider.Metadata("myCipher", 100, null,
        null, date, 123);
    assertEquals("myCipher", meta.getCipher());
    assertEquals(100, meta.getBitLength());
    assertNull(meta.getDescription());
    assertEquals(date, meta.getCreated());
    assertEquals(123, meta.getVersions());
    KeyProvider.Metadata second = new KeyProvider.Metadata(meta.serialize());
    assertEquals(meta.getCipher(), second.getCipher());
    assertEquals(meta.getBitLength(), second.getBitLength());
    assertNull(second.getDescription());
    assertTrue(second.getAttributes().isEmpty());
    assertEquals(meta.getCreated(), second.getCreated());
    assertEquals(meta.getVersions(), second.getVersions());
    int newVersion = second.addVersion();
    assertEquals(123, newVersion);
    assertEquals(124, second.getVersions());
    assertEquals(123, meta.getVersions());

    //Metadata with description
    format = new SimpleDateFormat("y/m/d");
    date = format.parse("2013/12/25");
    Map<String, String> attributes = new HashMap<String, String>();
    attributes.put("a", "A");
    meta = new KeyProvider.Metadata("myCipher", 100,
        "description", attributes, date, 123);
    assertEquals("myCipher", meta.getCipher());
    assertEquals(100, meta.getBitLength());
    assertEquals("description", meta.getDescription());
    assertEquals(attributes, meta.getAttributes());
    assertEquals(date, meta.getCreated());
    assertEquals(123, meta.getVersions());
    second = new KeyProvider.Metadata(meta.serialize());
    assertEquals(meta.getCipher(), second.getCipher());
    assertEquals(meta.getBitLength(), second.getBitLength());
    assertEquals(meta.getDescription(), second.getDescription());
    assertEquals(meta.getAttributes(), second.getAttributes());
    assertEquals(meta.getCreated(), second.getCreated());
    assertEquals(meta.getVersions(), second.getVersions());
    newVersion = second.addVersion();
    assertEquals(123, newVersion);
    assertEquals(124, second.getVersions());
    assertEquals(123, meta.getVersions());
  }

  @Test
  public void testOptions() throws Exception {
    Configuration conf = new Configuration();
    conf.set(KeyProvider.DEFAULT_CIPHER_NAME, "myCipher");
    conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 512);
    Map<String, String> attributes = new HashMap<String, String>();
    attributes.put("a", "A");
    KeyProvider.Options options = KeyProvider.options(conf);
    assertEquals("myCipher", options.getCipher());
    assertEquals(512, options.getBitLength());
    options.setCipher("yourCipher");
    options.setDescription("description");
    options.setAttributes(attributes);
    options.setBitLength(128);
    assertEquals("yourCipher", options.getCipher());
    assertEquals(128, options.getBitLength());
    assertEquals("description", options.getDescription());
    assertEquals(attributes, options.getAttributes());
    options = KeyProvider.options(new Configuration());
    assertEquals(KeyProvider.DEFAULT_CIPHER, options.getCipher());
    assertEquals(KeyProvider.DEFAULT_BITLENGTH, options.getBitLength());
  }

  @Test
  public void testUnnestUri() throws Exception {
    assertUnwraps("hdfs://nn.example.com/my/path",
        "myscheme://hdfs@nn.example.com/my/path");
    assertUnwraps("hdfs://nn/my/path?foo=bar&baz=bat#yyy",
        "myscheme://hdfs@nn/my/path?foo=bar&baz=bat#yyy");
    assertUnwraps("inner://hdfs@nn1.example.com/my/path",
        "outer://inner@hdfs@nn1.example.com/my/path");
    assertUnwraps("user:///", "outer://user/");
    assertUnwraps("wasb://account@container/secret.jceks",
        "jceks://wasb@account@container/secret.jceks");
    assertUnwraps("abfs://account@container/secret.jceks",
        "jceks://abfs@account@container/secret.jceks");
    assertUnwraps("s3a://container/secret.jceks",
        "jceks://s3a@container/secret.jceks");
    assertUnwraps("file:///tmp/secret.jceks",
        "jceks://file/tmp/secret.jceks");
    assertUnwraps("https://user:pass@service/secret.jceks?token=aia",
        "jceks://https@user:pass@service/secret.jceks?token=aia");
  }

  protected void assertUnwraps(final String unwrapped, final String outer)
      throws URISyntaxException {
    assertEquals(new Path(unwrapped),
        ProviderUtils.unnestUri(new URI(outer)));
  }

  private static class MyKeyProvider extends KeyProvider {
    private String algorithm;
    private int size;
    private byte[] material;

    public MyKeyProvider(Configuration conf) {
      super(conf);
    }

    @Override
    public KeyVersion getKeyVersion(String versionName)
        throws IOException {
      return null;
    }

    @Override
    public List<String> getKeys() throws IOException {
      return null;
    }

    @Override
    public List<KeyVersion> getKeyVersions(String name)
        throws IOException {
      return null;
    }

    @Override
    public Metadata getMetadata(String name) throws IOException {
      if (!"unknown".equals(name)) {
        return new Metadata(CIPHER, 128, "description", null, new Date(), 0);
      }
      return null;
    }

    @Override
    public KeyVersion createKey(String name, byte[] material,
        Options options) throws IOException {
      this.material = material;
      return null;
    }

    @Override
    public void deleteKey(String name) throws IOException {

    }

    @Override
    public KeyVersion rollNewVersion(String name, byte[] material)
        throws IOException {
      this.material = material;
      return null;
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    protected byte[] generateKey(int size, String algorithm)
        throws NoSuchAlgorithmException {
      this.size = size;
      this.algorithm = algorithm;
      return super.generateKey(size, algorithm);
    }
  }

  @Test
  public void testMaterialGeneration() throws Exception {
    MyKeyProvider kp = new MyKeyProvider(new Configuration());
    KeyProvider.Options options = new KeyProvider.Options(new Configuration());
    options.setCipher(CIPHER);
    options.setBitLength(128);
    kp.createKey("hello", options);
    Assert.assertEquals(128, kp.size);
    Assert.assertEquals(CIPHER, kp.algorithm);
    Assert.assertNotNull(kp.material);

    kp = new MyKeyProvider(new Configuration());
    kp.rollNewVersion("hello");
    Assert.assertEquals(128, kp.size);
    Assert.assertEquals(CIPHER, kp.algorithm);
    Assert.assertNotNull(kp.material);
  }

  @Test
  public void testRolloverUnknownKey() throws Exception {
    MyKeyProvider kp = new MyKeyProvider(new Configuration());
    KeyProvider.Options options = new KeyProvider.Options(new Configuration());
    options.setCipher(CIPHER);
    options.setBitLength(128);
    kp.createKey("hello", options);
    Assert.assertEquals(128, kp.size);
    Assert.assertEquals(CIPHER, kp.algorithm);
    Assert.assertNotNull(kp.material);

    kp = new MyKeyProvider(new Configuration());
    try {
      kp.rollNewVersion("unknown");
      fail("should have thrown");
    } catch (IOException e) {
      String expectedError = "Can't find Metadata for key";
      GenericTestUtils.assertExceptionContains(expectedError, e);
    }
  }

  @Test
  public void testConfiguration() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("a", "A");
    MyKeyProvider kp = new MyKeyProvider(conf);
    Assert.assertEquals("A", kp.getConf().get("a"));
  }

}
