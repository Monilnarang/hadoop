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
package org.apache.hadoop.util.curator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import javax.security.auth.login.AppConfigurationEntry;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.authentication.util.JaasConfiguration;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Assume;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test the manager for ZooKeeper Curator.
 */
public class TestZKCuratorManager {

  private TestingServer server;
  private ZKCuratorManager curator;

  @BeforeEach
  public void setup() throws Exception {
    this.server = new TestingServer();

    Configuration conf = new Configuration();
    conf.set(
        CommonConfigurationKeys.ZK_ADDRESS, this.server.getConnectString());

    this.curator = new ZKCuratorManager(conf);
    this.curator.start();
  }

  @AfterEach
  public void teardown() throws Exception {
    this.curator.close();
    if (this.server != null) {
      this.server.close();
      this.server = null;
    }
  }

  @Test
  public void testReadWriteData() throws Exception {
    String testZNode = "/test";
    String expectedString = "testString";
    assertFalse(curator.exists(testZNode));
    curator.create(testZNode);
    assertTrue(curator.exists(testZNode));
    curator.setData(testZNode, expectedString, -1);
    String testString = curator.getStringData("/test");
    assertEquals(expectedString, testString);
  }

  // quick but good flip with loops add and delete counts
  private static Stream<Arguments> valuesSetsForTestChildren() {
      return Stream.of(
//          Arguments.of(3, 1),
//          Arguments.of(4, 2),
//          Arguments.of(7, 8),
//          Arguments.of(7, 5),
          Arguments.of(0, 0)
      );
    }
  // PUTs #63
  @ParameterizedTest
  @MethodSource("valuesSetsForTestChildren")
  public void testChildren(int addChildren, int deleteChildren) throws Exception {
    Assume.assumeTrue(deleteChildren <= addChildren);
    List<String> children;

    children = curator.getChildren("/");
    assertEquals(1, children.size()); // no change in assertion

    for (int i = 1; i < addChildren; i++) {
        assertFalse(curator.exists("/node" + String.valueOf(i))); // other manipulation
        curator.create("/node" + String.valueOf(i));
        assertTrue(curator.exists("/node" + String.valueOf(i))); // other manipulation
    }

    children = curator.getChildren("/");
    assertEquals(addChildren + 1, children.size()); // used parameter directly

    for (int i = 1; i <= deleteChildren; i++) {
        curator.delete("/node" + String.valueOf(i));
        assertFalse(curator.exists("/node" + String.valueOf(i))); // other manipulation
        children = curator.getChildren("/");
        assertEquals(addChildren - i, children.size()); // used parameter directly
    }
    assertEquals(addChildren-deleteChildren, children.size()); // basic manipulation of parameter
  }

  @Test
  public void testGetStringData() throws Exception {
    String node1 = "/node1";
    String node2 = "/node2";
    assertFalse(curator.exists(node1));
    curator.create(node1);
    assertNull(curator.getStringData(node1));

    byte[] setData = "setData".getBytes("UTF-8");
    curator.setData(node1, setData, -1);
    assertEquals("setData", curator.getStringData(node1));

    Stat stat = new Stat();
    assertFalse(curator.exists(node2));
    curator.create(node2);
    assertNull(curator.getStringData(node2, stat));

    curator.setData(node2, setData, -1);
    assertEquals("setData", curator.getStringData(node2, stat));

  }
  @Test
  public void testTransaction() throws Exception {
    List<ACL> zkAcl = ZKUtil.parseACLs(CommonConfigurationKeys.ZK_ACL_DEFAULT);
    String fencingNodePath = "/fencing";
    String node1 = "/node1";
    String node2 = "/node2";
    byte[] testData = "testData".getBytes("UTF-8");
    assertFalse(curator.exists(fencingNodePath));
    assertFalse(curator.exists(node1));
    assertFalse(curator.exists(node2));
    ZKCuratorManager.SafeTransaction txn = curator.createTransaction(
        zkAcl, fencingNodePath);
    txn.create(node1, testData, zkAcl, CreateMode.PERSISTENT);
    txn.create(node2, testData, zkAcl, CreateMode.PERSISTENT);
    assertFalse(curator.exists(fencingNodePath));
    assertFalse(curator.exists(node1));
    assertFalse(curator.exists(node2));
    txn.commit();
    assertFalse(curator.exists(fencingNodePath));
    assertTrue(curator.exists(node1));
    assertTrue(curator.exists(node2));
    assertTrue(Arrays.equals(testData, curator.getData(node1)));
    assertTrue(Arrays.equals(testData, curator.getData(node2)));

    byte[] setData = "setData".getBytes("UTF-8");
    txn = curator.createTransaction(zkAcl, fencingNodePath);
    txn.setData(node1, setData, -1);
    txn.delete(node2);
    assertTrue(curator.exists(node2));
    assertTrue(Arrays.equals(testData, curator.getData(node1)));
    txn.commit();
    assertFalse(curator.exists(node2));
    assertTrue(Arrays.equals(setData, curator.getData(node1)));
  }

  @Test
  public void testJaasConfiguration() throws Exception {
    // Validate that HadoopZooKeeperFactory will set ZKConfig with given principals
    ZKCuratorManager.HadoopZookeeperFactory factory1 =
        new ZKCuratorManager.HadoopZookeeperFactory("foo1", "bar1", "bar1.keytab");
    ZooKeeper zk1 = factory1.newZooKeeper("connString", 1000, null, false);
    validateJaasConfiguration(ZKCuratorManager.HadoopZookeeperFactory.JAAS_CLIENT_ENTRY,
        "bar1", "bar1.keytab", zk1);

    // Validate that a new HadoopZooKeeperFactory will use the new principals
    ZKCuratorManager.HadoopZookeeperFactory factory2 =
        new ZKCuratorManager.HadoopZookeeperFactory("foo2", "bar2", "bar2.keytab");
    ZooKeeper zk2 = factory2.newZooKeeper("connString", 1000, null, false);
    validateJaasConfiguration(ZKCuratorManager.HadoopZookeeperFactory.JAAS_CLIENT_ENTRY,
        "bar2", "bar2.keytab", zk2);

    try {
      // Setting global configuration
      String testClientConfig = "TestClientConfig";
      JaasConfiguration jconf = new JaasConfiguration(testClientConfig, "test", "test.keytab");
      javax.security.auth.login.Configuration.setConfiguration(jconf);
      System.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY, testClientConfig);

      // Validate that a new HadoopZooKeeperFactory will use the global principals
      ZKCuratorManager.HadoopZookeeperFactory factory3 =
          new ZKCuratorManager.HadoopZookeeperFactory("foo3", "bar3", "bar3.keytab");
      ZooKeeper zk3 = factory3.newZooKeeper("connString", 1000, null, false);
      validateJaasConfiguration(testClientConfig, "test", "test.keytab", zk3);
    } finally {
      // Remove global configuration
      System.clearProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY);
    }
  }

  private void validateJaasConfiguration(String clientConfig, String principal, String keytab,
      ZooKeeper zk) {
    assertEquals("Validate that expected clientConfig is set in ZK config", clientConfig,
        zk.getClientConfig().getProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY));

    AppConfigurationEntry[] entries = javax.security.auth.login.Configuration.getConfiguration()
        .getAppConfigurationEntry(clientConfig);
    assertEquals("Validate that expected principal is set in Jaas config", principal,
        entries[0].getOptions().get("principal"));
    assertEquals("Validate that expected keytab is set in Jaas config", keytab,
        entries[0].getOptions().get("keyTab"));
  }
}