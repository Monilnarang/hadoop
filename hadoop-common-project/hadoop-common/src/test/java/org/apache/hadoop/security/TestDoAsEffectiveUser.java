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

import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.TestRpcBase;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.DefaultImpersonationProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Test do as effective user.
 */
public class TestDoAsEffectiveUser extends TestRpcBase {
  final private static String REAL_USER_NAME = "realUser1@HADOOP.APACHE.ORG";
  final private static String REAL_USER_SHORT_NAME = "realUser1";
  final private static String PROXY_USER_NAME = "proxyUser";
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String[] GROUP_NAMES = new String[] { GROUP1_NAME,
      GROUP2_NAME };

  private TestRpcService client;
  private static final Configuration masterConf = new Configuration();
  
  
  public static final Logger LOG = LoggerFactory
      .getLogger(TestDoAsEffectiveUser.class);
  
  
  static {
    masterConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL,
        "RULE:[2:$1@$0](.*@HADOOP.APACHE.ORG)s/@.*//" +
        "RULE:[1:$1@$0](.*@HADOOP.APACHE.ORG)s/@.*//"
        + "DEFAULT");
  }

  @BeforeEach
  public void setMasterConf() throws IOException {
    UserGroupInformation.setConfiguration(masterConf);
    refreshConf(masterConf);
  }

  private void configureSuperUserIPAddresses(Configuration conf,
      String superUserShortName) throws IOException {
    ArrayList<String> ipList = new ArrayList<>();
    Enumeration<NetworkInterface> netInterfaceList = NetworkInterface
        .getNetworkInterfaces();
    while (netInterfaceList.hasMoreElements()) {
      NetworkInterface inf = netInterfaceList.nextElement();
      Enumeration<InetAddress> addrList = inf.getInetAddresses();
      while (addrList.hasMoreElements()) {
        InetAddress addr = addrList.nextElement();
        ipList.add(addr.getHostAddress());
      }
    }
    StringBuilder builder = new StringBuilder();
    for (String ip : ipList) {
      builder.append(ip);
      builder.append(',');
    }
    builder.append("127.0.1.1,");
    builder.append(InetAddress.getLocalHost().getCanonicalHostName());
    LOG.info("Local Ip addresses: "+builder.toString());
    conf.setStrings(DefaultImpersonationProvider.getTestProvider().
            getProxySuperuserIpConfKey(superUserShortName),
        builder.toString());
  }
  
  /**
   * Test method for
   * {@link org.apache.hadoop.security.UserGroupInformation#createProxyUser(java.lang.String, org.apache.hadoop.security.UserGroupInformation)}
   * .
   */
  private static Stream<Arguments> valuesSetsForTestCreateProxyUser() {
      return Stream.of(
          Arguments.of("realUser1@HADOOP.APACHE.ORG", "proxyUser"),
          Arguments.of("realUser2@HADOOP.APACHE.ORG", "username"),
          Arguments.of("userName123123@HADOOP.APACHE.ORG", "GarbageValue")
      );
    }
  // PUTs #44
  @ParameterizedTest
  @MethodSource("valuesSetsForTestCreateProxyUser")
  public void testCreateProxyUser(String realUserName, String proxyUserName) throws Exception {
    // ensure that doAs works correctly
    UserGroupInformation realUserUgi = UserGroupInformation
        .createRemoteUser(realUserName);
    UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUser(
        proxyUserName, realUserUgi);
    UserGroupInformation curUGI = proxyUserUgi
        .doAs(new PrivilegedExceptionAction<UserGroupInformation>() {
          @Override
          public UserGroupInformation run() throws IOException {
            return UserGroupInformation.getCurrentUser();
          }
        });
    Assert.assertEquals(
        proxyUserName + " (auth:PROXY) via " + realUserName + " (auth:SIMPLE)",
        curUGI.toString()); // basic manipulation of parameter
  }

  private void checkRemoteUgi(final UserGroupInformation ugi,
                              final Configuration conf) throws Exception {
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws ServiceException {
        client = getClient(addr, conf);
        String currentUser = client.getCurrentUser(null,
            newEmptyRequest()).getUser();
        String serverRemoteUser = client.getServerRemoteUser(null,
            newEmptyRequest()).getUser();

        Assert.assertEquals(ugi.toString(), currentUser);
        Assert.assertEquals(ugi.toString(), serverRemoteUser);
        return null;
      }
    });    
  }
  
  @Test
  @Timeout(value = 4000, unit = TimeUnit.MILLISECONDS)
  public void testRealUserSetup() throws IOException {
    final Configuration conf = new Configuration();
    conf.setStrings(DefaultImpersonationProvider.getTestProvider().
        getProxySuperuserGroupConfKey(REAL_USER_SHORT_NAME), "group1");
    configureSuperUserIPAddresses(conf, REAL_USER_SHORT_NAME);
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, TestRpcService.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation.setConfiguration(conf);
    final Server server = setupTestServer(conf, 5);

    refreshConf(conf);
    try {
      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);
      checkRemoteUgi(realUserUgi, conf);
      
      UserGroupInformation proxyUserUgi =
          UserGroupInformation.createProxyUserForTesting(
          PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      checkRemoteUgi(proxyUserUgi, conf);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      stop(server, client);
    }
  }

  @Test
  @Timeout(value = 4000, unit = TimeUnit.MILLISECONDS)
  public void testRealUserAuthorizationSuccess() throws IOException {
    final Configuration conf = new Configuration();
    configureSuperUserIPAddresses(conf, REAL_USER_SHORT_NAME);
    conf.setStrings(DefaultImpersonationProvider.getTestProvider().
            getProxySuperuserGroupConfKey(REAL_USER_SHORT_NAME),
        "group1");
    RPC.setProtocolEngine(conf, TestRpcService.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation.setConfiguration(conf);
    final Server server = setupTestServer(conf, 5);

    refreshConf(conf);
    try {
      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);
      checkRemoteUgi(realUserUgi, conf);

      UserGroupInformation proxyUserUgi = UserGroupInformation
          .createProxyUserForTesting(PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      checkRemoteUgi(proxyUserUgi, conf);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      stop(server, client);
    }
  }

  /*
   * Tests authorization of superuser's ip.
   */
  @Test
  public void testRealUserIPAuthorizationFailure() throws IOException {
    final Configuration conf = new Configuration();
    conf.setStrings(DefaultImpersonationProvider.getTestProvider().
            getProxySuperuserIpConfKey(REAL_USER_SHORT_NAME),
        "20.20.20.20"); //Authorized IP address
    conf.setStrings(DefaultImpersonationProvider.getTestProvider().
            getProxySuperuserGroupConfKey(REAL_USER_SHORT_NAME),
        "group1");
    RPC.setProtocolEngine(conf, TestRpcService.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation.setConfiguration(conf);
    final Server server = setupTestServer(conf, 5);

    refreshConf(conf);
    
    try {
      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);

      UserGroupInformation proxyUserUgi = UserGroupInformation
          .createProxyUserForTesting(PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      String retVal = proxyUserUgi
          .doAs(new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws ServiceException {
              client = getClient(addr, conf);
              return client.getCurrentUser(null,
                  newEmptyRequest()).getUser();
            }
          });

      Assert.fail("The RPC must have failed " + retVal);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      stop(server, client);
    }
  }
  
  @Test
  public void testRealUserIPNotSpecified() throws IOException {
    final Configuration conf = new Configuration();
    conf.setStrings(DefaultImpersonationProvider.getTestProvider().
        getProxySuperuserGroupConfKey(REAL_USER_SHORT_NAME), "group1");
    RPC.setProtocolEngine(conf, TestRpcService.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation.setConfiguration(conf);
    final Server server = setupTestServer(conf, 2);

    refreshConf(conf);

    try {
      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);

      UserGroupInformation proxyUserUgi = UserGroupInformation
          .createProxyUserForTesting(PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      String retVal = proxyUserUgi
          .doAs(new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws ServiceException {
              client = getClient(addr, conf);
              return client.getCurrentUser(null,
                  newEmptyRequest()).getUser();
            }
          });

      Assert.fail("The RPC must have failed " + retVal);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      stop(server, client);
    }
  }

  @Test
  public void testRealUserGroupNotSpecified() throws IOException {
    final Configuration conf = new Configuration();
    configureSuperUserIPAddresses(conf, REAL_USER_SHORT_NAME);
    RPC.setProtocolEngine(conf, TestRpcService.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation.setConfiguration(conf);
    final Server server = setupTestServer(conf, 2);

    try {
      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);

      UserGroupInformation proxyUserUgi = UserGroupInformation
          .createProxyUserForTesting(PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      String retVal = proxyUserUgi
          .doAs(new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws ServiceException {
              client = getClient(addr, conf);
              return client.getCurrentUser(null,
                  newEmptyRequest()).getUser();
            }
          });

      Assert.fail("The RPC must have failed " + retVal);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      stop(server, client);
    }
  }
  
  @Test
  public void testRealUserGroupAuthorizationFailure() throws IOException {
    final Configuration conf = new Configuration();
    configureSuperUserIPAddresses(conf, REAL_USER_SHORT_NAME);
    conf.setStrings(DefaultImpersonationProvider.getTestProvider().
            getProxySuperuserGroupConfKey(REAL_USER_SHORT_NAME),
        "group3");
    RPC.setProtocolEngine(conf, TestRpcService.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation.setConfiguration(conf);
    final Server server = setupTestServer(conf, 2);
    
    refreshConf(conf);

    try {
      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);

      UserGroupInformation proxyUserUgi = UserGroupInformation
          .createProxyUserForTesting(PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      String retVal = proxyUserUgi
          .doAs(new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws ServiceException {
              client = getClient(addr, conf);
              return client.getCurrentUser(null,
                  newEmptyRequest()).getUser();
            }
          });

      Assert.fail("The RPC must have failed " + retVal);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      stop(server, client);
    }
  }

  /*
   *  Tests the scenario when token authorization is used.
   *  The server sees only the the owner of the token as the
   *  user.
   */
  private static Stream<Arguments> valuesSetsForTestProxyWithToken() {
      return Stream.of(
                Arguments.of("realUser1@HADOOP.APACHE.ORG", "proxyUser"),
                Arguments.of("realUser2@HADOOP.APACHE.ORG", "username"),
                Arguments.of("userName123123@HADOOP.APACHE.ORG", "GarbageValue")
            );
    }
  // PUTs #45
  @ParameterizedTest
  @MethodSource("valuesSetsForTestProxyWithToken")
  public void testProxyWithToken(String realUserName, String proxyUserName) throws Exception {
    final Configuration conf = new Configuration(masterConf);
    TestTokenSecretManager sm = new TestTokenSecretManager();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    RPC.setProtocolEngine(conf, TestRpcService.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation.setConfiguration(conf);
    final Server server = setupTestServer(conf, 5, sm);

    final UserGroupInformation current = UserGroupInformation
        .createRemoteUser(realUserName);

    TestTokenIdentifier tokenId = new TestTokenIdentifier(new Text(current
        .getUserName()), new Text("SomeSuperUser"));
    Token<TestTokenIdentifier> token = new Token<>(tokenId,
        sm);
    SecurityUtil.setTokenService(token, addr);
    UserGroupInformation proxyUserUgi = UserGroupInformation
        .createProxyUserForTesting(proxyUserName, current, GROUP_NAMES);
    proxyUserUgi.addToken(token);
    
    refreshConf(conf);

    String retVal = proxyUserUgi.doAs(new PrivilegedExceptionAction<String>() {
      @Override
      public String run() throws Exception {
        try {
          client = getClient(addr, conf);
          return client.getCurrentUser(null,
              newEmptyRequest()).getUser();
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        } finally {
          stop(server, client);
        }
      }
    });
    //The user returned by server must be the one in the token.
    Assert.assertEquals(realUserName + " (auth:TOKEN) via SomeSuperUser (auth:SIMPLE)", retVal); // basic manipulation of parameter
  }

  /*
   * The user gets the token via a superuser. Server should authenticate
   * this user. 
   */
  private static Stream<Arguments> valuesSetsForTestTokenBySuperUser() {
      return Stream.of(
          Arguments.of("realUser1@HADOOP.APACHE.ORG"),
          Arguments.of("realUser2@HADOOP.APACHE.ORG"),
          Arguments.of("userName123123@HADOOP.APACHE.ORG")
      );
    }
  // PUTs #46
  @ParameterizedTest
  @MethodSource("valuesSetsForTestTokenBySuperUser")
  public void testTokenBySuperUser(String realUserName) throws Exception {
    TestTokenSecretManager sm = new TestTokenSecretManager();
    final Configuration newConf = new Configuration(masterConf);
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, newConf);
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(newConf, TestRpcService.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation.setConfiguration(newConf);
    final Server server = setupTestServer(newConf, 5, sm);

    final UserGroupInformation current = UserGroupInformation
        .createUserForTesting(realUserName, GROUP_NAMES);
    
    refreshConf(newConf);

    TestTokenIdentifier tokenId = new TestTokenIdentifier(new Text(current
        .getUserName()), new Text("SomeSuperUser"));
    Token<TestTokenIdentifier> token = new Token<>(tokenId, sm);
    SecurityUtil.setTokenService(token, addr);
    current.addToken(token);
    String retVal = current.doAs(new PrivilegedExceptionAction<String>() {
      @Override
      public String run() throws Exception {
        try {
          client = getClient(addr, newConf);
          return client.getCurrentUser(null,
              newEmptyRequest()).getUser();
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        } finally {
          stop(server, client);
        }
      }
    });
    String expected = realUserName + " (auth:TOKEN) via SomeSuperUser (auth:SIMPLE)";
    Assert.assertEquals(retVal + "!=" + expected, expected, retVal); // no change in assertion
  }
  
  //
  private void refreshConf(Configuration conf) throws IOException {
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }
}
