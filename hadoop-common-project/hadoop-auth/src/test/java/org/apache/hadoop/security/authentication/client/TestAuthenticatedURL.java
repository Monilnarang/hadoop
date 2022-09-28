/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.client;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.Arrays;

@RunWith(Enclosed.class)
public class TestAuthenticatedURL {

  @RunWith(Parameterized.class)
  public static class TheParameterizedPart {
    @Parameterized.Parameter(value = 0)
    public String tokenStr;

    @Parameterized.Parameters
    public static Collection<Object> testData() {
      Object[] data = new Object[] {"foo",
                                    "RanDOMstrING",
//                                    "", // invalid input ; fails
                                    "123@456#"};
      return Arrays.asList(data);
    }

    // PUTs #3
  @Test
  public void testToken() throws Exception {
    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    Assert.assertFalse(token.isSet());
    token = new AuthenticatedURL.Token(tokenStr);
    Assert.assertTrue(token.isSet());
    Assert.assertEquals(tokenStr, token.toString());
  }

    // PUTs #4
  @Test
  public void testExtractTokenOK() throws Exception {
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);

    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);

    Map<String, List<String>> headers = new HashMap<String, List<String>>();
    List<String> cookies = new ArrayList<String>();
    cookies.add(AuthenticatedURL.AUTH_COOKIE + "=" + tokenStr);
    headers.put("Set-Cookie", cookies);
    Mockito.when(conn.getHeaderFields()).thenReturn(headers);

    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    AuthenticatedURL.extractToken(conn, token);

    Assert.assertEquals(tokenStr, token.toString());
  }

  }

  public static class NotParameterizedPart {

    // CUTs #1
    @Test(expected = IllegalArgumentException.class)
    public void testEmptyTokenFailure() {
      new AuthenticatedURL.Token(null);
    }

    @Test
    public void testInjectToken() throws Exception {
      HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
      AuthenticatedURL.Token token = new AuthenticatedURL.Token();
      token.set("foo");
      AuthenticatedURL.injectToken(conn, token);
      Mockito.verify(conn).addRequestProperty(Mockito.eq("Cookie"), Mockito.anyString());
    }


  @Test
  public void testExtractTokenFail() throws Exception {
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);

    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_UNAUTHORIZED);

    String tokenStr = "foo";
    Map<String, List<String>> headers = new HashMap<String, List<String>>();
    List<String> cookies = new ArrayList<String>();
    cookies.add(AuthenticatedURL.AUTH_COOKIE + "=" + tokenStr);
    headers.put("Set-Cookie", cookies);
    Mockito.when(conn.getHeaderFields()).thenReturn(headers);

    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    token.set("bar");
    try {
      AuthenticatedURL.extractToken(conn, token);
      Assert.fail();
    } catch (AuthenticationException ex) {
      // Expected
      Assert.assertFalse(token.isSet());
    } catch (Exception ex) {
      Assert.fail();
    }
  }

  @Test
  public void testConnectionConfigurator() throws Exception {
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getResponseCode()).
        thenReturn(HttpURLConnection.HTTP_UNAUTHORIZED);

    ConnectionConfigurator connConf =
        Mockito.mock(ConnectionConfigurator.class);
    Mockito.when(connConf.configure(Mockito.<HttpURLConnection>any())).
        thenReturn(conn);

    Authenticator authenticator = Mockito.mock(Authenticator.class);

    AuthenticatedURL aURL = new AuthenticatedURL(authenticator, connConf);
    aURL.openConnection(new URL("http://foo"), new AuthenticatedURL.Token());
    Mockito.verify(connConf).configure(Mockito.<HttpURLConnection>any());
  }

  @Test
  public void testGetAuthenticator() throws Exception {
    Authenticator authenticator = Mockito.mock(Authenticator.class);

    AuthenticatedURL aURL = new AuthenticatedURL(authenticator);
    Assert.assertEquals(authenticator, aURL.getAuthenticator());
    }
  }

}
