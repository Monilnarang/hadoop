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
package org.apache.hadoop.nfs;

import org.junit.Assert;

import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.oncrpc.XDR;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestNfsTime {

  @Parameterized.Parameter(value = 0)
  public int milliseconds;

  @Parameterized.Parameters
  public static Collection<Object> testMilliSeconds() {
    Object[] data = new Object[] {0, -1, -101000, 1001, 1000, 999999999, -4567654};
    return Arrays.asList(data);
  }

  // PUTs #17
  @Test
  public void testConstructor() {
    NfsTime nfstime = new NfsTime(milliseconds);
    Assert.assertEquals(milliseconds/1000, nfstime.getSeconds());
    Assert.assertEquals((milliseconds - (milliseconds/1000)*1000) * 1000000, nfstime.getNseconds());
  }
  // PUTs #18
  @Test
  public void testSerializeDeserialize() {
    // Serialize NfsTime
    NfsTime t1 = new NfsTime(milliseconds);
    XDR xdr = new XDR();
    t1.serialize(xdr);
    
    // Deserialize it back
    NfsTime t2 = NfsTime.deserialize(xdr.asReadOnlyWrap());
    
    // Ensure the NfsTimes are equal
    Assert.assertEquals(t1, t2);
  }
}
