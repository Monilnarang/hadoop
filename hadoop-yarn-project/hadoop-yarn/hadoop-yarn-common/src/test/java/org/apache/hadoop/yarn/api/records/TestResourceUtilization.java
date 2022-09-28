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

package org.apache.hadoop.yarn.api.records;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestResourceUtilization {

  @Parameterized.Parameter(value = 0)
  public int pmem;
  @Parameterized.Parameter(value = 1)
  public int vmem;
  @Parameterized.Parameter(value = 2)
  public float cpu;

    @Parameterized.Parameters
    public static Collection<Object> testData() {
      Object[][] data = new Object[][] { {10, 20, 0.5f}, {20, 15, 0.55f}
      };
      return Arrays.asList(data);
    }

  // PUTs #34
  @Test
  public void testResourceUtilization() {
    ResourceUtilization u1 = ResourceUtilization.newInstance(pmem, vmem, cpu);
    ResourceUtilization u2 = ResourceUtilization.newInstance(u1);
    ResourceUtilization u3 = ResourceUtilization.newInstance(pmem, vmem, cpu);
    ResourceUtilization u4 = ResourceUtilization.newInstance(pmem+10, vmem, cpu);
    ResourceUtilization u5 = ResourceUtilization.newInstance(pmem+20, vmem+20, cpu + 0.3f);

    Assert.assertEquals(u1, u2); // no change in assertion
    Assert.assertEquals(u1, u3); // no change in assertion
    Assert.assertNotEquals(u1, u4);
    Assert.assertNotEquals(u2, u5);
    Assert.assertNotEquals(u4, u5);

    Assert.assertTrue(u1.hashCode() == u2.hashCode());
    Assert.assertTrue(u1.hashCode() == u3.hashCode());
    Assert.assertFalse(u1.hashCode() == u4.hashCode());
    Assert.assertFalse(u2.hashCode() == u5.hashCode());
    Assert.assertFalse(u4.hashCode() == u5.hashCode());

    Assert.assertTrue(u1.getPhysicalMemory() == pmem);
    Assert.assertFalse(u1.getVirtualMemory() == vmem - 10);
    Assert.assertTrue(u1.getCPU() == cpu);

    Assert.assertEquals("<pmem:" + pmem + ", vmem:" + u1.getVirtualMemory()
        + ", vCores:" + String.valueOf(cpu) + ">", u1.toString()); // basic manipulation of parameter

    u1.addTo(10, 0, 0.0f);
    Assert.assertNotEquals(u1, u2);
    Assert.assertEquals(u1, u4); // no change in assertion
    u1.addTo(10, 20, 0.3f);
    Assert.assertEquals(u1, u5); // no change in assertion
    u1.subtractFrom(10, 20, 0.3f);
    Assert.assertEquals(u1, u4); // no change in assertion
    u1.subtractFrom(10, 0, 0.0f);
    Assert.assertEquals(u1, u3); // no change in assertion
  }

  // PUTs #35
  @Test
  public void testResourceUtilizationWithCustomResource() {
    Map<String, Float> customResources = new HashMap<>();
    customResources.put(ResourceInformation.GPU_URI, 5.0f);
    ResourceUtilization u1 = ResourceUtilization.
        newInstance(pmem, vmem, cpu, customResources);
    ResourceUtilization u2 = ResourceUtilization.newInstance(u1);
    ResourceUtilization u3 = ResourceUtilization.
        newInstance(pmem, vmem, cpu, customResources);
    ResourceUtilization u4 = ResourceUtilization.
        newInstance(pmem + 10, vmem, cpu, customResources);
    ResourceUtilization u5 = ResourceUtilization.
        newInstance(pmem + 20, vmem + 20, cpu + 0.3f, customResources);

    Assert.assertEquals(u1, u2); // no change in assertion
    Assert.assertEquals(u1, u3); // no change in assertion
    Assert.assertNotEquals(u1, u4);
    Assert.assertNotEquals(u2, u5);
    Assert.assertNotEquals(u4, u5);

    Assert.assertTrue(u1.hashCode() == u2.hashCode());
    Assert.assertTrue(u1.hashCode() == u3.hashCode());
    Assert.assertFalse(u1.hashCode() == u4.hashCode());
    Assert.assertFalse(u2.hashCode() == u5.hashCode());
    Assert.assertFalse(u4.hashCode() == u5.hashCode());

    Assert.assertTrue(u1.getPhysicalMemory() == pmem);
    Assert.assertFalse(u1.getVirtualMemory() == vmem - 10);
    Assert.assertTrue(u1.getCPU() == cpu);
    Assert.assertTrue(u1.
        getCustomResource(ResourceInformation.GPU_URI) == 5.0f);

    Assert.assertEquals("<pmem:" + pmem + ", vmem:" + u1.getVirtualMemory()
        + ", vCores:" + String.valueOf(cpu) + ", yarn.io/gpu:5.0>", u1.toString()); // basic manipulation of parameter

    u1.addTo(10, 0, 0.0f);
    Assert.assertNotEquals(u1, u2);
    Assert.assertEquals(u1, u4); // no change in assertion
    u1.addTo(10, 20, 0.3f);
    Assert.assertEquals(u1, u5); // no change in assertion
    u1.subtractFrom(10, 20, 0.3f);
    Assert.assertEquals(u1, u4); // no change in assertion
    u1.subtractFrom(10, 0, 0.0f);
    Assert.assertEquals(u1, u3); // no change in assertion
  }
}
