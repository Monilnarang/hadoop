/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.viewfs;

import java.net.URI;
import java.util.List;
import java.util.function.Function;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


/**
 * Unit test of nested mount point support in INodeTree
 */
@RunWith(JUnitParamsRunner.class)
public class TestNestedMountPoint {
  private InodeTree inodeTree;
  private Configuration conf;
  private String mtName;
  private URI fsUri;

  static class TestNestMountPointFileSystem {
    public URI getUri() {
      return uri;
    }

    private URI uri;

    TestNestMountPointFileSystem(URI uri) {
      this.uri = uri;
    }
  }

  static class TestNestMountPointInternalFileSystem extends TestNestMountPointFileSystem {
    TestNestMountPointInternalFileSystem(URI uri) {
      super(uri);
    }
  }

  private static final URI LINKFALLBACK_TARGET = URI.create("hdfs://nn00");
  private static final URI NN1_TARGET = URI.create("hdfs://nn01/a/b");
  private static final URI NN2_TARGET = URI.create("hdfs://nn02/a/b/e");
  private static final URI NN3_TARGET = URI.create("hdfs://nn03/a/b/c/d");
  private static final URI NN4_TARGET = URI.create("hdfs://nn04/a/b/c/d/e");
  private static final URI NN5_TARGET = URI.create("hdfs://nn05/b/c/d/e");
  private static final URI NN6_TARGET = URI.create("hdfs://nn06/b/c/d/e/f");

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    mtName = TestNestedMountPoint.class.getName();
    ConfigUtil.setIsNestedMountPointSupported(conf, true);
    ConfigUtil.addLink(conf, mtName, "/a/b", NN1_TARGET);
    ConfigUtil.addLink(conf, mtName, "/a/b/e", NN2_TARGET);
    ConfigUtil.addLink(conf, mtName, "/a/b/c/d", NN3_TARGET);
    ConfigUtil.addLink(conf, mtName, "/a/b/c/d/e", NN4_TARGET);
    ConfigUtil.addLink(conf, mtName, "/b/c/d/e", NN5_TARGET);
    ConfigUtil.addLink(conf, mtName, "/b/c/d/e/f", NN6_TARGET);
    ConfigUtil.addLinkFallback(conf, mtName, LINKFALLBACK_TARGET);

    fsUri = new URI(FsConstants.VIEWFS_SCHEME, mtName, "/", null, null);

    inodeTree = new InodeTree<TestNestedMountPoint.TestNestMountPointFileSystem>(conf,
        mtName, fsUri, false) {
      @Override
      protected Function<URI, TestNestedMountPoint.TestNestMountPointFileSystem> initAndGetTargetFs() {
        return new Function<URI, TestNestMountPointFileSystem>() {
          @Override
          public TestNestedMountPoint.TestNestMountPointFileSystem apply(URI uri) {
            return new TestNestMountPointFileSystem(uri);
          }
        };
      }

      // For intenral dir fs
      @Override
      protected TestNestedMountPoint.TestNestMountPointInternalFileSystem getTargetFileSystem(
          final INodeDir<TestNestedMountPoint.TestNestMountPointFileSystem> dir) {
        return new TestNestMountPointInternalFileSystem(fsUri);
      }

      @Override
      protected TestNestedMountPoint.TestNestMountPointInternalFileSystem getTargetFileSystem(
          final String settings, final URI[] mergeFsURIList) {
        return new TestNestMountPointInternalFileSystem(null);
      }
    };
  }

  @After
  public void tearDown() throws Exception {
    inodeTree = null;
  }

  private Object[] valueSetForTestPathResolveToLink() {
    return new Object[] {
                // Old Test 1 testPathResolveToLink
                // /a/b/c/d/e/f resolves to /a/b/c/d/e and /f
                new Object[] {"/a/b/c/d/e/f","/a/b/c/d/e","/f", true, NN4_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/c/d/e resolves to /a/b/c/d/e and /
                new Object[] {"/a/b/c/d/e","/a/b/c/d/e","/", true, NN4_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/c/d/e/f/g/h/i resolves to /a/b/c/d/e and /f/g/h/i
                new Object[] {"/a/b/c/d/e/f/g/h/i", "/a/b/c/d/e", "/f/g/h/i",
                    true, NN4_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/c/d/e/f/g/ resolves to /a/b/c/d/e and /f/g
                new Object[] {"/a/b/c/d/e/f/g", "/a/b/c/d/e", "/f/g",
                    true, NN4_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/c/d/e/f/g/h/i/j/k/l/m resolves to /a/b/c/d/e and /f/g/h/i/j/k/l/m
                new Object[] {"/a/b/c/d/e/f/g/h/i/j/k/l/m", "/a/b/c/d/e", "/f/g/h/i/j/k/l/m",
                    true, NN4_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/c/d/e/f resolves to /a/b/c/d/e and /f
                // Old Test 2 testPathResolveToLinkNotResolveLastComponent
                new Object[] {"/a/b/c/d/e/f", "/a/b/c/d/e", "/f", false, NN4_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/c/d/e resolves to /a/b/c/d and /e
                new Object[] {"/a/b/c/d/e", "/a/b/c/d", "/e", false, NN3_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/c/d/e/f/g/h/i resolves to /a/b/c/d/e and /f/g/h/i
                new Object[] {"/a/b/c/d/e/f/g/h/i", "/a/b/c/d/e", "/f/g/h/i",
                    false, NN4_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/e/c/d/a/g/h/i resolves to /a/b/e and /c/d/a/g/h/i
                new Object[] {"/a/b/e/c/d/a/g/h/i", "/a/b/e", "/c/d/a/g/h/i",
                    false, NN2_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/a/c/d/a/g/h/i resolves to /a/b and /a/c/d/a/g/h/i
                new Object[] {"/a/b/a/c/d/a/g/h/i", "/a/b", "/a/c/d/a/g/h/i",
                    false, NN1_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /b/c/d/e/d/a/g/h/i resolves to /b/c/d/e and /d/a/g/h/i
                new Object[] {"/b/c/d/e/d/a/g/h/i", "/b/c/d/e", "/d/a/g/h/i",
                    false, NN5_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /b/c/d/e/f/d/a/g/h/i resolves to /b/c/d/e/f and /d/a/g/h/i
                new Object[] {"/b/c/d/e/f/d/a/g/h/i", "/b/c/d/e/f", "/d/a/g/h/i",
                    false, NN6_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // Old Test 3 testPathResolveToDirLink
                // /a/b/c/d/f resolves to /a/b/c/d, /f
                new Object[] {"/a/b/c/d/f", "/a/b/c/d", "/f", true, NN3_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/c/d resolves to /a/b/c/d and /
                new Object[] {"/a/b/c/d", "/a/b/c/d", "/", true, NN3_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/c/d/f/g/h/i resolves to /a/b/c/d and /f/g/h/i
                new Object[] {"/a/b/c/d/f/g/h/i", "/a/b/c/d", "/f/g/h/i",
                    true, NN3_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // Old Test 4 testPathResolveToDirLinkNotResolveLastComponent
                // /a/b/c/d/f resolves to /a/b/c/d, /f
                new Object[] {"/a/b/c/d/f", "/a/b/c/d", "/f", false, NN3_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/c/d resolves to /a/b and /c/d
                new Object[] {"/a/b/c/d", "/a/b", "/c/d", false, NN1_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b/c/d/f/g/h/i resolves to /a/b/c/d and /f/g/h/i
                new Object[] {"/a/b/c/d/f/g/h/i", "/a/b/c/d", "/f/g/h/i",
                    false, NN3_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // Old Test 5 testMultiNestedMountPointsPathResolveToDirLink
                // /a/b/f resolves to /a/b and /f
                new Object[] {"/a/b/f", "/a/b", "/f", true, NN1_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b resolves to /a/b and /
                new Object[] {"/a/b", "/a/b", "/", true, NN1_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // Old Test 6 testMultiNestedMountPointsPathResolveToDirLinkNotResolveLastComponent
                // /a/b/f resolves to /a/b and /f
                new Object[] {"/a/b/f", "/a/b", "/f", false, NN1_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // /a/b resolves to /a and /b
                new Object[] {"/a/b", "/a", "/b", false, fsUri, InodeTree.ResultKind.INTERNAL_DIR},
                // Old Test 7 testPathResolveToDirLinkLastComponentInternalDir
                // /a/b/c resolves to /a/b and /c
                new Object[] {"/a/b/c", "/a/b", "/c", true, NN1_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // Old Test 8 testPathResolveToDirLinkLastComponentInternalDirNotResolveLastComponent
                // /a/b/c resolves to /a/b and /c
                new Object[] {"/a/b/c", "/a/b", "/c", false, NN1_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // Old Test 9 testPathResolveToLinkFallBack
                // /a/e resolves to linkfallback
                new Object[] {"/a/e", "/", "/a/e", true, LINKFALLBACK_TARGET, InodeTree.ResultKind.EXTERNAL_DIR},
                // Old Test 10 testPathNotResolveToLinkFallBackNotResolveLastComponent
                // /a/e resolves to internalDir instead of linkfallback
                new Object[] {"/a/e", "/a", "/e", false, LINKFALLBACK_TARGET, InodeTree.ResultKind.INTERNAL_DIR},
                // Old Test 11 testPathResolveToInternalDir
                // /b/c resolves to internal dir
                new Object[] {"/b/c", "/b/c", "/", true, LINKFALLBACK_TARGET, InodeTree.ResultKind.INTERNAL_DIR},
                // Old Test 12 testPathResolveToInternalDirNotResolveLastComponent
                // /b/c resolves to internal dir
                new Object[] {"/b/c", "/b", "/c", false, LINKFALLBACK_TARGET, InodeTree.ResultKind.INTERNAL_DIR},
                // Old Test 13 testSlashResolveToInternalDir
                // / resolves to internal dir
                new Object[] {"/", "/", "/", true, LINKFALLBACK_TARGET, InodeTree.ResultKind.INTERNAL_DIR},
    };
  }

  @Test
  @Parameters(method = "valueSetForTestPathResolveToLink")
  public void testPathResolveToLink(String path, String resolvedPath, String remainingPath,
        boolean resolveLastComponent, URI expectedURI, InodeTree.ResultKind resultKind) throws Exception {

    InodeTree.ResolveResult resolveResult = inodeTree.resolve(path, resolveLastComponent);
    Assert.assertEquals(resultKind, resolveResult.kind); // used parameter directly
    Assert.assertEquals(resolvedPath, resolveResult.resolvedPath); // used parameter directly
    Assert.assertEquals(new Path(remainingPath), resolveResult.remainingPath); // used parameter directly
    if (resultKind == InodeTree.ResultKind.EXTERNAL_DIR) {
        Assert.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
        Assert.assertEquals(expectedURI, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri()); // used parameter directly
        if (expectedURI == LINKFALLBACK_TARGET) {
            Assert.assertFalse(resolveResult.isLastInternalDirLink());
        } else {
            Assert.assertTrue(resolveResult.isLastInternalDirLink());
        }
    } else {
        Assert.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointInternalFileSystem);
        Assert.assertEquals(fsUri, ((TestNestMountPointInternalFileSystem) resolveResult.targetFileSystem).getUri());
        Assert.assertFalse(resolveResult.isLastInternalDirLink());
    }
  }

  @Test
  public void testInodeTreeMountPoints() throws Exception {
    List<InodeTree.MountPoint<FileSystem>> mountPoints = inodeTree.getMountPoints();
    Assert.assertEquals(6, mountPoints.size());
  }
}
