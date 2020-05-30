/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.dlog;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.fs.FSCheckpointManager;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test of {@link FSCheckpointManager}.
 */
public class DLCheckpointStoreTest extends TestDistributedLogBase {

    private static final byte[] TEST_BYTES = "dlog-checkpoint-manager".getBytes(UTF_8);

    @Rule
    public final TestName runtime = new TestName();

    private URI uri;
    private Namespace namespace;
    private DLCheckpointStore store;

    @BeforeClass
    public static void setupDL() throws Exception {
        setupCluster();
    }

    @AfterClass
    public static void teardownDL() throws Exception {
        teardownCluster();
    }

    @Before
    public void setUp() throws Exception {
        this.uri = DLMTestUtil.createDLMURI(zkPort, "/" + runtime.getMethodName());
        ensureURICreated(this.uri);
        this.namespace = NamespaceBuilder.newBuilder()
            .conf(new DistributedLogConfiguration())
            .uri(uri)
            .build();
        this.store = new DLCheckpointStore(namespace);
    }

    @After
    public void tearDown() throws Exception {
        if (null != store) {
            store.close();
        }
    }

    @Test
    public void testListFilesEmpty() throws Exception {
        // create a dummy log stream to ensure "dir" exists
        namespace.createLog(runtime.getMethodName());
        assertTrue(store.listFiles(runtime.getMethodName()).isEmpty());
    }

    @Test
    public void testListFilesNotFound() throws Exception {
        assertTrue(store.listFiles(runtime.getMethodName()).isEmpty());
    }

    @Test
    public void testListFiles() throws Exception {
        int numFiles = 3;
        List<String> expectedFiles = Lists.newArrayListWithExpectedSize(3);

        namespace.createLog(runtime.getMethodName());
        for (int i = 0; i < numFiles; ++i) {
            String filename = runtime.getMethodName() + "-" + i;
            expectedFiles.add(filename);
            namespace.createLog(runtime.getMethodName() + "/" + filename);
        }
        List<String> files = store.listFiles(runtime.getMethodName());
        Collections.sort(files);

        assertEquals(expectedFiles, files);
    }

    @Test
    public void testFileExists() throws Exception {
        namespace.createLog(runtime.getMethodName() + "/test");
        assertTrue(store.fileExists(runtime.getMethodName() + "/test"));
        assertFalse(store.fileExists(runtime.getMethodName() + "/test2"));
    }

    @Test
    public void testFileRename() throws Exception {
        namespace.createLog("src");
        namespace.createLog("dest");

        String srcFilePath = "src/" + runtime.getMethodName();
        String destFilePath = "dest/" + runtime.getMethodName();
        OutputStream os = store.openOutputStream(srcFilePath);
        os.write(TEST_BYTES);
        os.flush();
        os.close();

        store.rename(srcFilePath, destFilePath);
        assertTrue(store.fileExists(destFilePath));
        assertFalse(store.fileExists(srcFilePath));

        assertEquals(TEST_BYTES.length, store.getFileLength(destFilePath));

        try (InputStream is = store.openInputStream(destFilePath)) {
            byte[] readBytes = new byte[TEST_BYTES.length];
            ByteStreams.readFully(is, readBytes);

            assertArrayEquals(TEST_BYTES, readBytes);
        }
    }

    @Test
    public void testFileRenameDirNotExists() throws Exception {
        namespace.createLog("src");
        assertFalse(store.fileExists("dest"));

        String srcFilePath = "src/" + runtime.getMethodName();
        String destFilePath = "dest/" + runtime.getMethodName();

        assertFalse(store.fileExists(srcFilePath));

        OutputStream os = store.openOutputStream(srcFilePath);
        os.write(TEST_BYTES);
        os.flush();
        os.close();

        // rename will automatically create stream path in dlog
        store.rename(srcFilePath, destFilePath);
        assertTrue(store.fileExists(destFilePath));
        assertFalse(store.fileExists(srcFilePath));

        assertEquals(TEST_BYTES.length, store.getFileLength(destFilePath));

        try (InputStream is = store.openInputStream(destFilePath)) {
            byte[] readBytes = new byte[TEST_BYTES.length];
            ByteStreams.readFully(is, readBytes);

            assertArrayEquals(TEST_BYTES, readBytes);
        }
    }

    @Test
    public void testFileRenameFileExists() throws Exception {
        namespace.createLog("src");
        assertFalse(store.fileExists("dest"));

        String srcFilePath = "src/" + runtime.getMethodName();
        String destFilePath = "dest/" + runtime.getMethodName();
        namespace.createLog(destFilePath);
        assertTrue(store.fileExists(destFilePath));

        assertFalse(store.fileExists(srcFilePath));

        OutputStream os = store.openOutputStream(srcFilePath);
        os.write(TEST_BYTES);
        os.flush();
        os.close();

        assertTrue(store.fileExists(srcFilePath));

        try {
            store.rename(srcFilePath, destFilePath);
            fail("Should fail to rename if the dest dir doesn't exist");
        } catch (FileAlreadyExistsException e) {
            // expected
        }
        assertTrue(store.fileExists(destFilePath));
        assertTrue(store.fileExists(srcFilePath));
        assertEquals(0, store.getFileLength(destFilePath));
    }

    @Test
    public void testDelete() throws Exception {
        int numFiles = 3;
        List<String> expectedFiles = Lists.newArrayListWithExpectedSize(3);

        namespace.createLog(runtime.getMethodName());
        for (int i = 0; i < numFiles; ++i) {
            String filename = runtime.getMethodName() + "-" + i;
            expectedFiles.add(filename);
            namespace.createLog(runtime.getMethodName() + "/" + filename);
        }
        List<String> files = store.listFiles(runtime.getMethodName());
        Collections.sort(files);

        assertEquals(expectedFiles, files);

        store.delete(runtime.getMethodName());

        assertFalse(store.fileExists(runtime.getMethodName()));
    }

    @Test
    public void testDeleteRecursively() throws Exception {
        int numFiles = 3;
        List<String> expectedFiles = Lists.newArrayListWithExpectedSize(3);

        namespace.createLog(runtime.getMethodName());
        for (int i = 0; i < numFiles; ++i) {
            String filename = runtime.getMethodName() + "-" + i;
            expectedFiles.add(filename);
            namespace.createLog(runtime.getMethodName() + "/" + filename);
        }
        List<String> files = store.listFiles(runtime.getMethodName());
        Collections.sort(files);

        assertEquals(expectedFiles, files);

        store.delete(runtime.getMethodName());

        assertFalse(store.fileExists(runtime.getMethodName()));
    }

}
