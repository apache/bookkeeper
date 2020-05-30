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
package org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.fs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Unit test of {@link FSCheckpointManager}.
 */
public class FSCheckpointManagerTest {

    private static final byte[] TEST_BYTES = "fs-checkpoint-manager".getBytes(UTF_8);

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();
    @Rule
    public final TestName runtime = new TestName();

    private File rootDir;
    private FSCheckpointManager cm;

    @Before
    public void setUp() throws Exception {
        this.rootDir = testFolder.newFolder("checkpoints");
        this.cm = new FSCheckpointManager(rootDir);
    }

    @Test
    public void testListFilesEmpty() throws Exception {
        new File(rootDir, runtime.getMethodName()).mkdir();
        assertTrue(cm.listFiles(runtime.getMethodName()).isEmpty());
    }

    @Test
    public void testListFilesNotFound() throws Exception {
        assertTrue(cm.listFiles(runtime.getMethodName()).isEmpty());
    }

    @Test
    public void testListFiles() throws Exception {
        int numFiles = 3;
        List<String> expectedFiles = Lists.newArrayListWithExpectedSize(3);

        File testDir = new File(rootDir, runtime.getMethodName());
        testDir.mkdir();
        for (int i = 0; i < numFiles; ++i) {
            String filename = runtime.getMethodName() + "-" + i;
            expectedFiles.add(filename);
            new File(testDir, filename).mkdir();
        }
        List<String> files = cm.listFiles(runtime.getMethodName());
        Collections.sort(files);

        assertEquals(expectedFiles, files);
    }

    @Test
    public void testFileExists() throws Exception {
        File testDir = new File(new File(rootDir, runtime.getMethodName()), "test");
        testDir.mkdirs();
        assertTrue(cm.fileExists(runtime.getMethodName() + "/test"));
        assertFalse(cm.fileExists(runtime.getMethodName() + "/test2"));
    }

    @Test
    public void testFileRename() throws Exception {
        File srcDir = new File(rootDir, "src");
        srcDir.mkdir();
        File destDir = new File(rootDir, "dest");
        destDir.mkdir();

        String srcFilePath = "src/" + runtime.getMethodName();
        String destFilePath = "dest/" + runtime.getMethodName();
        OutputStream os = cm.openOutputStream(srcFilePath);
        os.write(TEST_BYTES);
        os.flush();
        os.close();

        cm.rename(srcFilePath, destFilePath);
        assertTrue(cm.fileExists(destFilePath));
        assertFalse(cm.fileExists(srcFilePath));
    }

    @Test
    public void testFileRenameDirNotExists() throws Exception {
        File srcDir = new File(rootDir, "src");
        srcDir.mkdir();

        String srcFilePath = "src/" + runtime.getMethodName();
        String destFilePath = "dest/" + runtime.getMethodName();
        OutputStream os = cm.openOutputStream(srcFilePath);
        os.write(TEST_BYTES);
        os.flush();
        os.close();

        try {
            cm.rename(srcFilePath, destFilePath);
            fail("Should fail to rename if the dest dir doesn't exist");
        } catch (NoSuchFileException e) {
            // expected
        }
        assertFalse(cm.fileExists(destFilePath));
        assertTrue(cm.fileExists(srcFilePath));
    }

}
