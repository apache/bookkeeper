/*
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

package org.apache.distributedlog.fs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration test for {@link DLFileSystem}.
 */
@Slf4j
public class TestDLFileSystem extends TestDLFSBase {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test(expected = FileNotFoundException.class)
    public void testOpenFileNotFound() throws Exception {
        Path path = new Path("not-found-file");
        fs.open(path, 1024);
    }

    @Test
    public void testBasicIO() throws Exception {
        Path path = new Path("/path/to/" + runtime.getMethodName());

        assertFalse(fs.exists(path));

        try (FSDataOutputStream out = fs.create(path)) {
            for (int i = 0; i < 100; i++) {
                out.writeBytes("line-" + i + "\n");
            }
            out.flush();
        }
        assertTrue(fs.exists(path));

        File tempFile = tmpDir.newFile();
        Path localDst = new Path(tempFile.getPath());
        // copy the file
        fs.copyToLocalFile(path, localDst);
        // copy the file to dest
        fs.copyFromLocalFile(localDst, new Path(runtime.getMethodName() + "-copied"));

        // rename
        Path dstPath = new Path(runtime.getMethodName() + "-renamed");
        fs.rename(path, dstPath);
        assertFalse(fs.exists(path));
        assertTrue(fs.exists(dstPath));

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(dstPath, 1134)))) {
            int lineno = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                assertEquals("line-" + lineno, line);
                ++lineno;
            }
            assertEquals(100, lineno);
        }


        // delete the file
        fs.delete(dstPath, false);
        assertFalse(fs.exists(dstPath));
    }

    @Test
    public void testListStatuses() throws Exception {
        Path parentPath = new Path("/path/to/" + runtime.getMethodName());
        assertFalse(fs.exists(parentPath));
        try (FSDataOutputStream parentOut = fs.create(parentPath)) {
            parentOut.writeBytes("parent");
            parentOut.flush();
        }
        assertTrue(fs.exists(parentPath));

        int numLogs = 3;
        for (int i = 0; i < numLogs; i++) {
            Path path = new Path("/path/to/" + runtime.getMethodName()
                + "/" + runtime.getMethodName() + "-" + i);
            assertFalse(fs.exists(path));
            try (FSDataOutputStream out = fs.create(path)) {
                out.writeBytes("line");
                out.flush();
            }
            assertTrue(fs.exists(path));
        }
        FileStatus[] files = fs.listStatus(new Path("/path/to/" + runtime.getMethodName()));

        assertEquals(3, files.length);
        for (int i = 0; i < numLogs; i++) {
            FileStatus file = files[i];
            assertEquals(4, file.getLen());
            assertFalse(file.isDirectory());
            assertEquals(3, file.getReplication());
            assertEquals(0L, file.getModificationTime());
            assertEquals(
                new Path("/path/to/" + runtime.getMethodName() + "/" + runtime.getMethodName() + "-" + i),
                file.getPath());
        }
    }

    @Test
    public void testMkDirs() throws Exception {
        Path path = new Path("/path/to/" + runtime.getMethodName());
        assertFalse(fs.exists(path));
        assertTrue(fs.mkdirs(path));
        assertTrue(fs.exists(path));
        assertTrue(fs.mkdirs(path));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testTruncation() throws Exception {
        Path path = new Path("/path/to/" + runtime.getMethodName());
        fs.truncate(path, 10);
    }

    @Test
    public void testDeleteRecursive() throws Exception {
        int numLogs = 3;
        for (int i = 0; i < numLogs; i++) {
            Path path = new Path("/path/to/" + runtime.getMethodName()
                + "/" + runtime.getMethodName() + "-" + i);
            assertFalse(fs.exists(path));
            try (FSDataOutputStream out = fs.create(path)) {
                out.writeBytes("line");
                out.flush();
            }
            assertTrue(fs.exists(path));
        }

        fs.delete(new Path("/path/to/" + runtime.getMethodName()), true);
        FileStatus[] files = fs.listStatus(new Path("/path/to/" + runtime.getMethodName()));
        assertEquals(0, files.length);
    }

    @Test
    public void testCreateOverwrite() throws Exception {
        Path path = new Path("/path/to/" + runtime.getMethodName());
        assertFalse(fs.exists(path));
        byte[] originData = "original".getBytes(UTF_8);
        try (FSDataOutputStream out = fs.create(path)) {
            out.write(originData);
            out.flush();
        }

        try (FSDataInputStream in = fs.open(path, 1024)) {
            assertEquals(originData.length, in.available());
            byte[] readData = new byte[originData.length];
            assertEquals(originData.length, in.read(readData));
            assertArrayEquals(originData, readData);
        }

        byte[] overwrittenData = "overwritten".getBytes(UTF_8);
        try (FSDataOutputStream out = fs.create(path, true)) {
            out.write(overwrittenData);
            out.flush();
        }

        try (FSDataInputStream in = fs.open(path, 1024)) {
            assertEquals(overwrittenData.length, in.available());
            byte[] readData = new byte[overwrittenData.length];
            assertEquals(overwrittenData.length, in.read(readData));
            assertArrayEquals(overwrittenData, readData);
        }
    }

    @Test
    public void testAppend() throws Exception {
        Path path = new Path("/path/to/" + runtime.getMethodName());
        assertFalse(fs.exists(path));
        byte[] originData = "original".getBytes(UTF_8);
        try (FSDataOutputStream out = fs.create(path)) {
            out.write(originData);
            out.flush();
        }

        try (FSDataInputStream in = fs.open(path, 1024)) {
            assertEquals(originData.length, in.available());
            byte[] readData = new byte[originData.length];
            assertEquals(originData.length, in.read(readData));
            assertArrayEquals(originData, readData);
        }

        byte[] appendData = "append".getBytes(UTF_8);
        try (FSDataOutputStream out = fs.append(path, 1024)) {
            out.write(appendData);
            out.flush();
        }

        try (FSDataInputStream in = fs.open(path, 1024)) {
            assertEquals(originData.length + appendData.length, in.available());
            byte[] readData = new byte[originData.length];
            assertEquals(originData.length, in.read(readData));
            assertArrayEquals(originData, readData);
            readData = new byte[appendData.length];
            assertEquals(appendData.length, in.read(readData));
            assertArrayEquals(appendData, readData);
        }
    }

}
