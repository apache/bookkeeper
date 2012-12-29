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
package org.apache.bookkeeper.util;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.bookkeeper.util.DiskChecker.DiskErrorException;
import org.apache.bookkeeper.util.DiskChecker.DiskOutOfSpaceException;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to verify {@link DiskChecker}
 * 
 */
public class TestDiskChecker {

    DiskChecker diskChecker;

    @Before
    public void setup() {
        diskChecker = new DiskChecker(0.95f);
    }

    /**
     * Check the disk full
     */
    @Test(expected = DiskOutOfSpaceException.class)
    public void testCheckDiskFull() throws IOException {
        File file = File.createTempFile("DiskCheck", "test");
        long usableSpace = file.getUsableSpace();
        long totalSpace = file.getTotalSpace();
        diskChecker
                .setDiskSpaceThreshold((1f - ((float) usableSpace / (float) totalSpace)) - 0.05f);
        diskChecker.checkDiskFull(file);
    }

    /**
     * Check disk full on non exist file. in this case it should check for
     * parent file
     */
    @Test(expected = DiskOutOfSpaceException.class)
    public void testCheckDiskFullOnNonExistFile() throws IOException {
        File file = File.createTempFile("DiskCheck", "test");
        long usableSpace = file.getUsableSpace();
        long totalSpace = file.getTotalSpace();
        diskChecker
                .setDiskSpaceThreshold((1f - ((float) usableSpace / (float) totalSpace)) - 0.05f);
        assertTrue(file.delete());
        diskChecker.checkDiskFull(file);
    }

    /**
     * Check disk error for file
     */
    @Test(expected = DiskErrorException.class)
    public void testCheckDiskErrorForFile() throws Exception {
        File parent = File.createTempFile("DiskCheck", "test");
        parent.delete();
        parent.mkdir();
        File child = File.createTempFile("DiskCheck", "test", parent);
        diskChecker.checkDir(child);
    }

    /**
     * Check disk error for valid dir.
     */
    @Test(timeout=60000)
    public void testCheckDiskErrorForDir() throws Exception {
        File parent = File.createTempFile("DiskCheck", "test");
        parent.delete();
        parent.mkdir();
        File child = File.createTempFile("DiskCheck", "test", parent);
        child.delete();
        child.mkdir();
        diskChecker.checkDir(child);
    }
}
