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
package org.apache.bookkeeper.util;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.util.DiskChecker.DiskErrorException;
import org.apache.bookkeeper.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.bookkeeper.util.DiskChecker.DiskWarnThresholdException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to verify {@link DiskChecker}.
 *
 */
public class TestDiskChecker {

    DiskChecker diskChecker;

    final List<File> tempDirs = new ArrayList<File>();
    private static final float THRESHOLD = 0.99f;

    @Before
    public void setup() throws IOException {
        diskChecker = new DiskChecker(THRESHOLD, THRESHOLD);

        // Create at least one file so that target disk will never be empty
        File placeHolderDir = IOUtils.createTempDir("DiskCheck", "test-placeholder");
        tempDirs.add(placeHolderDir);
        File placeHolder = new File(placeHolderDir, "test");
        FileOutputStream placeHolderStream = new FileOutputStream(placeHolder);
        placeHolderStream.write(new byte[100 * 1024]);
        placeHolderStream.close();
    }

    @After
    public void tearDown() throws Exception {
        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    /**
     * Check the disk full.
     */
    @Test(expected = DiskOutOfSpaceException.class)
    public void testCheckDiskFull() throws IOException {
        File file = createTempDir("DiskCheck", "test");
        long usableSpace = file.getUsableSpace();
        long totalSpace = file.getTotalSpace();
        float threshold = minMaxThreshold((1f - ((float) usableSpace / (float) totalSpace)) - (1.0f - THRESHOLD));

        diskChecker.setDiskSpaceThreshold(threshold, threshold);
        diskChecker.checkDiskFull(file);
    }

    @Test(expected = DiskWarnThresholdException.class)
    public void testDiskWarnThresholdException() throws IOException {
        File file = createTempDir("DiskCheck", "test");
        long usableSpace = file.getUsableSpace();
        long totalSpace = file.getTotalSpace();
        float diskSpaceThreshold = minMaxThreshold((1f - ((float) usableSpace / (float) totalSpace)) * 1.5f);
        float diskWarnThreshold = minMaxThreshold((1f - ((float) usableSpace / (float) totalSpace)) * 0.5f);

        diskChecker.setDiskSpaceThreshold(diskSpaceThreshold, diskWarnThreshold);
        diskChecker.checkDiskFull(file);
    }

    /**
     * Check disk full on non exist file. in this case it should check for
     * parent file.
     */
    @Test(expected = DiskOutOfSpaceException.class)
    public void testCheckDiskFullOnNonExistFile() throws IOException {
        File file = createTempDir("DiskCheck", "test");
        long usableSpace = file.getUsableSpace();
        long totalSpace = file.getTotalSpace();
        float threshold = minMaxThreshold((1f - ((float) usableSpace / (float) totalSpace)) * 0.5f);
        diskChecker.setDiskSpaceThreshold(threshold, threshold);
        assertTrue(file.delete());
        diskChecker.checkDiskFull(file);
    }

    /**
     * Check disk error for file.
     */
    @Test(expected = DiskErrorException.class)
    public void testCheckDiskErrorForFile() throws Exception {
        File parent = createTempDir("DiskCheck", "test");
        File child = File.createTempFile("DiskCheck", "test", parent);
        diskChecker.checkDir(child);
    }

    /**
     * Check disk error for valid dir.
     */
    @Test
    public void testCheckDiskErrorForDir() throws Exception {
        File parent = createTempDir("DiskCheck", "test");
        File child = File.createTempFile("DiskCheck", "test", parent);
        child.delete();
        child.mkdir();
        diskChecker.checkDir(child);
    }

    private static float minMaxThreshold(float threshold) {
        final float minThreshold = 0.0000001f;
        final float maxThreshold = 0.999999f;

        threshold = Math.min(threshold, maxThreshold);
        threshold = Math.max(threshold, minThreshold);
        return threshold;
    }
}
