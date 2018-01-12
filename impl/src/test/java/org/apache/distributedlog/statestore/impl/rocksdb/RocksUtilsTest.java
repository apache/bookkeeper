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
package org.apache.distributedlog.statestore.impl.rocksdb;

import static org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils.close;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils.getDestCheckpointMetadataPath;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils.getDestCheckpointPath;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils.getDestCheckpointsPath;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils.getDestSstPath;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils.getDestSstsPath;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils.getDestTempSstPath;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils.isSstFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.rocksdb.AbstractImmutableNativeReference;

/**
 * Unit test of {@link RocksUtils}.
 */
public class RocksUtilsTest {

    @Rule
    public final TestName runtime = new TestName();

    private String testPrefix;

    @Before
    public void setUp() {
        this.testPrefix = "path/to/" + runtime.getMethodName();
    }

    @Test
    public void testCloseNull() {
        close(null);
        assertTrue(true);
    }

    @Test
    public void testClose() {
        AbstractImmutableNativeReference ref = mock(AbstractImmutableNativeReference.class);
        close(ref);
        verify(ref, times(1)).close();
    }

    @Test
    public void testIsSstFile() {
        assertTrue(isSstFile(new File("test.sst")));
    }

    @Test
    public void testIsNotSstFile() {
        assertFalse(isSstFile(new File("test.sst1")));
    }

    @Test
    public void testGetDestCheckpointsPath() {
        assertEquals(testPrefix + "/checkpoints", getDestCheckpointsPath(testPrefix));
    }

    @Test
    public void testGetDestCheckpointPath() {
        assertEquals(
            testPrefix + "/checkpoints/" + runtime.getMethodName(),
            getDestCheckpointPath(testPrefix, runtime.getMethodName()));
    }

    @Test
    public void testGetDestCheckpointMetadataPath() {
        assertEquals(
            testPrefix + "/checkpoints/" + runtime.getMethodName() + "/metadata",
            getDestCheckpointMetadataPath(testPrefix, runtime.getMethodName()));
    }

    @Test
    public void testGetDestSstsPath() {
        assertEquals(
            testPrefix + "/ssts",
            getDestSstsPath(testPrefix));
    }

    @Test
    public void testGetDestSStPathFile() {
        assertEquals(
            testPrefix + "/ssts/" + runtime.getMethodName(),
            getDestSstPath(testPrefix, new File("/path/to/" + runtime.getMethodName())));
    }

    @Test
    public void testGetDestSStPath() {
        assertEquals(
            testPrefix + "/ssts/" + runtime.getMethodName(),
            getDestSstPath(testPrefix, runtime.getMethodName()));
    }

    @Test
    public void testGetDestTempSstPath() {
        assertEquals(
            testPrefix + "/checkpoints/" + runtime.getMethodName() + "/" + runtime.getMethodName() + ".sst",
            getDestTempSstPath(
                testPrefix,
                runtime.getMethodName(),
                new File("/path/to/" + runtime.getMethodName() + ".sst")));
    }

    @Test
    public void testGetDestPath() {
        assertEquals(
            testPrefix + "/checkpoints/" + runtime.getMethodName() + "/" + runtime.getMethodName() + ".sst",
            getDestTempSstPath(
                testPrefix,
                runtime.getMethodName(),
                new File("/path/to/" + runtime.getMethodName() + ".sst")));
    }

}
