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
package org.apache.bookkeeper.statelib.impl.rocksdb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import org.junit.Assert;
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
        RocksUtils.close(null);
        assertTrue(true);
    }

    @Test
    public void testClose() {
        AbstractImmutableNativeReference ref = mock(AbstractImmutableNativeReference.class);
        RocksUtils.close(ref);
        verify(ref, times(1)).close();
    }

    @Test
    public void testIsSstFile() {
        assertTrue(RocksUtils.isSstFile(new File("test.sst")));
    }

    @Test
    public void testIsNotSstFile() {
        assertFalse(RocksUtils.isSstFile(new File("test.sst1")));
    }

    @Test
    public void testGetDestCheckpointsPath() {
        Assert.assertEquals(testPrefix + "/checkpoints", RocksUtils.getDestCheckpointsPath(testPrefix));
    }

    @Test
    public void testGetDestCheckpointPath() {
        Assert.assertEquals(
            testPrefix + "/checkpoints/" + runtime.getMethodName(),
            RocksUtils.getDestCheckpointPath(testPrefix, runtime.getMethodName()));
    }

    @Test
    public void testGetDestCheckpointMetadataPath() {
        Assert.assertEquals(
            testPrefix + "/checkpoints/" + runtime.getMethodName() + "/metadata",
            RocksUtils.getDestCheckpointMetadataPath(testPrefix, runtime.getMethodName()));
    }

    @Test
    public void testGetDestSstsPath() {
        Assert.assertEquals(
            testPrefix + "/ssts",
            RocksUtils.getDestSstsPath(testPrefix));
    }

    @Test
    public void testGetDestSStPathFile() {
        Assert.assertEquals(
            testPrefix + "/ssts/" + runtime.getMethodName(),
            RocksUtils.getDestSstPath(testPrefix, new File("/path/to/" + runtime.getMethodName())));
    }

    @Test
    public void testGetDestSStPath() {
        Assert.assertEquals(
            testPrefix + "/ssts/" + runtime.getMethodName(),
            RocksUtils.getDestSstPath(testPrefix, runtime.getMethodName()));
    }

    @Test
    public void testGetDestTempSstPath() {
        Assert.assertEquals(
            testPrefix + "/checkpoints/" + runtime.getMethodName() + "/" + runtime.getMethodName() + ".sst",
            RocksUtils.getDestTempSstPath(
                testPrefix,
                runtime.getMethodName(),
                new File("/path/to/" + runtime.getMethodName() + ".sst")));
    }

    @Test
    public void testGetDestPath() {
        Assert.assertEquals(
            testPrefix + "/checkpoints/" + runtime.getMethodName() + "/" + runtime.getMethodName() + ".sst",
            RocksUtils.getDestTempSstPath(
                testPrefix,
                runtime.getMethodName(),
                new File("/path/to/" + runtime.getMethodName() + ".sst")));
    }

}
