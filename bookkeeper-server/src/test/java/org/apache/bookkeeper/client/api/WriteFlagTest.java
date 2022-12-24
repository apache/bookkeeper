/*
 *
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
 *
 */
package org.apache.bookkeeper.client.api;

import static org.apache.bookkeeper.client.api.WriteFlag.DEFERRED_SYNC;
import static org.junit.Assert.assertEquals;

import java.util.EnumSet;
import org.junit.Test;

/**
 * Unit tests for WriteFlag.
 */
public class WriteFlagTest {

    private static final int NONE = 0;

    @Test
    public void testGetWriteFlagsDeferredSync() {
        assertEquals(EnumSet.of(DEFERRED_SYNC),
                WriteFlag.getWriteFlags(DEFERRED_SYNC.getValue()));
    }

    @Test
    public void testGetWriteFlagsNone() {
        assertEquals(WriteFlag.NONE,
                WriteFlag.getWriteFlags(NONE));
    }

    @Test(expected = NullPointerException.class)
    public void testGetWriteFlagsValueNull() {
        WriteFlag.getWriteFlagsValue(null);
    }

    @Test
    public void testGetWriteFlagsValueEmpty() {
        assertEquals(0, WriteFlag.getWriteFlagsValue(WriteFlag.NONE));
    }

    @Test
    public void testGetWriteFlagsValueDeferredSync() {
        assertEquals(1, WriteFlag.getWriteFlagsValue(EnumSet.of(DEFERRED_SYNC)));
    }
}
