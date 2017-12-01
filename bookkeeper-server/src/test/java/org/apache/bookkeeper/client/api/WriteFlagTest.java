/**
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

import java.util.EnumSet;
import static org.apache.bookkeeper.client.api.WriteFlag.DEFERRED_FORCE;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Utit tests for WriteFlag
 */
public class WriteFlagTest {

    @Test
    public void testGetWriteFlagsDeferredForce() {
        assertEquals(EnumSet.of(DEFERRED_FORCE), WriteFlag.getWriteFlags(1));
    }

    @Test
    public void testGetWriteFlagsNone() {
        assertEquals(EnumSet.noneOf(WriteFlag.class), WriteFlag.getWriteFlags(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetWriteFlagsNegative() {
        WriteFlag.getWriteFlags(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetWriteFlagsUnknown() {
        WriteFlag.getWriteFlags(8);
    }

    @Test(expected = NullPointerException.class)
    public void testGetWriteFlagsValueNull() {
        WriteFlag.getWriteFlagsValue(null);
    }

    @Test
    public void testGetWriteFlagsValueEmpty() {
        assertEquals(0, WriteFlag.getWriteFlagsValue(EnumSet.noneOf(WriteFlag.class)));
    }

    @Test
    public void testGetWriteFlagsValueDeferredForce() {
        assertEquals(1, WriteFlag.getWriteFlagsValue(EnumSet.of(DEFERRED_FORCE)));
    }
}
