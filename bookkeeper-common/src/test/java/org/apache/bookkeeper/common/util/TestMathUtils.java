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

package org.apache.bookkeeper.common.util;

import static org.apache.bookkeeper.common.util.MathUtils.findNextPositivePowerOfTwo;
import static org.apache.bookkeeper.common.util.MathUtils.nowInNano;
import static org.apache.bookkeeper.common.util.MathUtils.signSafeMod;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test of {@link MathUtils}.
 */
public class TestMathUtils {

    @Test
    public void testSignSafeMod() {
        assertEquals(1, signSafeMod(11, 2));
        assertEquals(1, signSafeMod(-11, 2));
        assertEquals(1, signSafeMod(11, -2));
        assertEquals(-3, signSafeMod(-11, -2));
    }

    @Test
    public void testFindNextPositivePowerOfTwo() {
        assertEquals(16384, findNextPositivePowerOfTwo(12345));
    }

    @Test
    public void testNowInNanos() {
        long nowInNanos = nowInNano();
        assertTrue(System.nanoTime() >= nowInNanos);
    }

}
