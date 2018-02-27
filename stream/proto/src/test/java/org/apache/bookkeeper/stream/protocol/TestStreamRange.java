/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test for {@link RangeId}.
 */
public class TestStreamRange {

    @Test
    public void testConstructor() {
        RangeId sr = RangeId.of(1234L, 5678L);
        assertEquals(1234L, sr.getStreamId());
        assertEquals(5678L, sr.getRangeId());
    }

    @Test
    public void testEqual() {
        RangeId sr1 = RangeId.of(1234L, 5678L);
        RangeId sr2 = RangeId.of(1234L, 5678L);
        RangeId sr3 = RangeId.of(1234L, 5679L);
        RangeId sr4 = RangeId.of(1235L, 5679L);

        assertTrue(sr1.equals(sr2));
        assertFalse(sr1.equals(sr3));
        assertFalse(sr1.equals(sr4));
        assertFalse(sr3.equals(sr4));
    }

    @Test
    public void testToString() {
        RangeId sr = RangeId.of(1234L, 5678L);
        assertEquals("range(1234, 5678)", sr.toString());
    }

}
