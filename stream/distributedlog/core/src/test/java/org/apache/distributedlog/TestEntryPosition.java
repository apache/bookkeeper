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
package org.apache.distributedlog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


/**
 * Test Case for {@link EntryPosition}.
 */
public class TestEntryPosition {

    private void checkPosition(EntryPosition position,
                               long lssn,
                               long entryId) {
        assertEquals(position.getLogSegmentSequenceNumber(), lssn);
        assertEquals(position.getEntryId(), entryId);
    }

    @Test
    public void testAdvance() {
        EntryPosition position = new EntryPosition(9L, 99L);

        checkPosition(position, 9L, 99L);

        // advance (8L, 100L) takes no effect
        assertFalse(position.advance(8L, 100L));
        checkPosition(position, 9L, 99L);
        // advance (9L, 98L) takes no effect
        assertFalse(position.advance(9L, 98L));
        checkPosition(position, 9L, 99L);
        // advance (9L, 99L) takes no effect
        assertFalse(position.advance(9L, 99L));
        checkPosition(position, 9L, 99L);
        // advance (9L, 100L) takes effects
        assertTrue(position.advance(9L, 100L));
        checkPosition(position, 9L, 100L);
        // advance (10L, 0L) takes effects
        assertTrue(position.advance(10L, 0L));
        checkPosition(position, 10L, 0L);
    }

}
