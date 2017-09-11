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
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Tests for SyncCursor
 */
public class SyncCursorTest {

    @Test
    public void testSimple() {
        SyncCursor cursor = new SyncCursor();
        try {
            cursor.update(-12);
            fail("should not allow negative entries");
        } catch (IllegalArgumentException ok) {
        }

        assertEquals(-1, cursor.getCurrentMinAddSynced());
        for (long i = 0; i < 100; i++) {
            cursor.update(i);
            assertEquals(i, cursor.getCurrentMinAddSynced());
            assertEquals(0, cursor.getNumRanges());
        }
    }

    @Test
    public void testWithGap() {
        SyncCursor cursor = new SyncCursor();
        assertEquals(-1, cursor.getCurrentMinAddSynced());
        cursor.update(0);
        assertEquals(0, cursor.getCurrentMinAddSynced());
        cursor.update(2);
        assertEquals(0, cursor.getCurrentMinAddSynced());
        cursor.update(3);
        assertEquals(0, cursor.getCurrentMinAddSynced());

        cursor.update(1);
        assertEquals(3, cursor.getCurrentMinAddSynced());
        cursor.update(1);
        assertEquals(3, cursor.getCurrentMinAddSynced());
        assertEquals(0, cursor.getNumRanges());
    }

    @Test
    public void testWithSparseRanges() {
        SyncCursor cursor = new SyncCursor();

        cursor.update(0);
        assertEquals(0, cursor.getCurrentMinAddSynced());
        cursor.update(2);
        cursor.update(3);
        cursor.update(4);

        cursor.update(200);
        cursor.update(201);
        cursor.update(202);
        assertEquals(0, cursor.getCurrentMinAddSynced());

        cursor.update(1);
        assertEquals(4, cursor.getCurrentMinAddSynced());

        cursor.update(199);
        assertEquals(4, cursor.getCurrentMinAddSynced());
        cursor.update(198);

        assertEquals(4, cursor.getCurrentMinAddSynced());
        for (int i = 5; i <= 198; i++) {
            cursor.update(i);
        }
        assertEquals(202, cursor.getCurrentMinAddSynced());

        cursor.update(203);
        assertEquals(203, cursor.getCurrentMinAddSynced());
        assertEquals(0, cursor.getNumRanges());

    }

    @Test
    public void testReverse() {
        SyncCursor cursor = new SyncCursor();

        for (int i = 100; i >= 1; i--) {
            cursor.update(i);
            assertEquals(-1, cursor.getCurrentMinAddSynced());
            assertEquals(1, cursor.getNumRanges());
        }
        cursor.update(0);
        assertEquals(100, cursor.getCurrentMinAddSynced());

        // assert all is clean
        assertEquals(0, cursor.getNumRanges());

    }
}
