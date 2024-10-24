/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Enumeration;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.junit.jupiter.api.Test;

/**
 * Test the mocked BookKeeper client.
 */
public class MockBookKeeperTest {

    @Test
    void mockedBookKeeper() throws Exception {
        BookKeeper bkc = new MockBookKeeper(null);

        LedgerHandle lh = bkc.createLedger(DigestType.CRC32, new byte[0]);

        assertEquals(0, lh.addEntry("entry-0".getBytes()));
        assertEquals(1, lh.addEntry("entry-1".getBytes()));

        assertEquals(1, lh.getLastAddConfirmed());

        Enumeration<LedgerEntry> entries = lh.readEntries(0, 1);
        assertTrue(entries.hasMoreElements());
        assertEquals("entry-0", new String(entries.nextElement().getEntry()));
        assertTrue(entries.hasMoreElements());
        assertEquals("entry-1", new String(entries.nextElement().getEntry()));
        assertFalse(entries.hasMoreElements());

        lh.close();
        bkc.close();
    }
}
