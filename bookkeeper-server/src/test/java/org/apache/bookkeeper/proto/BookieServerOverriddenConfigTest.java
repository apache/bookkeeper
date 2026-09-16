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

package org.apache.bookkeeper.proto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Test;

/**
 * Unit test for {@link BookieServer#overriddenConfig(ServerConfiguration)}.
 */
public class BookieServerOverriddenConfigTest {

    @Test
    public void testEmptyConfiguration() {
        assertTrue(BookieServer.overriddenConfig(new ServerConfiguration()).isEmpty());
    }

    @Test
    public void testValuesEqualToDefaultsAreNotReported() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setBookiePort(3181);
        conf.setJournalSyncData(true);
        conf.setGcWaitTime(600000);
        conf.setJournalDirName("/tmp/bk-txn");

        assertTrue(BookieServer.overriddenConfig(conf).isEmpty());
    }

    @Test
    public void testOverriddenValuesAreReported() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setBookiePort(3181);
        conf.setJournalSyncData(false);
        conf.setGcWaitTime(300000);
        conf.setLedgerDirNames(new String[] { "/data/ledgers1", "/data/ledgers2" });

        Map<String, Object> overrides = BookieServer.overriddenConfig(conf);

        assertEquals(Set.of("journalSyncData", "gcWaitTime", "ledgerDirectories"), overrides.keySet());
        assertEquals("false", String.valueOf(overrides.get("journalSyncData")));
        assertEquals("300000", String.valueOf(overrides.get("gcWaitTime")));
        assertEquals("[/data/ledgers1, /data/ledgers2]", String.valueOf(overrides.get("ledgerDirectories")));
    }

    @Test
    public void testOnlyKeysActuallySetAreReported() {
        ServerConfiguration conf = new ServerConfiguration();
        // getJournalDirNames() looks up journalDirectories first and falls back to journalDirectory
        conf.setProperty("journalDirectory", "/data/journal");

        Map<String, Object> overrides = BookieServer.overriddenConfig(conf);

        assertEquals(Set.of("journalDirectory"), overrides.keySet());
        assertEquals("/data/journal", overrides.get("journalDirectory"));
    }

    @Test
    public void testUnknownKeysAreNotReported() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setProperty("brokerServicePort", "6650");

        assertTrue(BookieServer.overriddenConfig(conf).isEmpty());
    }

    @Test
    public void testValueRejectedByGetterIsReported() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setProperty("ledgerManagerFactoryClass", "does.not.Exist");

        Map<String, Object> overrides = BookieServer.overriddenConfig(conf);

        assertEquals("does.not.Exist", overrides.get("ledgerManagerFactoryClass"));
    }
}
