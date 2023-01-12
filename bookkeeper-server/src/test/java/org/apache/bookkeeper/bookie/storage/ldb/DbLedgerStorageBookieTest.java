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
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for {@link DbLedgerStorageBookieTest}.
 */
public class DbLedgerStorageBookieTest extends BookKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(DbLedgerStorageBookieTest.class);

    public DbLedgerStorageBookieTest() {
        super(1);
        baseConf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        baseConf.setFlushInterval(60000);
        baseConf.setGcWaitTime(60000);

        // Leave it empty to pickup default
        baseConf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, "");

        // Configure explicitly with a int object
        baseConf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 16);
    }

    @Test
    public void testRecoveryEmptyLedger() throws Exception {
        LedgerHandle lh1 = bkc.createLedger(1, 1, DigestType.MAC, new byte[0]);

        // Force ledger close & recovery
        LedgerHandle lh2 = bkc.openLedger(lh1.getId(), DigestType.MAC, new byte[0]);

        assertEquals(0, lh2.getLength());
        assertEquals(-1, lh2.getLastAddConfirmed());
    }

    @Test
    public void testV2ReadWrite() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setUseV2WireProtocol(true);
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        BookKeeper bkc = new BookKeeper(conf);
        LedgerHandle lh1 = bkc.createLedger(1, 1, DigestType.CRC32, new byte[0]);
        lh1.addEntry("Foobar".getBytes());
        lh1.close();

        LedgerHandle lh2 = bkc.openLedger(lh1.getId(), DigestType.CRC32, new byte[0]);
        assertEquals(0, lh2.getLastAddConfirmed());
        assertEquals(new String(lh2.readEntries(0, 0).nextElement().getEntry()),
                     "Foobar");
    }
}
