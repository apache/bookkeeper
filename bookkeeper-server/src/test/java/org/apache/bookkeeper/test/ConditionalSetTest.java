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
package org.apache.bookkeeper.test;

import java.io.IOException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests conditional set of the ledger metadata znode.
 */
public class ConditionalSetTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(ConditionalSetTest.class);

    byte[] entry;
    private final DigestType digestType;
    BookKeeper bkcReader;

    public ConditionalSetTest() {
        super(3);
        this.digestType = DigestType.CRC32;
    }

    @Override
    @Before
    public void setUp() throws IOException, Exception {
        super.setUp();
        entry = new byte[10]; // initialize the entries list
        this.bkcReader = new BookKeeperTestClient(baseClientConf);
    }

    /**
     * Opens a ledger before the ledger writer, which triggers ledger recovery.
     * When the ledger writer tries to close the ledger, the close operation
     * should fail.
     *
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws BKException
     * @throws KeeperException
     */

    @Test
    public void testConditionalSet() throws IOException, InterruptedException,
                                    BKException, KeeperException {
        LedgerHandle lhWrite = bkc.createLedger(digestType, new byte[] { 'a',
                'b' });
        long ledgerId = lhWrite.getId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ledger ID: {}", ledgerId);
        }
        for (int i = 0; i < 10; i++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding entry: " + i);
            }
            lhWrite.addEntry(entry);
        }

        /*
         * Open a ledger for reading, which triggers recovery, since the ledger
         * is still open.
         */
        if (LOG.isDebugEnabled()) {
            LOG.debug("Instantiating new bookkeeper client.");
        }
        LedgerHandle lhRead = bkcReader.openLedger(lhWrite.getId(), digestType,
                                        new byte[] { 'a', 'b' });
        if (LOG.isDebugEnabled()) {
            LOG.debug("Opened the ledger already");
        }

        /*
         * Writer tries to close the ledger, and it should succeed as recovery closed
         * the ledger already, but with the correct LAC and length
         */
        lhWrite.close();
    }
}
