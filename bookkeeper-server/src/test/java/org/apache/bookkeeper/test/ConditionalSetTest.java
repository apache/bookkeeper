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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests conditional set of the ledger metadata znode.
 */
public class ConditionalSetTest extends BaseTestCase {
    static Logger LOG = LoggerFactory.getLogger(ConditionalSetTest.class);

    byte[] entry;
    DigestType digestType;
    BookKeeper bkcReader;

    public ConditionalSetTest(DigestType digestType) {
        super(3);
        this.digestType = digestType;
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

    @Test(timeout=60000)
    public void testConditionalSet() throws IOException, InterruptedException,
                                    BKException, KeeperException {
        LedgerHandle lhWrite = bkc.createLedger(digestType, new byte[] { 'a',
                'b' });
        long ledgerId = lhWrite.getId();
        LOG.debug("Ledger ID: " + lhWrite.getId());
        for (int i = 0; i < 10; i++) {
            LOG.debug("Adding entry: " + i);
            lhWrite.addEntry(entry);
        }

        /*
         * Open a ledger for reading, which triggers recovery, since the ledger
         * is still open.
         */
        LOG.debug("Instantiating new bookkeeper client.");
        LedgerHandle lhRead = bkcReader.openLedger(lhWrite.getId(), digestType,
                                        new byte[] { 'a', 'b' });
        LOG.debug("Opened the ledger already");

        /*
         * Writer tries to close the ledger, and if should fail.
         */
        try{
            lhWrite.close();
            fail("Should have received an exception when trying to close the ledger.");
        } catch (BKException e) {
            /*
             * Correctly failed to close the ledger
             */
        }
    }
}
