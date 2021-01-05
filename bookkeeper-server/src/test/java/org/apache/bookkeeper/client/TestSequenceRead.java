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
package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test reading an entry from replicas in sequence way.
 */
public class TestSequenceRead extends BookKeeperClusterTestCase {

    private static final Logger logger = LoggerFactory.getLogger(TestSequenceRead.class);

    public TestSequenceRead() {
        super(5);
    }

    private long createLedgerWithDuplicatedBookies() throws Exception {
        long ledgerId = 12345L;
        // introduce duplicated bookies in an ensemble.
        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create()
            .withId(ledgerId).withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
            .newEnsembleEntry(0L, Lists.newArrayList(getBookie(0), getBookie(0), getBookie(0)));
        ClientUtil.setupLedger(bkc.getLedgerManager(), ledgerId, builder);

        logger.info("Update ledger metadata with duplicated bookies for ledger {}.", ledgerId);
        return ledgerId;
    }

    @Test
    public void testSequenceReadOnDuplicatedBookies() throws Exception {
        final long ledgerId = createLedgerWithDuplicatedBookies();

        // should be able to open the ledger even it has duplicated bookies
        final LedgerHandle readLh = bkc.openLedger(
                ledgerId, DigestType.fromApiDigestType(ClientUtil.DIGEST_TYPE), ClientUtil.PASSWD);
        assertEquals(LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
    }

}
