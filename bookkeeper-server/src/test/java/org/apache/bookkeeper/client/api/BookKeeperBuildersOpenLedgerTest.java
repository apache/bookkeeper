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
package org.apache.bookkeeper.client.api;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.MockBookKeeperTestCase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for BookKeeper open ledger operations.
 */
@RunWith(Parameterized.class)
public class BookKeeperBuildersOpenLedgerTest extends MockBookKeeperTestCase {

    private static final int ensembleSize = 3;
    private static final int writeQuorumSize = 2;
    private static final int ackQuorumSize = 1;
    private static final long ledgerId = 12342L;
    private static final Map<String, byte[]> customMetadata = new HashMap<>();
    private static final byte[] password = new byte[3];
    private static final byte[] entryData = new byte[32];

    private boolean withRecovery;

    public BookKeeperBuildersOpenLedgerTest(boolean withRecovery) {
        this.withRecovery = withRecovery;
    }

    @Parameterized.Parameters(name = "withRecovery:({0})")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Test
    public void testOpenLedger() throws Exception {
        LedgerMetadata ledgerMetadata = generateLedgerMetadata(ensembleSize,
            writeQuorumSize, ackQuorumSize, password, customMetadata);
        registerMockLedgerMetadata(ledgerId, ledgerMetadata);

        ledgerMetadata.getAllEnsembles().values().forEach(bookieAddressList -> {
            bookieAddressList.forEach(bookieAddress -> {
                    registerMockEntryForRead(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, bookieAddress, entryData, -1);
                    registerMockEntryForRead(ledgerId, 0, bookieAddress, entryData, -1);
            });
        });

        result(newOpenLedgerOp()
            .withPassword(ledgerMetadata.getPassword())
            .withDigestType(DigestType.CRC32)
            .withLedgerId(ledgerId)
            .withRecovery(withRecovery)
            .execute());
    }

    @Test
    public void testOpenLedgerWithTimeoutEx() throws Exception {
        mockReadEntryTimeout();
        LedgerMetadata ledgerMetadata = generateLedgerMetadata(ensembleSize,
                writeQuorumSize, ackQuorumSize, password, customMetadata);
        registerMockLedgerMetadata(ledgerId, ledgerMetadata);
        ledgerMetadata.getAllEnsembles().values().forEach(bookieAddressList -> {
            bookieAddressList.forEach(bookieAddress -> {
                registerMockEntryForRead(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, bookieAddress, entryData, -1);
                registerMockEntryForRead(ledgerId, 0, bookieAddress, entryData, -1);
            });
        });
        try {
            result(newOpenLedgerOp()
                .withPassword(ledgerMetadata.getPassword())
                .withDigestType(DigestType.CRC32)
                .withLedgerId(ledgerId)
                .withRecovery(withRecovery)
                .execute());
            fail("Expect timeout error");
        } catch (BKException.BKTimeoutException timeoutException) {
            // Expect timeout error.
        }
        // Reset bk client.
        resetBKClient();
    }

    protected LedgerMetadata generateLedgerMetadata(int ensembleSize,
        int writeQuorumSize, int ackQuorumSize, byte[] password,
        Map<String, byte[]> customMetadata) throws BKException.BKNotEnoughBookiesException {
        return LedgerMetadataBuilder.create()
            .withId(12L)
            .withEnsembleSize(ensembleSize)
            .withWriteQuorumSize(writeQuorumSize)
            .withAckQuorumSize(ackQuorumSize)
            .withPassword(password)
            .withDigestType(BookKeeper.DigestType.CRC32.toApiDigestType())
            .withCustomMetadata(customMetadata)
            .withCreationTime(System.currentTimeMillis())
            .newEnsembleEntry(0, generateNewEnsemble(ensembleSize)).build();
    }

    private void mockReadEntryTimeout() {
        // Mock read entry.
        doAnswer(invocation -> {
            long ledgerId = (long) invocation.getArguments()[1];
            long entryId = (long) invocation.getArguments()[2];

            BookkeeperInternalCallbacks.ReadEntryCallback callback =
                    (BookkeeperInternalCallbacks.ReadEntryCallback) invocation.getArguments()[3];
            Object ctx = invocation.getArguments()[4];
            callback.readEntryComplete(BKException.Code.TimeoutException, ledgerId, entryId, null, ctx);
            return null;
        }).when(bookieClient).readEntry(any(BookieId.class),
                anyLong(), anyLong(), any(BookkeeperInternalCallbacks.ReadEntryCallback.class),
                any(), anyInt(), any());
        // Mock read lac.
        doAnswer(invocation -> {
            long ledgerId = (long) invocation.getArguments()[1];
            BookkeeperInternalCallbacks.ReadLacCallback callback =
                    (BookkeeperInternalCallbacks.ReadLacCallback) invocation.getArguments()[2];
            Object ctx = invocation.getArguments()[3];
            callback.readLacComplete(BKException.Code.TimeoutException, ledgerId, null, null, ctx);
            return null;
        }).when(bookieClient).readLac(any(BookieId.class),
                anyLong(), any(BookkeeperInternalCallbacks.ReadLacCallback.class),
                any());
    }

    private void resetBKClient() throws Exception {
        tearDown();
        setup();
    }
}
