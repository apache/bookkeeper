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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.client.BKException.BKClientClosedException;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.BKException.BKIncorrectParameterException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.MockBookKeeperTestCase;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;

import org.junit.Test;

/**
 * Unit tests of builders
 */
public class BookKeeperBuildersTest extends MockBookKeeperTestCase {

    private final static int ensembleSize = 3;
    private final static int writeQuorumSize = 2;
    private final static int ackQuorumSize = 1;
    private final static long ledgerId = 12342L;
    private final static Map<String, byte[]> customMetadata = new HashMap<>();
    private final static byte[] password = new byte[3];
    private final static byte[] entryData = new byte[32];

    @Test
    public void testCreateLedger() throws Exception {
        setNewGeneratedLedgerId(ledgerId);
        WriteHandle writer = newCreateLedgerOp()
            .withAckQuorumSize(ackQuorumSize)
            .withEnsembleSize(ensembleSize)
            .withWriteQuorumSize(writeQuorumSize)
            .withCustomMetadata(customMetadata)
            .withPassword(password)
            .execute()
            .get();
        assertEquals(ledgerId, writer.getId());
        LedgerMetadata metadata = getLedgerMetadata(ledgerId);
        assertEquals(ensembleSize, metadata.getEnsembleSize());
        assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
        assertEquals(writeQuorumSize, metadata.getWriteQuorumSize());
        assertArrayEquals(password, metadata.getPassword());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailEnsembleSize0() throws Exception {
        result(newCreateLedgerOp()
            .withEnsembleSize(0)
            .withPassword(password)
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailWriteQuorumSize0() throws Exception {
        result(newCreateLedgerOp()
            .withEnsembleSize(2)
            .withWriteQuorumSize(0)
            .withPassword(password)
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailAckQuorumSize0() throws Exception {
        result(newCreateLedgerOp()
            .withEnsembleSize(2)
            .withWriteQuorumSize(1)
            .withAckQuorumSize(0)
            .withPassword(password)
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailWriteQuorumSizeGreaterThanEnsembleSize() throws Exception {
        result(newCreateLedgerOp()
            .withEnsembleSize(1)
            .withWriteQuorumSize(2)
            .withAckQuorumSize(1)
            .withPassword(password)
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailAckQuorumSizeGreaterThanWriteQuorumSize() throws Exception {
        result(newCreateLedgerOp()
            .withEnsembleSize(1)
            .withWriteQuorumSize(1)
            .withAckQuorumSize(2)
            .withPassword(password)
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailNoPassword() throws Exception {
        result(newCreateLedgerOp()
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailPasswordNull() throws Exception {
        result(newCreateLedgerOp()
            .withPassword(null)
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailCustomMetadataNull() throws Exception {
        result(newCreateLedgerOp()
            .withCustomMetadata(null)
            .withPassword(password)
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailDigestTypeNullAndAutodetectionTrue() throws Exception {
        ClientConfiguration config = new ClientConfiguration();
        config.setEnableDigestTypeAutodetection(true);
        setBookkeeperConfig(config);
        result(newCreateLedgerOp()
            .withDigestType(null)
            .withPassword(password)
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailDigestTypeNullAndAutodetectionFalse() throws Exception {
        ClientConfiguration config = new ClientConfiguration();
        config.setEnableDigestTypeAutodetection(false);
        setBookkeeperConfig(config);
        result(newCreateLedgerOp()
            .withDigestType(null)
            .withPassword(password)
            .execute());
        fail("shoud not be able to create a ledger with such specs");
    }

    @Test(expected = BKClientClosedException.class)
    public void testFailDigestTypeNullAndBookkKeeperClosed() throws Exception {
        closeBookkeeper();
        result(newCreateLedgerOp()
            .withPassword(password)
            .execute());
        fail("shoud not be able to create a ledger, client is closed");
    }

    @Test
    public void testCreateAdvLedger() throws Exception {
        setNewGeneratedLedgerId(ledgerId);
        WriteAdvHandle writer = newCreateLedgerOp()
            .withAckQuorumSize(ackQuorumSize)
            .withEnsembleSize(ensembleSize)
            .withPassword(password)
            .withWriteQuorumSize(writeQuorumSize)
            .withCustomMetadata(customMetadata)
            .makeAdv()
            .execute()
            .get();
        assertEquals(ledgerId, writer.getId());
        LedgerMetadata metadata = getLedgerMetadata(ledgerId);
        assertEquals(ensembleSize, metadata.getEnsembleSize());
        assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
        assertEquals(writeQuorumSize, metadata.getWriteQuorumSize());
        assertArrayEquals(password, metadata.getPassword());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailCreateAdvLedgerBadFixedLedgerIdMinus1() throws Exception {
        result(newCreateLedgerOp()
            .withPassword(password)
            .makeAdv()
            .withLedgerId(-1)
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testFailCreateAdvLedgerBadFixedLedgerIdNegative() throws Exception {
        result(newCreateLedgerOp()
            .withPassword(password)
            .makeAdv()
            .withLedgerId(-2)
            .execute());
        fail("shoud not be able to create a ledger with such specs");
    }

    @Test(expected = BKNoSuchLedgerExistsException.class)
    public void testOpenLedgerNoId() throws Exception {
        result(newOpenLedgerOp().execute());
    }

    @Test(expected = BKNoSuchLedgerExistsException.class)
    public void testOpenLedgerBadId() throws Exception {
        result(newOpenLedgerOp()
            .withPassword(password)
            .withLedgerId(ledgerId)
            .execute());
    }

    @Test(expected = BKClientClosedException.class)
    public void testOpenLedgerClientClosed() throws Exception {
        closeBookkeeper();
        result(newOpenLedgerOp()
            .withPassword(password)
            .withLedgerId(ledgerId)
            .execute());
    }

    @Test
    public void testOpenLedgerNoRecovery() throws Exception {
        LedgerMetadata ledgerMetadata = generateLedgerMetadata(ensembleSize,
            writeQuorumSize, ackQuorumSize, password, customMetadata);
        registerMockLedgerMetadata(ledgerId, ledgerMetadata);

        ledgerMetadata.getEnsembles().values().forEach(bookieAddressList -> {
            bookieAddressList.forEach(bookieAddress -> {
                registerMockEntryForRead(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, bookieAddress, entryData, -1);
                registerMockEntryForRead(ledgerId, 0, bookieAddress, entryData, -1);
            });
        });

        result(newOpenLedgerOp()
            .withPassword(ledgerMetadata.getPassword())
            .withDigestType(DigestType.CRC32)
            .withLedgerId(ledgerId)
            .withRecovery(true)
            .execute());
    }

    @Test
    public void testOpenLedgerRecovery() throws Exception {
        LedgerMetadata ledgerMetadata = generateLedgerMetadata(ensembleSize,
            writeQuorumSize, ackQuorumSize, password, customMetadata);
        registerMockLedgerMetadata(ledgerId, ledgerMetadata);

        ledgerMetadata.getEnsembles().values().forEach(bookieAddressList -> {
            bookieAddressList.forEach(bookieAddress -> {
                registerMockEntryForRead(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, bookieAddress, entryData, -1);
                registerMockEntryForRead(ledgerId, 0, bookieAddress, entryData, -1);
            });
        });
        result(newOpenLedgerOp()
            .withPassword(ledgerMetadata.getPassword())
            .withDigestType(DigestType.CRC32)
            .withLedgerId(ledgerId)
            .withRecovery(false)
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testDeleteLedgerNoLedgerId() throws Exception {
        result(newDeleteLedgerOp()
            .execute());
    }

    @Test(expected = BKIncorrectParameterException.class)
    public void testDeleteLedgerBadLedgerId() throws Exception {
        result(newDeleteLedgerOp()
            .withLedgerId(-1)
            .execute());
    }

    @Test
    public void testDeleteLedger() throws Exception {
        LedgerMetadata ledgerMetadata = generateLedgerMetadata(ensembleSize,
            writeQuorumSize, ackQuorumSize, password, customMetadata);
        registerMockLedgerMetadata(ledgerId, ledgerMetadata);

        result(newDeleteLedgerOp()
            .withLedgerId(ledgerId)
            .execute());
    }

    @Test(expected = BKClientClosedException.class)
    public void testDeleteLedgerBookKeeperClosed() throws Exception {
        closeBookkeeper();
        result(newDeleteLedgerOp()
            .withLedgerId(ledgerId)
            .execute());
    }

    protected LedgerMetadata generateLedgerMetadata(int ensembleSize,
        int writeQuorumSize, int ackQuorumSize, byte[] password,
        Map<String, byte[]> customMetadata) {
        LedgerMetadata ledgerMetadata = new LedgerMetadata(ensembleSize, writeQuorumSize,
            ackQuorumSize, BookKeeper.DigestType.CRC32, password, customMetadata);
        ledgerMetadata.addEnsemble(0, generateNewEnsemble(ensembleSize));
        return ledgerMetadata;
    }

}
