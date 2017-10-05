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
import org.apache.bookkeeper.client.BKException.BKUnauthorizedAccessException;
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

    @Test
    public void testCreateLedger() throws Exception {

        int ensembleSize = 3;
        int writeQuorumSize = 2;
        int ackQuorumSize = 1;
        long ledgerId = 12342L;
        Map<String, byte[]> customMetadata = new HashMap<>();
        byte[] password = new byte[3];

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

        try {
            result(newCreateLedgerOp()
                .withEnsembleSize(0)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withEnsembleSize(2)
                .withWriteQuorumSize(0)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withEnsembleSize(2)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(0)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(2)
                .withAckQuorumSize(1)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(2)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withPassword(null)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withCustomMetadata(null)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            ClientConfiguration config = new ClientConfiguration();
            config.setEnableDigestTypeAutodetection(true);
            setBookkeeperConfig(config);
            result(newCreateLedgerOp()
                .withDigestType(null)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            ClientConfiguration config = new ClientConfiguration();
            config.setEnableDigestTypeAutodetection(false);
            setBookkeeperConfig(config);
            result(newCreateLedgerOp()
                .withDigestType(null)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        closeBookkeeper();
        try {
            result(newCreateLedgerOp()
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger, client is closed");
        } catch (BKClientClosedException err) {
        }
    }

    @Test
    public void testCreateAdvLedger() throws Exception {

        int ensembleSize = 3;
        int writeQuorumSize = 2;
        int ackQuorumSize = 1;
        long ledgerId = 12342L;
        byte[] password = new byte[3];
        Map<String, byte[]> customMetadata = new HashMap<>();

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

        try {
            result(newCreateLedgerOp()
                .withEnsembleSize(0)
                .withPassword(password)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withEnsembleSize(2)
                .withWriteQuorumSize(0)
                .withPassword(password)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withEnsembleSize(2)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(0)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(2)
                .withAckQuorumSize(1)
                .withPassword(password)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(2)
                .withPassword(password)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withPassword(null)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withCustomMetadata(null)
                .withPassword(password)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            ClientConfiguration config = new ClientConfiguration();
            config.setEnableDigestTypeAutodetection(true);
            setBookkeeperConfig(config);
            result(newCreateLedgerOp()
                .withDigestType(null)
                .withPassword(password)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            ClientConfiguration config = new ClientConfiguration();
            config.setEnableDigestTypeAutodetection(false);
            setBookkeeperConfig(config);
            result(newCreateLedgerOp()
                .withDigestType(null)
                .withPassword(password)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withPassword(password)
                .makeAdv()
                .withLedgerId(-1)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newCreateLedgerOp()
                .withPassword(password)
                .makeAdv()
                .withLedgerId(-2)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        assertEquals(0, result(newCreateLedgerOp()
            .withPassword(password)
            .makeAdv()
            .withLedgerId(0)
            .execute()).getId());

        assertEquals(Integer.MAX_VALUE + 1L, result(newCreateLedgerOp()
            .withPassword(password)
            .makeAdv()
            .withLedgerId(Integer.MAX_VALUE + 1L)
            .execute()).getId());

        closeBookkeeper();
        try {
            result(newCreateLedgerOp()
                .withPassword(password)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger, client is closed");
        } catch (BKClientClosedException err) {
        }

    }

    @Test
    public void testOpenLedger() throws Exception {

        long ledgerId = 12342L;
        byte[] password = new byte[16];
        Map<String, byte[]> customMetadata = new HashMap<>();
        int ensembleSize = 1;
        int writeQuorumSize = 1;
        int ackQuorumSize = 1;

        byte[] entryData = new byte[32];
        LedgerMetadata ledgerMetadata = generateLedgerMetadata(ensembleSize,
            writeQuorumSize, ackQuorumSize, password, customMetadata);
        registerMockLedgerMetadata(ledgerId, ledgerMetadata);

        registerMockEntryForRead(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, password, entryData, -1);
        registerMockEntryForRead(ledgerId, 0, password, entryData, -1);

        try {
            result(newOpenLedgerOp()
                .withPassword(ledgerMetadata.getPassword())
                .execute());
        } catch (BKNoSuchLedgerExistsException err) {
        }

        try {
            result(newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .execute());
            fail("should not be able to read with bad password");
        } catch (BKUnauthorizedAccessException err) {
        }

        try {
            result(newOpenLedgerOp()
                .withPassword(ledgerMetadata.getPassword())
                .withLedgerId(ledgerId)
                .execute());
        } catch (BKDigestMatchException err) {
        }

        try {
            result(newOpenLedgerOp()
                .withPassword(ledgerMetadata.getPassword())
                .withDigestType(DigestType.CRC32)
                .withLedgerId(ledgerId)
                .execute());
        } catch (BKDigestMatchException err) {
        }

        result(newOpenLedgerOp()
            .withPassword(ledgerMetadata.getPassword())
            .withDigestType(DigestType.MAC)
            .withLedgerId(ledgerId)
            .withRecovery(true)
            .execute());

        result(newOpenLedgerOp()
            .withPassword(ledgerMetadata.getPassword())
            .withDigestType(DigestType.MAC)
            .withLedgerId(ledgerId)
            .withRecovery(false)
            .execute());
        closeBookkeeper();
        try {
            result(newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .execute());
            fail("shoud not be able to open a ledger, client is closed");
        } catch (BKClientClosedException err) {
        }

    }

    @Test
    public void testDeleteLedger() throws Exception {
        long ledgerId = 12342L;

        byte[] password = new byte[16];
        Map<String, byte[]> customMetadata = new HashMap<>();
        int ensembleSize = 1;
        int writeQuorumSize = 1;
        int ackQuorumSize = 1;
        LedgerMetadata ledgerMetadata = generateLedgerMetadata(ensembleSize,
            writeQuorumSize, ackQuorumSize, password, customMetadata);
        registerMockLedgerMetadata(ledgerId, ledgerMetadata);

        try {
            result(newDeleteLedgerOp()
                .withLedgerId(-1)
                .execute());
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(newDeleteLedgerOp()
                .withLedgerId(ledgerId + 1)
                .execute());
        } catch (BKNoSuchLedgerExistsException err) {
        }

        result(newDeleteLedgerOp()
            .withLedgerId(ledgerId)
            .execute());

        closeBookkeeper();
        try {
            result(newDeleteLedgerOp()
                .withLedgerId(ledgerId)
                .execute());
            fail("shoud not be able to delete a ledger, client is closed");
        } catch (BKClientClosedException err) {
        }

    }

    protected LedgerMetadata generateLedgerMetadata(int ensembleSize, int writeQuorumSize, int ackQuorumSize, byte[] password, Map<String, byte[]> customMetadata) {
        LedgerMetadata ledgerMetadata = new LedgerMetadata(ensembleSize, writeQuorumSize,
            ackQuorumSize, BookKeeper.DigestType.MAC, password, customMetadata);
        ledgerMetadata.addEnsemble(0, generateNewEnsemble(ensembleSize));
        return ledgerMetadata;
    }

}
