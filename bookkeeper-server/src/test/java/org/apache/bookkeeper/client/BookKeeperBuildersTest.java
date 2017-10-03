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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BKException.BKClientClosedException;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.BKException.BKIncorrectParameterException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BKException.BKUnauthorizedAccessException;
import org.apache.bookkeeper.client.LedgerCreateOp.CreateBuilderImpl;
import org.apache.bookkeeper.client.LedgerDeleteOp.DeleteBuilderImpl;
import org.apache.bookkeeper.client.LedgerOpenOp.OpenBuilderImpl;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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

        prepareBookieWatcherForNewEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata,
            Arrays.asList(new BookieSocketAddress("localhost", 1234)));

        setNewGeneratedLedgerId(ledgerId);

        AtomicReference<LedgerMetadata> metadataHolder = new AtomicReference<>();

        doAnswer((Answer<Void>) new Answer<Void>() {
            @Override
            @SuppressWarnings("unchecked")
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[2];
                metadataHolder.set((LedgerMetadata) args[1]);
                cb.operationComplete(BKException.Code.OK, null);
                return null;
            }
        }).when(ledgerManager).createLedgerMetadata(eq(ledgerId), any(), any());

        byte[] password = new byte[3];

        WriteHandle writer = new CreateBuilderImpl(bk)
            .withAckQuorumSize(ackQuorumSize)
            .withEnsembleSize(ensembleSize)
            .withWriteQuorumSize(writeQuorumSize)
            .withCustomMetadata(customMetadata)
            .withPassword(password)
            .execute()
            .get();
        assertEquals(ledgerId, writer.getId());
        LedgerMetadata metadata = metadataHolder.get();
        assertEquals(ensembleSize, metadata.getEnsembleSize());
        assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
        assertEquals(writeQuorumSize, metadata.getWriteQuorumSize());
        assertArrayEquals(password, metadata.getPassword());

        try {
            result(new CreateBuilderImpl(bk)
                .withEnsembleSize(0)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
                .withEnsembleSize(2)
                .withWriteQuorumSize(0)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
                .withEnsembleSize(2)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(0)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
                .withEnsembleSize(1)
                .withWriteQuorumSize(2)
                .withAckQuorumSize(1)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(2)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
                .withPassword(null)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
                .withCustomMetadata(null)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            ClientConfiguration config = new ClientConfiguration();
            config.setEnableDigestTypeAutodetection(true);
            when(bk.getConf()).thenReturn(config);
            result(new CreateBuilderImpl(bk)
                .withDigestType(null)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            ClientConfiguration config = new ClientConfiguration();
            config.setEnableDigestTypeAutodetection(false);
            when(bk.getConf()).thenReturn(config);
            result(new CreateBuilderImpl(bk)
                .withDigestType(null)
                .withPassword(password)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        when(bk.isClosed()).thenReturn(true);
        try {
            result(new CreateBuilderImpl(bk)
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

        prepareBookieWatcherForNewEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata,
            Arrays.asList(new BookieSocketAddress("localhost", 1234)));

        setNewGeneratedLedgerId(ledgerId);

        AtomicReference<LedgerMetadata> metadataHolder = new AtomicReference<>();

        doAnswer((Answer<Void>) new Answer<Void>() {
            @Override
            @SuppressWarnings("unchecked")
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[2];
                metadataHolder.set((LedgerMetadata) args[1]);
                cb.operationComplete(BKException.Code.OK, null);
                return null;
            }
        }).when(ledgerManager).createLedgerMetadata(anyLong(), any(), any());

        WriteAdvHandle writer = new CreateBuilderImpl(bk)
            .withAckQuorumSize(ackQuorumSize)
            .withEnsembleSize(ensembleSize)
            .withPassword(password)
            .withWriteQuorumSize(writeQuorumSize)
            .withCustomMetadata(customMetadata)
            .makeAdv()
            .execute()
            .get();
        assertEquals(ledgerId, writer.getId());
        LedgerMetadata metadata = metadataHolder.get();
        assertEquals(ensembleSize, metadata.getEnsembleSize());
        assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
        assertEquals(writeQuorumSize, metadata.getWriteQuorumSize());
        assertArrayEquals(password, metadata.getPassword());

        try {
            result(new CreateBuilderImpl(bk)
                .withEnsembleSize(0)
                .withPassword(password)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
                .withEnsembleSize(2)
                .withWriteQuorumSize(0)
                .withPassword(password)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
                .withEnsembleSize(2)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(0)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
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
            result(new CreateBuilderImpl(bk)
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
            result(new CreateBuilderImpl(bk)
                .withPassword(null)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
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
            when(bk.getConf()).thenReturn(config);
            result(new CreateBuilderImpl(bk)
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
            when(bk.getConf()).thenReturn(config);
            result(new CreateBuilderImpl(bk)
                .withDigestType(null)
                .withPassword(password)
                .makeAdv()
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
                .withPassword(password)
                .makeAdv()
                .withLedgerId(-1)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new CreateBuilderImpl(bk)
                .withPassword(password)
                .makeAdv()
                .withLedgerId(-2)
                .execute());
            fail("shoud not be able to create a ledger with such specs");
        } catch (BKIncorrectParameterException err) {
        }

        assertEquals(0, result(new CreateBuilderImpl(bk)
            .withPassword(password)
            .makeAdv()
            .withLedgerId(0)
            .execute()).getId());

        assertEquals(Integer.MAX_VALUE + 1L, result(new CreateBuilderImpl(bk)
            .withPassword(password)
            .makeAdv()
            .withLedgerId(Integer.MAX_VALUE + 1L)
            .execute()).getId());

        when(bk.isClosed()).thenReturn(true);
        try {
            result(new CreateBuilderImpl(bk)
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

        when(bk.getBookieWatcher().newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata))
            .thenReturn(new ArrayList<>(Arrays.asList(new BookieSocketAddress("localhost", 1234))));

        doAnswer((Answer) (InvocationOnMock invokation) -> {
            Object[] args = invokation.getArguments();

            long _ledgerId = (Long) args[1];
            DigestManager macManager = new MacDigestManager(_ledgerId, password);
            long entryId = (Long) args[3];
            ReadEntryCallback callback = (ReadEntryCallback) args[4];

            scheduler.submit(() -> {
                if (entryId == 0 || entryId == -1) {
                    long fakeLastAddConfirmed = 0;
                    ByteBuf entry = macManager.computeDigestAndPackageForSending(entryId, fakeLastAddConfirmed, 32, Unpooled.wrappedBuffer(new byte[32]));
                    callback.readEntryComplete(BKException.Code.OK, _ledgerId, entryId, Unpooled.copiedBuffer(entry), args[5]);
                    entry.release();
                } else {
                    callback.readEntryComplete(BKException.Code.NoSuchEntryException, _ledgerId, entryId, null, args[5]);
                }
            });
            return null;
        }).when(bookieClient).readEntryAndFenceLedger(any(), anyLong(), any(), anyLong(), any(ReadEntryCallback.class), any());

        doAnswer((Answer) (InvocationOnMock invokation) -> {
            Object[] args = invokation.getArguments();
            long _ledgerId = (Long) args[1];
            long entryId = (Long) args[2];
            DigestManager macManager = new MacDigestManager(_ledgerId, password);

            ReadEntryCallback callback = (ReadEntryCallback) args[3];

            scheduler.submit(() -> {
                if (entryId == 0 || entryId == -1) {
                    long fakeLastAddConfirmed = 0;
                    ByteBuf entry = macManager.computeDigestAndPackageForSending(entryId, fakeLastAddConfirmed, 32, Unpooled.wrappedBuffer(new byte[32]));
                    callback.readEntryComplete(BKException.Code.OK, _ledgerId, entryId, Unpooled.copiedBuffer(entry), args[4]);
                    entry.release();
                } else {
                    callback.readEntryComplete(BKException.Code.NoSuchEntryException, _ledgerId, entryId, null, args[4]);
                }
            });
            return null;
        }).when(bookieClient).readEntry(any(), anyLong(), anyLong(), any(ReadEntryCallback.class), any());

        LedgerMetadata ledgerMetadata = new LedgerMetadata(ensembleSize, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.MAC, password, customMetadata);
        ledgerMetadata.addEnsemble(0, new ArrayList<>(Arrays.asList(new BookieSocketAddress("localhost", 1234))));

        doAnswer((Answer<Void>) new Answer<Void>() {
            @Override
            @SuppressWarnings("unchecked")
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[1];
                cb.operationComplete(BKException.Code.OK, ledgerMetadata);
                return null;
            }
        }).when(ledgerManager).readLedgerMetadata(eq(ledgerId), any());

        doAnswer((Answer<Void>) new Answer<Void>() {
            @Override
            @SuppressWarnings("unchecked")
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[2];
                cb.operationComplete(BKException.Code.OK, null);
                return null;
            }
        }).when(ledgerManager).writeLedgerMetadata(eq(ledgerId), any(), any());

        doAnswer((Answer<Void>) new Answer<Void>() {
            @Override
            @SuppressWarnings("unchecked")
            public Void answer(InvocationOnMock invocation) throws Throwable {
                return null;
            }
        }).when(ledgerManager).registerLedgerMetadataListener(eq(ledgerId), any());

        try {
            result(new OpenBuilderImpl(bk)
                .withPassword(ledgerMetadata.getPassword())
                .execute());
        } catch (BKNoSuchLedgerExistsException err) {
        }

        try {
            result(new OpenBuilderImpl(bk)
                .withLedgerId(ledgerId)
                .execute());
            fail("should not be able to read with bad password");
        } catch (BKUnauthorizedAccessException err) {
        }

        try {
            result(new OpenBuilderImpl(bk)
                .withPassword(ledgerMetadata.getPassword())
                .withLedgerId(ledgerId)
                .execute());
        } catch (BKDigestMatchException err) {
        }

        try {
            result(new OpenBuilderImpl(bk)
                .withPassword(ledgerMetadata.getPassword())
                .withDigestType(DigestType.CRC32)
                .withLedgerId(ledgerId)
                .execute());
        } catch (BKDigestMatchException err) {
        }

        result(new OpenBuilderImpl(bk)
            .withPassword(ledgerMetadata.getPassword())
            .withDigestType(DigestType.MAC)
            .withLedgerId(ledgerId)
            .withRecovery(true)
            .execute());

        result(new OpenBuilderImpl(bk)
            .withPassword(ledgerMetadata.getPassword())
            .withDigestType(DigestType.MAC)
            .withLedgerId(ledgerId)
            .withRecovery(false)
            .execute());
        when(bk.isClosed()).thenReturn(true);
        try {
            result(new OpenBuilderImpl(bk)
                .withLedgerId(ledgerId)
                .execute());
            fail("shoud not be able to open a ledger, client is closed");
        } catch (BKClientClosedException err) {
        }

    }

    @Test
    public void testDeleteLedger() throws Exception {
        long ledgerId = 12342L;

        doAnswer((Answer<Void>) new Answer<Void>() {
            @Override
            @SuppressWarnings("unchecked")
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                long _ledgerId = (Long) args[0];
                BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[2];
                if (_ledgerId == ledgerId) {
                    cb.operationComplete(BKException.Code.OK, null);
                } else {
                    cb.operationComplete(BKException.Code.NoSuchLedgerExistsException, null);
                }
                return null;
            }
        }).when(ledgerManager).removeLedgerMetadata(anyLong(), any(), any());

        try {
            result(new DeleteBuilderImpl(bk)
                .withLedgerId(-1)
                .execute());
        } catch (BKIncorrectParameterException err) {
        }

        try {
            result(new DeleteBuilderImpl(bk)
                .withLedgerId(ledgerId + 1)
                .execute());
        } catch (BKNoSuchLedgerExistsException err) {
        }

        result(new DeleteBuilderImpl(bk)
            .withLedgerId(ledgerId)
            .execute());

        when(bk.isClosed()).thenReturn(true);
        try {
            result(new DeleteBuilderImpl(bk)
                .withLedgerId(ledgerId)
                .execute());
            fail("shoud not be able to delete a ledger, client is closed");
        } catch (BKClientClosedException err) {
        }

    }

}
