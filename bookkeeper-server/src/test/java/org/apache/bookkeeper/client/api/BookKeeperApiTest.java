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

import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.BKException.BKDuplicateEntryIdException;
import org.apache.bookkeeper.client.BKException.BKLedgerFencedException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BKException.BKUnauthorizedAccessException;
import org.apache.bookkeeper.client.MockBookKeeperTestCase;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.junit.Test;

/**
 * Unit tests of classes in this package
 */
public class BookKeeperApiTest extends MockBookKeeperTestCase {

    @Test
    public void testWriteHandle() throws Exception {
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
        try (WriteHandle writer
            = result(
                newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withWriteQuorumSize(2)
                    .withEnsembleSize(3)
                    .withDigestType(DigestType.MAC)
                    .withCustomMetadata(customMetadata)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .execute());) {

            byte[] data = "foo".getBytes(StandardCharsets.UTF_8);
            writer.append(ByteBuffer.wrap(data)).get();
            writer.append(ByteBuffer.wrap(data)).get();
            writer.append(Unpooled.wrappedBuffer(data)).get();

            writer.append(ByteBuffer.wrap(data)).get();
            long expectedEntryId = writer.append(ByteBuffer.wrap(data)).get();
            assertEquals(expectedEntryId, writer.getLastAddConfirmed());
        }
    }

    @Test
    public void testWriteAdvHandle() throws Exception {
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
        try (WriteAdvHandle writer
            = result(newCreateLedgerOp()
                .withAckQuorumSize(1)
                .withWriteQuorumSize(2)
                .withEnsembleSize(3)
                .withDigestType(DigestType.MAC)
                .withCustomMetadata(customMetadata)
                .withPassword("password".getBytes(StandardCharsets.UTF_8))
                .makeAdv()
                .execute());) {

            long entryId = 0;
            byte[] data = "foo".getBytes(StandardCharsets.UTF_8);
            writer.write(entryId++, ByteBuffer.wrap(data)).get();
            writer.write(entryId++, ByteBuffer.wrap(data)).get();
            writer.write(entryId++, Unpooled.wrappedBuffer(data)).get(1, TimeUnit.MINUTES);

            writer.write(entryId++, ByteBuffer.wrap(data)).get(1, TimeUnit.MINUTES);
            long expectedEntryId = writer.write(entryId++, ByteBuffer.wrap(data)).get(1, TimeUnit.MINUTES);
            assertEquals(expectedEntryId, writer.getLastAddConfirmed());
        }
    }

    @Test
    public void testWriteAdvHandleWithFixedLedgerId() throws Exception {
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
        try (WriteAdvHandle writer
            = result(newCreateLedgerOp()
                .withAckQuorumSize(1)
                .withWriteQuorumSize(2)
                .withEnsembleSize(3)
                .withDigestType(DigestType.MAC)
                .withCustomMetadata(customMetadata)
                .withPassword("password".getBytes(StandardCharsets.UTF_8))
                .makeAdv()
                .withLedgerId(1234)
                .execute());) {

            assertEquals(1234, writer.getId());

            long entryId = 0;
            byte[] data = "foo".getBytes(StandardCharsets.UTF_8);
            writer.write(entryId++, ByteBuffer.wrap(data)).get();
            writer.write(entryId++, ByteBuffer.wrap(data)).get();
            writer.write(entryId++, Unpooled.wrappedBuffer(data)).get();
            try {
                result(writer.write(entryId - 1, Unpooled.wrappedBuffer(data)));
            } catch (BKDuplicateEntryIdException ok) {
            }
            writer.write(entryId++, ByteBuffer.wrap(data)).get();
            long expectedEntryId = writer.write(entryId++, ByteBuffer.wrap(data)).get();
            assertEquals(expectedEntryId, writer.getLastAddConfirmed());
        }
    }

    @Test
    public void testOpenLedger() throws Exception {
        byte[] data = "foo".getBytes(StandardCharsets.UTF_8);
        final byte[] password = "password".getBytes(StandardCharsets.UTF_8);
        long lId;
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
        try (WriteHandle writer
            = result(newCreateLedgerOp()
                .withAckQuorumSize(1)
                .withWriteQuorumSize(2)
                .withEnsembleSize(3)
                .withDigestType(DigestType.MAC)
                .withCustomMetadata(customMetadata)
                .withPassword(password)
                .execute());) {
            lId = writer.getId();

            writer.append(ByteBuffer.wrap(data));
            writer.append(ByteBuffer.wrap(data));
            writer.append(ByteBuffer.wrap(data)).get();
        }

        try (ReadHandle reader
            = result(newOpenLedgerOp()
                .withDigestType(DigestType.MAC)
                .withPassword("bad-password".getBytes(StandardCharsets.UTF_8))
                .withLedgerId(lId)
                .execute())) {
            fail("should not open ledger, bad password");
        } catch (BKUnauthorizedAccessException ok) {
        }

        try (ReadHandle reader = result(newOpenLedgerOp()
            .withDigestType(DigestType.CRC32)
            .withPassword(password)
            .withLedgerId(lId)
            .execute())) {
            fail("should not open ledger, bad digest");
        } catch (BKDigestMatchException ok) {
        }

        try {
            result(newOpenLedgerOp().execute());
            fail("should not open ledger, no id");
        } catch (BKNoSuchLedgerExistsException ok) {
        }

        try {
            result(newOpenLedgerOp().withLedgerId(Long.MAX_VALUE - 1).execute());
            fail("should not open ledger, bad id");
        } catch (BKNoSuchLedgerExistsException ok) {
        }

        registerMockEntryForRead(lId, BookieProtocol.LAST_ADD_CONFIRMED, password, data, -1);
        registerMockEntryForRead(lId, 0, password, data, -1);
        registerMockEntryForRead(lId, 1, password, data, 0);
        registerMockEntryForRead(lId, 2, password, data, 1);

        try (ReadHandle reader = result(newOpenLedgerOp()
            .withDigestType(DigestType.MAC)
            .withPassword(password)
            .withRecovery(false)
            .withLedgerId(lId)
            .execute())) {
            assertEquals(2, reader.getLastAddConfirmed());
            assertEquals(2, reader.readLastAddConfirmed().get().intValue());
            assertEquals(2, reader.tryReadLastAddConfirmed().get().intValue());

            checkEntries(reader.read(0, reader.getLastAddConfirmed()).get(), data);
            checkEntries(reader.readUnconfirmed(0, reader.getLastAddConfirmed()).get(), data);
        }
    }

    @Test
    public void testOpenLedgerWithRecovery() throws Exception {
        long lId;
        byte[] data = "foo".getBytes(StandardCharsets.UTF_8);
        final byte[] password = "password".getBytes(StandardCharsets.UTF_8);
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
        try (WriteHandle writer = result(newCreateLedgerOp()
            .withAckQuorumSize(1)
            .withWriteQuorumSize(2)
            .withEnsembleSize(3)
            .withDigestType(DigestType.MAC)
            .withCustomMetadata(customMetadata)
            .withPassword(password)
            .execute());) {
            lId = writer.getId();

            writer.append(ByteBuffer.wrap(data)).get();
            writer.append(ByteBuffer.wrap(data)).get();

            registerMockEntryForRead(lId, BookieProtocol.LAST_ADD_CONFIRMED, password, data, -1);
            registerMockEntryForRead(lId, 0, password, data, -1);
            registerMockEntryForRead(lId, 1, password, data, 0);

            try (ReadHandle reader = result(newOpenLedgerOp()
                .withDigestType(DigestType.MAC)
                .withPassword(password)
                .withRecovery(true)
                .withLedgerId(lId)
                .execute())) {
            }

            try {
                result(writer.append(ByteBuffer.wrap(data)));
                fail("should not be able to write");
            } catch (BKLedgerFencedException ok) {
            }
        }
        try (ReadHandle reader = result(newOpenLedgerOp()
            .withDigestType(DigestType.MAC)
            .withPassword(password)
            .withRecovery(false)
            .withLedgerId(lId)
            .execute())) {
            assertEquals(1, reader.getLastAddConfirmed());
            assertEquals(1, reader.readLastAddConfirmed().get().intValue());
            checkEntries(reader.read(0, reader.getLastAddConfirmed()).get(), data);
        }
    }

    @Test
    public void testDeleteLedger() throws Exception {
        long lId;

        try (WriteHandle writer = result(newCreateLedgerOp()
            .withPassword("password".getBytes(StandardCharsets.UTF_8))
            .execute());) {
            lId = writer.getId();
        }
        try (ReadHandle opened = result(newOpenLedgerOp()
            .withPassword("password".getBytes(StandardCharsets.UTF_8))
            .withLedgerId(lId)
            .execute());) {
        }

        newDeleteLedgerOp().withLedgerId(lId).execute().get();

        try {
            result(newOpenLedgerOp()
                .withPassword("password".getBytes(StandardCharsets.UTF_8))
                .withLedgerId(lId)
                .execute());
            fail("ledger cannot be open if delete succeeded");
        } catch (BKNoSuchLedgerExistsException ok) {
        }

        try {
            result(newDeleteLedgerOp().withLedgerId(lId).execute());
            fail("ledger cannot be deleted twice");
        } catch (BKNoSuchLedgerExistsException ok) {
        }

    }

    private static void checkEntries(Iterable<LedgerEntry> entries, byte[] data)
        throws InterruptedException, BKException {
        for (LedgerEntry le : entries) {
            assertArrayEquals(le.getEntry(), data);
        }
    }

}
