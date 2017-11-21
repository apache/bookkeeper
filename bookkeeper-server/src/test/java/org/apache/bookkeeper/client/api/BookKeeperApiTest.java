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

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.BKException.BKDuplicateEntryIdException;
import org.apache.bookkeeper.client.BKException.BKLedgerFencedException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BKException.BKUnauthorizedAccessException;
import org.apache.bookkeeper.client.MockBookKeeperTestCase;
import org.junit.Test;

/**
 * Unit tests of classes in this package
 */
public class BookKeeperApiTest extends MockBookKeeperTestCase {

    final static byte[] data = "foo".getBytes(UTF_8);
    final static byte[] password = "password".getBytes(UTF_8);

    @Test
    public void testWriteHandle() throws Exception {
        try (WriteHandle writer
            = result(newCreateLedgerOp()
                .withAckQuorumSize(1)
                .withWriteQuorumSize(2)
                .withEnsembleSize(3)
                .withPassword(password)
                .execute())) {

            // test writer is able to write
            result(writer.append(ByteBuffer.wrap(data)));
            assertEquals(0L, writer.getLastAddPushed());
            result(writer.append(Unpooled.wrappedBuffer(data)));
            assertEquals(1L, writer.getLastAddPushed());
            long expectedEntryId = result(writer.append(ByteBuffer.wrap(data)));
            assertEquals(expectedEntryId, writer.getLastAddConfirmed());
            assertEquals(3 * data.length, writer.getLength());
        }
    }

    @Test
    public void testWriteAdvHandle() throws Exception {
        long ledgerId = 12345;
        setNewGeneratedLedgerId(ledgerId);
        try (WriteAdvHandle writer
            = result(newCreateLedgerOp()
                .withAckQuorumSize(1)
                .withWriteQuorumSize(2)
                .withEnsembleSize(3)
                .withPassword(password)
                .makeAdv()
                .execute())) {
            assertEquals(ledgerId, writer.getId());

            // test writer is able to write
            long entryId = 0;
            result(writer.write(entryId++, ByteBuffer.wrap(data)));
            result(writer.write(entryId++, Unpooled.wrappedBuffer(data)));
            long expectedEntryId = result(writer.write(entryId++, ByteBuffer.wrap(data)));
            assertEquals(expectedEntryId, writer.getLastAddConfirmed());
            assertEquals(3 * data.length, writer.getLength());
        }
    }

    @Test
    public void testWriteAdvHandleWithFixedLedgerId() throws Exception {
        setNewGeneratedLedgerId(12345);
        try (WriteAdvHandle writer
            = result(newCreateLedgerOp()
                .withAckQuorumSize(1)
                .withWriteQuorumSize(2)
                .withEnsembleSize(3)
                .withPassword(password)
                .makeAdv()
                .withLedgerId(1234)
                .execute())) {
            assertEquals(1234, writer.getId());

            // test writer is able to write
            long entryId = 0;
            result(writer.write(entryId++, ByteBuffer.wrap(data)));
            result(writer.write(entryId++, Unpooled.wrappedBuffer(data)));
            long expectedEntryId = result(writer.write(entryId++, ByteBuffer.wrap(data)));
            assertEquals(expectedEntryId, writer.getLastAddConfirmed());
            assertEquals(3 * data.length, writer.getLength());
        }
    }

    @Test(expected = BKDuplicateEntryIdException.class)
    public void testWriteAdvHandleBKDuplicateEntryId() throws Exception {
        try (WriteAdvHandle writer
            = result(newCreateLedgerOp()
                .withAckQuorumSize(1)
                .withWriteQuorumSize(2)
                .withEnsembleSize(3)
                .withPassword(password)
                .makeAdv()
                .withLedgerId(1234)
                .execute())) {
            assertEquals(1234, writer.getId());
            long entryId = 0;
            result(writer.write(entryId++, ByteBuffer.wrap(data)));
            assertEquals(data.length, writer.getLength());
            result(writer.write(entryId - 1, ByteBuffer.wrap(data)));
        }
    }

    @Test(expected = BKUnauthorizedAccessException.class)
    public void testOpenLedgerUnauthorized() throws Exception {
        long lId;
        try (WriteHandle writer
            = result(newCreateLedgerOp()
                .withAckQuorumSize(1)
                .withWriteQuorumSize(2)
                .withEnsembleSize(3)
                .withPassword(password)
                .execute())) {
            lId = writer.getId();
            assertEquals(-1L, writer.getLastAddPushed());
        }
        try (ReadHandle ignored
            = result(newOpenLedgerOp()
                .withPassword("bad-password".getBytes(UTF_8))
                .withLedgerId(lId)
                .execute())) {
        }
    }

    @Test(expected = BKDigestMatchException.class)
    public void testOpenLedgerDigestUnmatched() throws Exception {
        long lId;
        try (WriteHandle writer
            = result(newCreateLedgerOp()
                .withAckQuorumSize(1)
                .withWriteQuorumSize(2)
                .withEnsembleSize(3)
                .withDigestType(DigestType.MAC)
                .withPassword(password)
                .execute())) {
            lId = writer.getId();
            assertEquals(-1L, writer.getLastAddPushed());
        }
        try (ReadHandle ignored = result(newOpenLedgerOp()
            .withDigestType(DigestType.CRC32)
            .withPassword(password)
            .withLedgerId(lId)
            .execute())) {
        }
    }

    @Test
    public void testOpenLedgerRead() throws Exception {
        long lId;
        try (WriteHandle writer
            = result(newCreateLedgerOp()
                .withAckQuorumSize(1)
                .withWriteQuorumSize(2)
                .withEnsembleSize(3)
                .withPassword(password)
                .execute())) {
            lId = writer.getId();
            // write data and populate LastAddConfirmed
            result(writer.append(ByteBuffer.wrap(data)));
            result(writer.append(ByteBuffer.wrap(data)));
            result(writer.append(ByteBuffer.wrap(data)));
        }

        try (ReadHandle reader = result(newOpenLedgerOp()
            .withPassword(password)
            .withRecovery(false)
            .withLedgerId(lId)
            .execute())) {
            assertEquals(2, reader.getLastAddConfirmed());
            assertEquals(3 * data.length, reader.getLength());
            assertEquals(2, result(reader.readLastAddConfirmed()).intValue());
            assertEquals(2, result(reader.tryReadLastAddConfirmed()).intValue());
            checkEntries(result(reader.read(0, reader.getLastAddConfirmed())), data);
            checkEntries(result(reader.readUnconfirmed(0, reader.getLastAddConfirmed())), data);

            // test readLastAddConfirmedAndEntry
            LastConfirmedAndEntry lastConfirmedAndEntry =
                result(reader.readLastAddConfirmedAndEntry(0, 999, false));
            assertEquals(2L, lastConfirmedAndEntry.getLastAddConfirmed());
            assertArrayEquals(data, lastConfirmedAndEntry.getEntry().getEntry());
            lastConfirmedAndEntry.close();
        }
    }

    @Test(expected = BKLedgerFencedException.class)
    public void testOpenLedgerWithRecovery() throws Exception {
        long lId;
        try (WriteHandle writer = result(newCreateLedgerOp()
            .withAckQuorumSize(1)
            .withWriteQuorumSize(2)
            .withEnsembleSize(3)
            .withPassword(password)
            .execute())) {
            lId = writer.getId();

            result(writer.append(ByteBuffer.wrap(data)));
            result(writer.append(ByteBuffer.wrap(data)));
            assertEquals(1L, writer.getLastAddPushed());

            // open with fencing
            try (ReadHandle reader = result(newOpenLedgerOp()
                .withPassword(password)
                .withRecovery(true)
                .withLedgerId(lId)
                .execute())) {
                assertEquals(1L, reader.getLastAddConfirmed());
            }

            result(writer.append(ByteBuffer.wrap(data)));

        }
    }

    @Test(expected = BKNoSuchLedgerExistsException.class)
    public void testDeleteLedger() throws Exception {
        long lId;

        try (WriteHandle writer = result(newCreateLedgerOp()
            .withPassword(password)
            .execute())) {
            lId = writer.getId();
            assertEquals(-1L, writer.getLastAddPushed());
        }

        result(newDeleteLedgerOp().withLedgerId(lId).execute());

        result(newOpenLedgerOp()
            .withPassword(password)
            .withLedgerId(lId)
            .execute());
    }

    @Test(expected = BKNoSuchLedgerExistsException.class)
    public void testCannotDeleteLedgerTwice() throws Exception {
        long lId;

        try (WriteHandle writer = result(newCreateLedgerOp()
            .withPassword(password)
            .execute())) {
            lId = writer.getId();
            assertEquals(-1L, writer.getLastAddPushed());
        }
        result(newDeleteLedgerOp().withLedgerId(lId).execute());
        result(newDeleteLedgerOp().withLedgerId(lId).execute());
    }

    private static void checkEntries(Iterable<LedgerEntry> entries, byte[] data)
        throws InterruptedException, BKException {
        for (LedgerEntry le : entries) {
            assertArrayEquals(data, le.getEntry());
        }
    }

}
