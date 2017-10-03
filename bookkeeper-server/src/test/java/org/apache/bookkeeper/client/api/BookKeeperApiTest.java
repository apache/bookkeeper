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

import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BKException.BKUnauthorizedAccessException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Unit tests of builders in this package
 */
public class BookKeeperApiTest extends BookKeeperClusterTestCase {

    public BookKeeperApiTest() {
        super(3);
    }

    @Test
    public void testApi() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(20000);
        try (BookKeeper bkc = BookKeeper.newBuilder(conf).build();) {
            {
                Map<String, byte[]> customMetadata = new HashMap<>();
                customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
                try (WriteHandle writer
                        = FutureUtils.result(
                                bkc.newCreateLedgerOp()
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
            {
                Map<String, byte[]> customMetadata = new HashMap<>();
                customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
                try (WriteAdvHandle writer
                        = FutureUtils.result(bkc.newCreateLedgerOp()
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
        }
        {
            Map<String, byte[]> customMetadata = new HashMap<>();
            customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
            try (WriteAdvHandle writer
                    = FutureUtils.result(bkc.newCreateLedgerOp()
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
                writer.write(entryId++, ByteBuffer.wrap(data)).get();
                long expectedEntryId = writer.write(entryId++, ByteBuffer.wrap(data)).get();
                assertEquals(expectedEntryId, writer.getLastAddConfirmed());
            }
        }
    }

    @Test
    public void testOpenLedger() throws Exception {
        byte[] data = "foo".getBytes(StandardCharsets.UTF_8);

        ClientConfiguration conf = new ClientConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(20000);

        try (BookKeeper bkc = BookKeeper.newBuilder(conf).build();) {
            {
                long lId;
                Map<String, byte[]> customMetadata = new HashMap<>();
                customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
                try (WriteHandle writer
                        = FutureUtils.result(bkc.newCreateLedgerOp()
                                .withAckQuorumSize(1)
                                .withWriteQuorumSize(2)
                                .withEnsembleSize(3)
                                .withDigestType(DigestType.MAC)
                                .withCustomMetadata(customMetadata)
                                .withPassword("password".getBytes(StandardCharsets.UTF_8))
                                .execute());) {
                    lId = writer.getId();

                    writer.append(ByteBuffer.wrap(data));
                    writer.append(ByteBuffer.wrap(data));
                    writer.append(ByteBuffer.wrap(data)).get();
                }
                try (ReadHandle reader
                        = FutureUtils.result(bkc.newOpenLedgerOp()
                                .withDigestType(DigestType.MAC)
                                .withPassword("bad-password".getBytes(StandardCharsets.UTF_8))
                                .withLedgerId(lId)
                                .execute())) {
                    fail("should not open ledger, bad password");
                }
                catch (BKUnauthorizedAccessException ok) {
                }

                try (ReadHandle reader = FutureUtils.result(bkc.newOpenLedgerOp()
                        .withDigestType(DigestType.CRC32)
                        .withPassword("password".getBytes(StandardCharsets.UTF_8))
                        .withLedgerId(lId)
                        .execute())) {
                    fail("should not open ledger, bad digest");
                }
                catch (BKDigestMatchException ok) {
                }

                try {
                    FutureUtils.result(bkc.newOpenLedgerOp().execute());
                    fail("should not open ledger, no id");
                }
                catch (BKNoSuchLedgerExistsException ok) {
                }

                try {
                    FutureUtils.result(bkc.newOpenLedgerOp().withLedgerId(Long.MAX_VALUE - 1).execute());
                    fail("should not open ledger, bad id");
                }
                catch (BKNoSuchLedgerExistsException ok) {
                }

                try (ReadHandle reader = FutureUtils.result(bkc.newOpenLedgerOp()
                        .withDigestType(DigestType.MAC)
                        .withPassword("password".getBytes(StandardCharsets.UTF_8))
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
            {
                long lId;
                Map<String, byte[]> customMetadata = new HashMap<>();
                customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
                try (WriteHandle writer = FutureUtils.result(bkc.newCreateLedgerOp()
                        .withAckQuorumSize(1)
                        .withWriteQuorumSize(2)
                        .withEnsembleSize(3)
                        .withDigestType(DigestType.MAC)
                        .withCustomMetadata(customMetadata)
                        .withPassword("password".getBytes(StandardCharsets.UTF_8))
                        .execute());) {
                    lId = writer.getId();

                    writer.append(ByteBuffer.wrap(data)).get();
                    writer.append(ByteBuffer.wrap(data)).get();

                    try (ReadHandle reader = FutureUtils.result(bkc.newOpenLedgerOp()
                            .withDigestType(DigestType.MAC)
                            .withPassword("password".getBytes(StandardCharsets.UTF_8))
                            .withRecovery(true)
                            .withLedgerId(lId)
                            .execute())) {
                    }

                    try {
                        FutureUtils.result(writer.append(ByteBuffer.wrap(data)));
                        fail("should not be able to write");
                    }
                    catch (BKException.BKLedgerFencedException ok) {
                    }
                }
                try (ReadHandle reader = FutureUtils.result(bkc.newOpenLedgerOp()
                        .withDigestType(DigestType.MAC)
                        .withPassword("password".getBytes(StandardCharsets.UTF_8))
                        .withRecovery(false)
                        .withLedgerId(lId)
                        .execute())) {
                    assertEquals(1, reader.getLastAddConfirmed());
                    assertEquals(1, reader.readLastAddConfirmed().get().intValue());
                    checkEntries(reader.read(0, reader.getLastAddConfirmed()).get(), data);
                }
            }
        }
    }

    @Test
    public void testDeleteLedger() throws Exception {
        byte[] data = "foo".getBytes(StandardCharsets.UTF_8);

        ClientConfiguration conf = new ClientConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(20000);
        long lId;
        try (BookKeeper bkc = BookKeeper.newBuilder(conf).build();) {
            Map<String, byte[]> customMetadata = new HashMap<>();
            customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
            try (WriteHandle writer = FutureUtils.result(bkc.newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withWriteQuorumSize(2)
                    .withEnsembleSize(3)
                    .withCustomMetadata(customMetadata)
                    .withDigestType(DigestType.MAC)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .execute());) {
                lId = writer.getId();
            }

            try (ReadHandle opened = FutureUtils.result(bkc.newOpenLedgerOp()
                    .withDigestType(DigestType.MAC)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .withLedgerId(lId)
                    .execute());) {
            }

            bkc.newDeleteLedgerOp().withLedgerId(lId).execute().get();

            try {
                FutureUtils.result(bkc.newOpenLedgerOp()
                        .withDigestType(DigestType.MAC)
                        .withPassword("password".getBytes(StandardCharsets.UTF_8))
                        .withLedgerId(lId)
                        .execute());
                fail("ledger cannot be open if delete succeeded");
            }
            catch (BKNoSuchLedgerExistsException ok) {
            }

            try (WriteHandle writer = FutureUtils.result(bkc.newCreateLedgerOp()
                    .execute());) {
                lId = writer.getId();
            }
            try (ReadHandle opened = FutureUtils.result(bkc.newOpenLedgerOp()
                    .withLedgerId(lId)
                    .execute());) {
            }

            bkc.newDeleteLedgerOp().withLedgerId(lId).execute().get();

            try {
                FutureUtils.result(bkc.newOpenLedgerOp()
                        .withLedgerId(lId)
                        .execute());
                fail("ledger cannot be open if delete succeeded");
            }
            catch (BKNoSuchLedgerExistsException ok) {
            }

            try {
                FutureUtils.result(bkc.newOpenLedgerOp()
                        .withLedgerId(lId)
                        .execute());
                fail("ledger cannot be open if delete succeeded");
            }
            catch (BKNoSuchLedgerExistsException ok) {
            }

            try (WriteHandle writer = FutureUtils.result(bkc.newCreateLedgerOp()
                    .execute());) {
                lId = writer.getId();
            }
            try (ReadHandle opened = FutureUtils.result(bkc.newOpenLedgerOp()
                    .withLedgerId(lId)
                    .execute());) {
            }

            bkc.newDeleteLedgerOp().withLedgerId(lId).execute().get();

            try {
                FutureUtils.result(bkc.newOpenLedgerOp()
                        .withLedgerId(lId)
                        .execute());
                fail("ledger cannot be open if delete succeeded");
            }
            catch (BKNoSuchLedgerExistsException ok) {
            }

            try {
                FutureUtils.result(bkc.newDeleteLedgerOp().withLedgerId(lId).execute());
                fail("ledger cannot be deleted twice");;
            }
            catch (BKNoSuchLedgerExistsException ok) {
            }

            try {
                FutureUtils.result(bkc.newDeleteLedgerOp().execute());
                fail("ledger cannot be deleted, no id");
            }
            catch (BKNoSuchLedgerExistsException ok) {
            }
        }
    }

    private static void checkEntries(Iterable<org.apache.bookkeeper.client.api.LedgerEntry> entries, byte[] data)
            throws InterruptedException, BKException {
        for (org.apache.bookkeeper.client.api.LedgerEntry le : entries) {
            assertArrayEquals(le.getEntry(), data);
        }
    }

}
