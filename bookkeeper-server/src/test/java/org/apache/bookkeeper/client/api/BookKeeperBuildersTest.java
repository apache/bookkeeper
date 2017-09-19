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
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BKException.BKUnauthorizedAccessException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.MacDigestManager;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Unit tests of builders in this package
 */
public class BookKeeperBuildersTest extends BookKeeperClusterTestCase {

    public BookKeeperBuildersTest() {
        super(4);
    }

    private static LedgerMetadata getMetadata(Object object) throws Exception {
        Method method = LedgerHandle.class.getDeclaredMethod("getLedgerMetadata");
        method.setAccessible(true);
        return (LedgerMetadata) method.invoke(object);
    }

    @Test
    public void testCreateLedgerDefaults() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(20000);
        try (org.apache.bookkeeper.client.api.BookKeeper bkc
            = org.apache.bookkeeper.client.BookKeeper
                .forConfig(conf)
                .build();) {
                try (WriteHandler writer = bkc.createLedgerOp()
                    .create();) {
                    LedgerMetadata metadata = getMetadata(writer);
                    assertEquals(3, metadata.getEnsembleSize());
                    assertEquals(3, metadata.getWriteQuorumSize());
                    assertEquals(3, metadata.getAckQuorumSize());
                    assertEquals(org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32, metadata.getDigestType());
                    assertArrayEquals(MacDigestManager.genDigest("ledger", new byte[0]), ((LedgerHandle) writer).getLedgerKey());
                    assertTrue(metadata.getCustomMetadata().isEmpty());
                }
            }
    }

    @Test
    public void testCreateLedger() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(20000);
        try (org.apache.bookkeeper.client.api.BookKeeper bkc
            = org.apache.bookkeeper.client.BookKeeper
                .forConfig(conf)
                .build();) {
                Map<String, byte[]> customMetadata = new HashMap<>();
                customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
                try (WriteHandler writer = bkc.createLedgerOp()
                    .withAckQuorumSize(1)
                    .withWriteQuorumSize(2)
                    .withEnsembleSize(3)
                    .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                    .withCustomMetadata(customMetadata)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .create();) {

                    LedgerMetadata metadata = getMetadata(writer);
                    assertEquals(3, metadata.getEnsembleSize());
                    assertEquals(2, metadata.getWriteQuorumSize());
                    assertEquals(1, metadata.getAckQuorumSize());
                    assertEquals(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC, metadata.getDigestType());
                    assertArrayEquals("test".getBytes(StandardCharsets.UTF_8),
                        metadata.getCustomMetadata().get("test"));
                    assertArrayEquals(MacDigestManager.genDigest("ledger", "password".getBytes(StandardCharsets.UTF_8)),
                        ((LedgerHandle) writer).getLedgerKey());

                    byte[] data = "foo".getBytes(StandardCharsets.UTF_8);
                    writer.append(data).get();
                    writer.append(data, 0, data.length).get();
                    writer.append(Unpooled.wrappedBuffer(data)).get();

                    writer.append(data).get();
                    long expectedEntryId = writer.append(data).get();
                    assertEquals(expectedEntryId, writer.getLastAddConfirmed());
                }
            }
    }

    @Test
    public void testCreateAdvLedger() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(20000);
        try (org.apache.bookkeeper.client.api.BookKeeper bkc
            = org.apache.bookkeeper.client.BookKeeper
                .forConfig(conf)
                .build();) {
                Map<String, byte[]> customMetadata = new HashMap<>();
                customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
                try (WriteAdvHandler writer = bkc.createLedgerOp()
                    .withAckQuorumSize(1)
                    .withWriteQuorumSize(2)
                    .withEnsembleSize(3)
                    .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                    .withCustomMetadata(customMetadata)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .makeAdv()
                    .create();) {

                    LedgerMetadata metadata = getMetadata(writer);
                    assertEquals(3, metadata.getEnsembleSize());
                    assertEquals(2, metadata.getWriteQuorumSize());
                    assertEquals(1, metadata.getAckQuorumSize());
                    assertEquals(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC, metadata.getDigestType());
                    assertArrayEquals("test".getBytes(StandardCharsets.UTF_8),
                        metadata.getCustomMetadata().get("test"));
                    assertArrayEquals(MacDigestManager.genDigest("ledger", "password".getBytes(StandardCharsets.UTF_8)),
                        ((LedgerHandle) writer).getLedgerKey());

                    long entryId = 0;
                    byte[] data = "foo".getBytes(StandardCharsets.UTF_8);
                    writer.write(entryId++, data).get();
                    writer.write(entryId++, data, 0, data.length).get();
                    writer.write(entryId++, Unpooled.wrappedBuffer(data)).get(1, TimeUnit.MINUTES);

                    writer.write(entryId++, data).get(1, TimeUnit.MINUTES);
                    long expectedEntryId = writer.write(entryId++, data).get(1, TimeUnit.MINUTES);
                    assertEquals(expectedEntryId, writer.getLastAddConfirmed());
                }
            }
    }

    @Test
    public void testCreateAdvLedgerWithFixedId() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(20000);
        try (org.apache.bookkeeper.client.api.BookKeeper bkc
            = org.apache.bookkeeper.client.BookKeeper
                .forConfig(conf)
                .build();) {
                Map<String, byte[]> customMetadata = new HashMap<>();
                customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
                try (WriteAdvHandler writer = bkc.createLedgerOp()
                    .withAckQuorumSize(1)
                    .withWriteQuorumSize(2)
                    .withEnsembleSize(3)
                    .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                    .withCustomMetadata(customMetadata)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .makeAdv()
                    .withLedgerId(1234)
                    .create();) {

                    LedgerMetadata metadata = getMetadata(writer);
                    assertEquals(3, metadata.getEnsembleSize());
                    assertEquals(2, metadata.getWriteQuorumSize());
                    assertEquals(1, metadata.getAckQuorumSize());
                    assertEquals(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC, metadata.getDigestType());
                    assertArrayEquals("test".getBytes(StandardCharsets.UTF_8),
                        metadata.getCustomMetadata().get("test"));
                    assertEquals(1234, writer.getId());

                    long entryId = 0;
                    byte[] data = "foo".getBytes(StandardCharsets.UTF_8);
                    writer.write(entryId++, data).get();
                    writer.write(entryId++, data, 0, data.length).get();
                    writer.write(entryId++, Unpooled.wrappedBuffer(data)).get();
                    writer.write(entryId++, data).get();
                    long expectedEntryId = writer.write(entryId++, data).get();
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
        long lId;
        try (org.apache.bookkeeper.client.api.BookKeeper bkc
            = org.apache.bookkeeper.client.BookKeeper
                .forConfig(conf)
                .build();) {
                Map<String, byte[]> customMetadata = new HashMap<>();
                customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
                try (WriteHandler writer = bkc.createLedgerOp()
                    .withAckQuorumSize(1)
                    .withWriteQuorumSize(2)
                    .withEnsembleSize(3)
                    .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                    .withCustomMetadata(customMetadata)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .create();) {
                    lId = writer.getId();

                    writer.append(data);
                    writer.append(data);
                    writer.append(data).get();
                }
                try (ReadHandler reader = bkc.openLedgerOp()
                    .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                    .withPassword("bad-password".getBytes(StandardCharsets.UTF_8))
                    .open(lId)) {
                    fail("should not open ledger, bad password");
                } catch (BKUnauthorizedAccessException ok) {
                }

                try (ReadHandler reader = bkc.openLedgerOp()
                    .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .open(lId)) {
                    fail("should not open ledger, bad digest");
                } catch (BKDigestMatchException ok) {
                }

                try (ReadHandler reader = bkc.openLedgerOp()
                    .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .withRecovery(false)
                    .open(lId)) {
                    assertEquals(2, reader.getLastAddConfirmed());
                    assertEquals(2, reader.readLastConfirmedEntryId().get().intValue());
                    assertEquals(2, reader.tryReadLastConfirmedEntryId().get().intValue());

                    checkEntries(reader.read(0, reader.getLastAddConfirmed()).get(), data);
                    checkEntries(reader.readUnconfirmed(0, reader.getLastAddConfirmed()).get(), data);
                }
            }
    }

    @Test
    public void testOpenLedgerWithFencing() throws Exception {
        byte[] data = "foo".getBytes(StandardCharsets.UTF_8);

        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(20000);
        long lId;
        try (org.apache.bookkeeper.client.api.BookKeeper bkc
            = org.apache.bookkeeper.client.BookKeeper
                .forConfig(conf)
                .build();) {
                Map<String, byte[]> customMetadata = new HashMap<>();
                customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
                try (WriteHandler writer = bkc.createLedgerOp()
                    .withAckQuorumSize(1)
                    .withWriteQuorumSize(2)
                    .withEnsembleSize(3)
                    .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                    .withCustomMetadata(customMetadata)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .create();) {
                    lId = writer.getId();

                    writer.append(data).get();
                    writer.append(data).get();

                    try (ReadHandler reader = bkc.openLedgerOp()
                        .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                        .withPassword("password".getBytes(StandardCharsets.UTF_8))
                        .withRecovery(true)
                        .open(lId)) {
                    }

                    try {
                        FutureUtils.result(writer.append(data));
                        fail("should not be able to write");
                    } catch (BKException.BKLedgerFencedException ok) {
                    }
                }
                try (ReadHandler reader = bkc.openLedgerOp()
                    .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .withRecovery(false)
                    .open(lId)) {
                    assertEquals(1, reader.getLastAddConfirmed());
                    assertEquals(1, reader.readLastConfirmedEntryId().get().intValue());
                    checkEntries(reader.read(0, reader.getLastAddConfirmed()).get(), data);
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
        try (org.apache.bookkeeper.client.api.BookKeeper bkc
            = org.apache.bookkeeper.client.BookKeeper
                .forConfig(conf)
                .build();) {
                Map<String, byte[]> customMetadata = new HashMap<>();
                customMetadata.put("test", "test".getBytes(StandardCharsets.UTF_8));
                try (WriteHandler writer = bkc.createLedgerOp()
                    .withAckQuorumSize(1)
                    .withWriteQuorumSize(2)
                    .withEnsembleSize(3)
                    .withCustomMetadata(customMetadata)
                    .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .create();) {
                    lId = writer.getId();
                }

                try (ReadHandler opened = bkc.openLedgerOp()
                    .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                    .withPassword("password".getBytes(StandardCharsets.UTF_8))
                    .open(lId);) {
                }

                bkc.deleteLedgerOp().execute(lId).get();

                try {
                    bkc.openLedgerOp()
                        .withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType.MAC)
                        .withPassword("password".getBytes(StandardCharsets.UTF_8))
                        .open(lId);
                    fail("ledger cannot be open if delete succeeded");
                } catch (BKNoSuchLedgerExistsException ok) {
                }

                try (WriteHandler writer = bkc.createLedgerOp()
                    .create();) {
                    lId = writer.getId();
                }
                try (ReadHandler opened = bkc.openLedgerOp()
                    .open(lId);) {
                }

                bkc.deleteLedgerOp().execute(lId).get();
                try {
                    bkc.openLedgerOp()
                        .open(lId);
                    fail("ledger cannot be open if delete succeeded");
                } catch (BKNoSuchLedgerExistsException ok) {
                }

                try (WriteHandler writer = bkc.createLedgerOp()
                    .create();) {
                    lId = writer.getId();
                }
                try (ReadHandler opened = bkc.openLedgerOp()
                    .open(lId);) {
                }

                bkc.deleteLedgerOp().execute(lId).get();

                try {
                    bkc.openLedgerOp()
                        .open(lId);
                    fail("ledger cannot be open if delete succeeded");
                } catch (BKNoSuchLedgerExistsException ok) {
                }

                try {
                    FutureUtils.result(bkc.deleteLedgerOp().execute(lId));
                } catch (BKNoSuchLedgerExistsException ok) {
                }
            }
    }

    private static void checkEntries(Iterable<LedgerEntry> entries, byte[] data)
        throws InterruptedException, BKException {
        for (LedgerEntry le : entries) {
            assertArrayEquals(le.getEntry(), data);
        }
    }

}
