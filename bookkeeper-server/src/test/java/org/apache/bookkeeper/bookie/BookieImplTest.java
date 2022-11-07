/**
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
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.PortManager;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookieImplTest extends BookKeeperClusterTestCase {
    private static final Logger log = LoggerFactory.getLogger(BookieImplTest.class);

    private static final int bookiePort = PortManager.nextFreePort();

    private static final int ADD = 0;
    private static final int RECOVERY_ADD = 1;

    public BookieImplTest() {
        super(0);
    }

    @Test
    public void testWriteLac() throws Exception {
        final String metadataServiceUri = zkUtil.getMetadataServiceUri();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setMetadataServiceUri(metadataServiceUri);

        MetadataBookieDriver metadataDriver = BookieResources.createMetadataDriver(
                conf, NullStatsLogger.INSTANCE);
        RegistrationManager rm = metadataDriver.createRegistrationManager();
        TestBookieImpl.Resources resources = new TestBookieImpl.ResourceBuilder(conf)
                .withMetadataDriver(metadataDriver).withRegistrationManager(rm).build();
        BookieImpl b = new TestBookieImpl(resources);
        b.start();

        final BookieImpl spyBookie = spy(b);

        final long ledgerId = 10;
        final long lac = 23;

        DigestManager digestManager = DigestManager.instantiate(ledgerId, "".getBytes(StandardCharsets.UTF_8),
                BookKeeper.DigestType.toProtoDigestType(BookKeeper.DigestType.CRC32), UnpooledByteBufAllocator.DEFAULT,
                baseClientConf.getUseV2WireProtocol());

        final ByteBufList toSend = digestManager.computeDigestAndPackageForSendingLac(lac);
        ByteString body = UnsafeByteOperations.unsafeWrap(toSend.array(), toSend.arrayOffset(), toSend.readableBytes());

        final ByteBuf lacToAdd = Unpooled.wrappedBuffer(body.asReadOnlyByteBuffer());
        final byte[] masterKey = ByteString.copyFrom("masterKey".getBytes()).toByteArray();

        final ByteBuf explicitLACEntry = b.createExplicitLACEntry(ledgerId, lacToAdd);
        lacToAdd.resetReaderIndex();

        doReturn(explicitLACEntry)
                .when(spyBookie)
                .createExplicitLACEntry(eq(ledgerId), eq(lacToAdd));

        spyBookie.setExplicitLac(lacToAdd, null, null, masterKey);

        assertEquals(0, lacToAdd.refCnt());
        assertEquals(0, explicitLACEntry.refCnt());

        b.shutdown();

    }

    @Test
    public void testAddEntry() throws Exception {
        mockAddEntryReleased(ADD);
    }

    @Test
    public void testRecoveryAddEntry() throws Exception {
        mockAddEntryReleased(RECOVERY_ADD);
    }

    public void mockAddEntryReleased(int flag) throws Exception {
        final String metadataServiceUri = zkUtil.getMetadataServiceUri();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setMetadataServiceUri(metadataServiceUri);

        MetadataBookieDriver metadataDriver = BookieResources.createMetadataDriver(
                conf, NullStatsLogger.INSTANCE);
        RegistrationManager rm = metadataDriver.createRegistrationManager();
        TestBookieImpl.Resources resources = new TestBookieImpl.ResourceBuilder(conf)
                .withMetadataDriver(metadataDriver).withRegistrationManager(rm).build();
        BookieImpl b = new TestBookieImpl(resources);
        b.start();

        final BookieImpl spyBookie = spy(b);

        final long ledgerId = 10;

        final byte[] masterKey = ByteString.copyFrom("masterKey".getBytes()).toByteArray();

        final ByteBuf masterKeyEntry = b.createMasterKeyEntry(ledgerId, masterKey);

        doReturn(masterKeyEntry)
                .when(spyBookie)
                .createMasterKeyEntry(eq(ledgerId), eq(masterKey));

        final ByteBuf entry = generateEntry(ledgerId, 0);

        AtomicBoolean complete = new AtomicBoolean(false);
        final BookkeeperInternalCallbacks.WriteCallback writeCallback =
                new BookkeeperInternalCallbacks.WriteCallback() {
                    @Override
                    public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                        complete.set(true);
                    }
                };

        switch (flag) {
            case ADD:
                spyBookie.addEntry(entry, false, writeCallback, null, masterKey);
                break;
            case RECOVERY_ADD:
                spyBookie.recoveryAddEntry(entry, writeCallback, null, masterKey);
                break;
            default:
                throw new IllegalArgumentException("Only support ADD and RECOVERY_ADD flag.");
        }

        Awaitility.await().untilAsserted(() -> assertTrue(complete.get()));

        assertEquals(0, entry.refCnt());
        assertEquals(0, masterKeyEntry.refCnt());

        b.shutdown();

    }

    private ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = ("ledger-" + ledger + "-" + entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }
}
