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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.PortManager;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The tests for {@link Bookie}.
 */
public class BookieTest extends BookKeeperClusterTestCase {
    private static final Logger log = LoggerFactory.getLogger(BookieTest.class);

    private static final int bookiePort = PortManager.nextFreePort();

    public BookieTest() {
        super(0);
    }

    @Test
    public void testWriteLac() throws Exception {
        final String metadataServiceUri = zkUtil.getMetadataServiceUri();
        File ledgerDir = createTempDir("bkLacTest", ".dir");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setMetadataServiceUri(metadataServiceUri)
                .setBookiePort(bookiePort)
                .setJournalDirName(ledgerDir.toString())
                .setLedgerDirNames(new String[]{ledgerDir.getAbsolutePath()});

        Bookie b = new Bookie(conf);
        b.start();

        final Bookie spyBookie = spy(b);

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
}