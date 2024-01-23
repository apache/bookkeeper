/*
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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Field;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookieProtoEncoding;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.proto.checksum.DummyDigestManager;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.zookeeper.AsyncCallback;
import org.junit.Test;
import org.mockito.Mockito;

public class TestLedgerFragmentReplicationWithMock {

    @Test
    public void testRecoverLedgerFragmentEntrySendRightRequestWithFlag() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        BookieClientImpl bookieClient = Mockito.mock(BookieClientImpl.class);
        doAnswer(invocationOnMock -> {
            ByteBuf toSend = invocationOnMock.getArgument(4);
            BookieProtoEncoding.RequestEnDeCoderPreV3 deCoderPreV3 =
                new BookieProtoEncoding.RequestEnDeCoderPreV3(null);
            toSend.readerIndex(4);
            BookieProtocol.ParsedAddRequest request = (BookieProtocol.ParsedAddRequest) deCoderPreV3.decode(toSend);

            Field flagField = request.getClass().getSuperclass().getDeclaredField("flags");
            flagField.setAccessible(true);
            short flag = flagField.getShort(request);
            assertEquals(flag, BookieProtocol.FLAG_RECOVERY_ADD);
            latch.countDown();
            return null;
        }).when(bookieClient)
            .addEntry(any(), anyLong(), any(), anyLong(), any(), any(), any(), anyInt(), anyBoolean(), any());

        BookKeeper bkc = Mockito.mock(BookKeeper.class);
        when(bkc.getBookieClient()).thenReturn(bookieClient);

        LedgerHandle lh = Mockito.mock(LedgerHandle.class);
        DummyDigestManager ds = new DummyDigestManager(1L, true, ByteBufAllocator.DEFAULT);
        when(lh.getDigestManager()).thenReturn(ds);
        when(lh.getLedgerKey()).thenReturn(DigestManager.generateMasterKey("".getBytes()));

        ByteBuf data = Unpooled.wrappedBuffer(new byte[1024]);
        LedgerEntry entry = new LedgerEntry(LedgerEntryImpl.create(1L, 1L, data.readableBytes(), data));
        List<LedgerEntry> list = new LinkedList<>();
        list.add(entry);
        Enumeration<LedgerEntry> entries = IteratorUtils.asEnumeration(list.iterator());
        doAnswer(invocation -> {
            org.apache.bookkeeper.client.AsyncCallback.ReadCallback rc =
                invocation.getArgument(2, org.apache.bookkeeper.client.AsyncCallback.ReadCallback.class);
            rc.readComplete(0, lh, entries, null);
            return null;
        }).when(lh).asyncReadEntries(anyLong(), anyLong(), any(), any());

        ClientConfiguration conf = new ClientConfiguration();
        LedgerFragmentReplicator lfr = new LedgerFragmentReplicator(bkc, conf);

        Set<BookieId> bookies = new HashSet<>();
        bookies.add(BookieId.parse("127.0.0.1:3181"));

        AsyncCallback.VoidCallback vc = new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
            }
        };

        lfr.recoverLedgerFragmentEntry(1L, lh, vc, bookies, (lid, le) -> {});

        latch.await();
    }
}
