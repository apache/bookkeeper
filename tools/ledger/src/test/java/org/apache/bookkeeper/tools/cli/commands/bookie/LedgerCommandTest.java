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
 */
package org.apache.bookkeeper.tools.cli.commands.bookie;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import java.util.Iterator;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.LedgerEntryPage;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.SingleDirectoryDbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

/**
 * Unit test for {@link LedgerCommand}.
 */
public class LedgerCommandTest extends BookieCommandTestBase {

    private LedgerCache.LedgerIndexMetadata metadata;

    public LedgerCommandTest() {
        super(3, 0);
    }

    public void setup() throws Exception {
        super.setup();
        mockServerConfigurationConstruction(serverconf -> {
            final ServerConfiguration defaultValue = new ServerConfiguration();
            when(serverconf.getLedgerStorageClass()).thenReturn(defaultValue.getLedgerStorageClass());
        });
        final MockedStatic<DbLedgerStorage> dbLedgerStorageMockedStatic = mockStatic(DbLedgerStorage.class);
        dbLedgerStorageMockedStatic
                .when(() -> DbLedgerStorage.readLedgerIndexEntries(anyLong(),
                        any(ServerConfiguration.class),
                        any(SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor.class)))
                .thenAnswer(invocation -> {
                    SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor p = invocation.getArgument(2);
                    p.process(1L, 1L, 1L);
                    return true;
                });


        LedgerCache.PageEntries e = mock(LedgerCache.PageEntries.class);
        LedgerCache.PageEntriesIterable i = mock(LedgerCache.PageEntriesIterable.class);

        metadata = mock(LedgerCache.LedgerIndexMetadata.class);
        mockConstruction(InterleavedLedgerStorage.class, (interleavedLedgerStorage, context) -> {
            when(interleavedLedgerStorage.getIndexEntries(anyLong())).thenReturn(i);
            when(interleavedLedgerStorage.readLedgerIndexMetadata(anyLong())).thenReturn(metadata);
        });

        final MockedStatic<BookieImpl> bookieMockedStatic = mockStatic(BookieImpl.class);
        bookieMockedStatic.when(() -> BookieImpl.mountLedgerStorageOffline(any(), any()))
                .thenReturn(mock(LedgerStorage.class));

        when(i.iterator()).thenReturn(getPageIterator(e));
        LedgerEntryPage lep = mock(LedgerEntryPage.class);
        when(e.getLEP()).thenReturn(lep);



        when(metadata.getMasterKeyHex()).thenReturn("");
    }

    public Iterator<LedgerCache.PageEntries> getPageIterator(LedgerCache.PageEntries page) {
        Iterator<LedgerCache.PageEntries> i = new Iterator<LedgerCache.PageEntries>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                if (i < 2) {
                    i++;
                    return true;
                }
                return false;
            }

            @Override
            public LedgerCache.PageEntries next() {
                return page;
            }
        };
        return i;
    }

    // Test without ledger id
    @Test
    public void testWithoutLedgerId() {
        testLedgerCommand("");
    }

    // test ledger command without args
    @Test
    public void testNoArguments() {
        testLedgerCommand("-id", "1");
    }

    @Test
    public void testWithMeta() throws Exception {
        LedgerCommand cmd = new LedgerCommand();
        cmd.apply(bkFlags, new String[] { "-id", "1", "-m" });

        verify(metadata, times(1)).getMasterKeyHex();
    }

    @Test
    public void testDbLedgerStorage() throws Exception {

        mockServerConfigurationConstruction(conf -> {
            when(conf.getLedgerStorageClass()).thenReturn("org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage");
        });
        LedgerCommand cmd = new LedgerCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[]{"-id", "1"}));
    }

    // test update formatter by flag
    @Test
    public void testFormatterFlag() {
        testLedgerCommand("-id", "1", "-l", "hex");
    }

    public void testLedgerCommand(String... args) {
        LedgerCommand ledgerCommand = new LedgerCommand();
        try {
            ledgerCommand.apply(bkFlags, args);
        } catch (IllegalArgumentException iae) {
            if (!iae.getMessage().equals("No ledger id is specified")) {
                Assert.fail("exception is not expect ! ");
            }
        }
    }
}
