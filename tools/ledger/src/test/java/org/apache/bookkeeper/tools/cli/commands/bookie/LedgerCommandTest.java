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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.Iterator;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.LedgerEntryPage;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.SingleDirectoryDbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link LedgerCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DbLedgerStorage.class, SortedLedgerStorage.class, InterleavedLedgerStorage.class, Bookie.class,
        LedgerStorage.class, LedgerCache.PageEntries.class, LedgerCache.PageEntriesIterable.class, LedgerCommand.class,
        LedgerCache.LedgerIndexMetadata.class })
public class LedgerCommandTest extends BookieCommandTestBase {

    private LedgerCache.LedgerIndexMetadata metadata;
    private ServerConfiguration tConf;

    public LedgerCommandTest() {
        super(3, 0);
    }

    public void setup() throws Exception {
        super.setup();
        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);

        PowerMockito.mockStatic(DbLedgerStorage.class);
        PowerMockito.doAnswer(invocationOnMock -> {
            SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor p = invocationOnMock.getArgument(2);
            p.process(1L, 1L, 1L);
            return true;
        }).when(DbLedgerStorage.class, "readLedgerIndexEntries", anyLong(), any(ServerConfiguration.class),
                any(SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor.class));
        PowerMockito.when(DbLedgerStorage.class.getName())
                .thenReturn("org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage");

        tConf = PowerMockito.mock(ServerConfiguration.class);
        PowerMockito.whenNew(ServerConfiguration.class).withArguments(ServerConfiguration.class)
                .thenReturn(tConf);

        InterleavedLedgerStorage interleavedLedgerStorage = PowerMockito.mock(InterleavedLedgerStorage.class);
        PowerMockito.whenNew(InterleavedLedgerStorage.class).withNoArguments().thenReturn(interleavedLedgerStorage);

        PowerMockito.mockStatic(Bookie.class);
        PowerMockito.when(Bookie.mountLedgerStorageOffline(eq(tConf), eq(interleavedLedgerStorage)))
                .thenReturn(PowerMockito.mock(LedgerStorage.class));

        LedgerCache.PageEntries e = PowerMockito.mock(LedgerCache.PageEntries.class);
        LedgerCache.PageEntriesIterable i = PowerMockito.mock(LedgerCache.PageEntriesIterable.class);
        PowerMockito.when(interleavedLedgerStorage.getIndexEntries(anyLong())).thenReturn(i);
        PowerMockito.when(i.iterator()).thenReturn(getPageIterator(e));
        LedgerEntryPage lep = PowerMockito.mock(LedgerEntryPage.class);
        PowerMockito.when(e.getLEP()).thenReturn(lep);

        metadata = PowerMockito.mock(LedgerCache.LedgerIndexMetadata.class);
        PowerMockito.when(interleavedLedgerStorage.readLedgerIndexMetadata(anyLong())).thenReturn(metadata);
        PowerMockito.when(metadata.getMasterKeyHex()).thenReturn("");
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

        PowerMockito.verifyNew(ServerConfiguration.class, times(1)).withArguments(eq(conf));
        PowerMockito.verifyNew(InterleavedLedgerStorage.class, times(1)).withNoArguments();

        verify(metadata, times(1)).getMasterKeyHex();
    }

    @Test
    public void testDbLedgerStorage() throws Exception {
        conf.setLedgerStorageClass("org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage");
        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
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
