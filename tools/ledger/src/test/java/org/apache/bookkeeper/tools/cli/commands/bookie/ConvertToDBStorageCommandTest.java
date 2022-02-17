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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Iterator;
import java.util.Vector;
import java.util.stream.LongStream;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

/**
 * Unit test for {@link ConvertToDBStorageCommand}.
 */
public class ConvertToDBStorageCommandTest extends BookieCommandTestBase {

    private MockedConstruction<InterleavedLedgerStorage> interleavedLedgerStorageMockedConstruction;
    private MockedConstruction<DbLedgerStorage> dbLedgerStorageMockedConstruction;
    private LedgerCache.LedgerIndexMetadata metadata;
    private LedgerCache.PageEntriesIterable entries;
    private MockedStatic<BookieImpl> bookieImplMockedStatic;

    public ConvertToDBStorageCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        createMockedServerConfiguration();

        metadata = mock(LedgerCache.LedgerIndexMetadata.class);
        entries = mock(LedgerCache.PageEntriesIterable.class);

        interleavedLedgerStorageMockedConstruction =
                mockConstruction(InterleavedLedgerStorage.class, (interleavedLedgerStorage, context) -> {
                    doNothing().when(interleavedLedgerStorage).shutdown();
                    when(interleavedLedgerStorage.getActiveLedgersInRange(anyLong(), anyLong())).thenReturn(
                            ConvertToDBStorageCommandTest.this::getLedgerId);
                    when(interleavedLedgerStorage.readLedgerIndexMetadata(anyLong())).thenReturn(metadata);
                    when(interleavedLedgerStorage.getIndexEntries(anyLong())).thenReturn(entries);
                });
        dbLedgerStorageMockedConstruction = mockConstruction(DbLedgerStorage.class, (dbStorage, context) -> {
            doNothing().when(dbStorage).shutdown();
            when(dbStorage.addLedgerToIndex(anyLong(), anyBoolean(), eq(new byte[0]),
                    any(LedgerCache.PageEntriesIterable.class))).thenReturn(1L);
        });
        bookieImplMockedStatic = mockStatic(BookieImpl.class);
        bookieImplMockedStatic.when(() -> BookieImpl.mountLedgerStorageOffline(any(ServerConfiguration.class),
                        any(InterleavedLedgerStorage.class)))
            .thenReturn(mock(InterleavedLedgerStorage.class));
        bookieImplMockedStatic.when(() -> BookieImpl.mountLedgerStorageOffline(any(ServerConfiguration.class),
                any(DbLedgerStorage.class)))
                .thenAnswer((invocation) -> dbLedgerStorageMockedConstruction.constructed().get(0));
    }

    @After
    public void cleanup() {
        bookieImplMockedStatic.close();
        interleavedLedgerStorageMockedConstruction.close();
        dbLedgerStorageMockedConstruction.close();
    }

    private Iterator<Long> getLedgerId() {
        Vector<Long> longs = new Vector<>();
        LongStream.range(0L, 10L).forEach(longs::add);
        return longs.iterator();
    }

    @Test
    public void testCTDB() {
        ConvertToDBStorageCommand cmd = new ConvertToDBStorageCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));

        try {
            InterleavedLedgerStorage interleavedLedgerStorage = interleavedLedgerStorageMockedConstruction
                    .constructed().get(0);

            DbLedgerStorage dbStorage = dbLedgerStorageMockedConstruction.constructed().get(0);
            verify(interleavedLedgerStorage, times(10)).readLedgerIndexMetadata(anyLong());
            verify(interleavedLedgerStorage, times(10)).getIndexEntries(anyLong());
            verify(dbStorage, times(10))
                .addLedgerToIndex(anyLong(), anyBoolean(), any(), any(LedgerCache.PageEntriesIterable.class));
            verify(interleavedLedgerStorage, times(10)).deleteLedger(anyLong());

            verify(dbStorage, times(1)).shutdown();
            verify(interleavedLedgerStorage, times(1)).shutdown();
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }
}
