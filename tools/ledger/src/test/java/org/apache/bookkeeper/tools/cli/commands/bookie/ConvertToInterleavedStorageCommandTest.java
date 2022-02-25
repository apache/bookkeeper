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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.stream.LongStream;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link ConvertToInterleavedStorageCommand}.
 */
public class ConvertToInterleavedStorageCommandTest extends BookieCommandTestBase {

    private LedgerCache interleavedLedgerCache;


    public ConvertToInterleavedStorageCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        createTmpFile();
        mockServerConfigurationConstruction();

        mockConstruction(DiskChecker.class);

        mockConstruction(LedgerDirsManager.class, (ledgersDirManager, context) -> {
            when(ledgersDirManager.getAllLedgerDirs()).thenReturn(getFileList());
        });


        mockConstruction(DbLedgerStorage.class, (dbStorage, context) -> {
            when(dbStorage.getActiveLedgersInRange(anyLong(), anyLong()))
                    .thenReturn(ConvertToInterleavedStorageCommandTest.this::getLedgerId);
        });

        interleavedLedgerCache = mock(LedgerCache.class);
        doNothing().when(interleavedLedgerCache).flushLedger(anyBoolean());

        mockConstruction(InterleavedLedgerStorage.class,
                (interleavedLedgerStorage, context) -> {
                    doNothing().when(interleavedLedgerStorage).flush();
                    doNothing().when(interleavedLedgerStorage).shutdown();
                    when(interleavedLedgerStorage.getLedgerCache()).thenReturn(interleavedLedgerCache);
                });
    }


    private Iterator<Long> getLedgerId() {
        Vector<Long> longs = new Vector<>();
        LongStream.range(0L, 10L).forEach(longs::add);
        return longs.iterator();
    }

    private List<File> getFileList() {
        List<File> files = new LinkedList<>();
        files.add(testDir.getRoot());
        return files;
    }

    private void createTmpFile() {
        try {
            testDir.newFile("ledgers");
            testDir.newFile("locations");
        } catch (IOException e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    @Test
    public void testConvertToInterleavedStorageCommand() {
        ConvertToInterleavedStorageCommand cmd = new ConvertToInterleavedStorageCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));

        try {
            final DbLedgerStorage dbStorage = getMockedConstruction(DbLedgerStorage.class).constructed().get(0);
            final InterleavedLedgerStorage interleavedLedgerStorage =
                    getMockedConstruction(InterleavedLedgerStorage.class).constructed().get(0);
            verify(dbStorage, times(1)).initialize(
                    any(ServerConfiguration.class), eq(null), any(LedgerDirsManager.class),
                any(LedgerDirsManager.class), eq(NullStatsLogger.INSTANCE), eq(PooledByteBufAllocator.DEFAULT));

            verify(interleavedLedgerStorage, times(1))
                .initialize(any(ServerConfiguration.class), eq(null), any(LedgerDirsManager.class),
                    any(LedgerDirsManager.class), eq(NullStatsLogger.INSTANCE), eq(PooledByteBufAllocator.DEFAULT));
            verify(dbStorage, times(1)).getActiveLedgersInRange(anyLong(), anyLong());
            verify(dbStorage, times(10)).readMasterKey(anyLong());
            verify(interleavedLedgerStorage, times(10)).setMasterKey(anyLong(), any());
            verify(dbStorage, times(10)).getLastEntryInLedger(anyLong());
            verify(dbStorage, times(10)).getLocation(anyLong(), anyLong());
            verify(dbStorage, times(1)).shutdown();
            verify(interleavedLedgerCache, times(1)).flushLedger(true);
            verify(interleavedLedgerStorage, times(1)).flush();
            verify(interleavedLedgerStorage, times(1)).shutdown();
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }
}
