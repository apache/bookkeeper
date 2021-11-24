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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

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
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link ConvertToInterleavedStorageCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ConvertToInterleavedStorageCommand.class, LedgerCache.class })
public class ConvertToInterleavedStorageCommandTest extends BookieCommandTestBase {

    private LedgerDirsManager ledgerDirsManager;
    private DbLedgerStorage dbStorage;
    private InterleavedLedgerStorage interleavedLedgerStorage;
    private LedgerCache interleavedLedgerCache;

    @Rule
    private TemporaryFolder folder = new TemporaryFolder();

    public ConvertToInterleavedStorageCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        createTmpFile();
        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
        PowerMockito.whenNew(ServerConfiguration.class).withParameterTypes(AbstractConfiguration.class)
            .withArguments(eq(conf)).thenReturn(conf);

        DiskChecker diskChecker = mock(DiskChecker.class);
        whenNew(DiskChecker.class).withArguments(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold())
            .thenReturn(diskChecker);

        ledgerDirsManager = mock(LedgerDirsManager.class);
        whenNew(LedgerDirsManager.class).withParameterTypes(ServerConfiguration.class, File[].class, DiskChecker.class)
            .withArguments(conf, conf.getLedgerDirs(), diskChecker).thenReturn(ledgerDirsManager);
        when(ledgerDirsManager.getAllLedgerDirs()).thenReturn(getFileList());

        dbStorage = mock(DbLedgerStorage.class);
        whenNew(DbLedgerStorage.class).withNoArguments().thenReturn(dbStorage);
        when(dbStorage.getActiveLedgersInRange(anyLong(), anyLong())).thenReturn(this::getLedgerId);

        interleavedLedgerCache = mock(LedgerCache.class);
        doNothing().when(interleavedLedgerCache).flushLedger(anyBoolean());

        interleavedLedgerStorage = mock(InterleavedLedgerStorage.class);
        whenNew(InterleavedLedgerStorage.class).withNoArguments().thenReturn(interleavedLedgerStorage);
        doNothing().when(interleavedLedgerStorage).flush();
        doNothing().when(interleavedLedgerStorage).shutdown();
        when(interleavedLedgerStorage.getLedgerCache()).thenReturn(interleavedLedgerCache);
    }

    private Iterator<Long> getLedgerId() {
        Vector<Long> longs = new Vector<>();
        LongStream.range(0L, 10L).forEach(longs::add);
        return longs.iterator();
    }

    private List<File> getFileList() {
        List<File> files = new LinkedList<>();
        files.add(folder.getRoot());
        return files;
    }

    private void createTmpFile() {
        try {
            folder.newFile("ledgers");
            folder.newFile("locations");
            System.out.println(folder.getRoot().getAbsolutePath());
        } catch (IOException e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    @Test
    public void testConvertToInterleavedStorageCommand() {
        ConvertToInterleavedStorageCommand cmd = new ConvertToInterleavedStorageCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));

        try {
            verifyNew(ServerConfiguration.class).withArguments(eq(conf));
            verifyNew(LedgerDirsManager.class, times(2))
                .withArguments(eq(conf), any(File[].class), any(DiskChecker.class));
            verifyNew(DbLedgerStorage.class, times(1)).withNoArguments();
            verifyNew(InterleavedLedgerStorage.class, times(1)).withNoArguments();

            verify(dbStorage, times(1)).initialize(eq(conf), eq(null), any(LedgerDirsManager.class),
                any(LedgerDirsManager.class), eq(NullStatsLogger.INSTANCE), eq(PooledByteBufAllocator.DEFAULT));
            verify(interleavedLedgerStorage, times(1))
                .initialize(eq(conf), eq(null), any(LedgerDirsManager.class),
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
