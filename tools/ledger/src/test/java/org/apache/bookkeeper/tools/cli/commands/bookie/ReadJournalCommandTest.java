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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.File;
import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


/**
 * Unit test for read journal command.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ReadJournalCommand.class, Journal.class, LedgerDirsManager.class,
    DiskChecker.class })
public class ReadJournalCommandTest extends BookieCommandTestBase {

    private Journal journal;

    @Rule
    private TemporaryFolder folder = new TemporaryFolder();

    public ReadJournalCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);

        DiskChecker checker = mock(DiskChecker.class);
        whenNew(DiskChecker.class).withArguments(eq(conf.getDiskUsageThreshold()), eq(conf.getDiskUsageWarnThreshold()))
            .thenReturn(checker);
        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        whenNew(LedgerDirsManager.class).withArguments(eq(conf), eq(conf.getLedgerDirs()), eq(checker))
            .thenReturn(ledgerDirsManager);
        journal = mock(Journal.class);
        whenNew(Journal.class).withArguments(anyInt(), any(File.class), eq(conf), eq(ledgerDirsManager))
            .thenReturn(journal);
        when(journal.getJournalDirectory()).thenReturn(conf.getJournalDirs()[0]);
    }

    @Test
    public void testWithJournalId() throws Exception {
        conf.setJournalDirsName(new String[] { folder.getRoot().getAbsolutePath() });
        when(journal.getJournalDirectory()).thenReturn(new File(""));
        testCommand("-id", "1");
        verifyNew(Journal.class, times(1))
            .withArguments(anyInt(), any(File.class), eq(conf), any(LedgerDirsManager.class));
        verifyNew(LedgerDirsManager.class, times(1))
            .withArguments(eq(conf), eq(conf.getLedgerDirs()), any(DiskChecker.class));
        verifyNew(DiskChecker.class, times(1))
            .withArguments(eq(conf.getDiskUsageThreshold()), eq(conf.getDiskUsageWarnThreshold()));
    }

    @Test
    public void testWithFilename() throws Exception {
        conf.setJournalDirsName(new String[] { folder.getRoot().getAbsolutePath() });
        when(journal.getJournalDirectory()).thenReturn(new File(""));
        File file = folder.newFile("1.txn");
        testCommand("-f", file.getAbsolutePath(), "-m");
        verifyNew(Journal.class, times(1))
            .withArguments(anyInt(), any(File.class), eq(conf), any(LedgerDirsManager.class));
        verifyNew(LedgerDirsManager.class, times(1))
            .withArguments(eq(conf), eq(conf.getLedgerDirs()), any(DiskChecker.class));
        verifyNew(DiskChecker.class, times(1))
            .withArguments(eq(conf.getDiskUsageThreshold()), eq(conf.getDiskUsageWarnThreshold()));
    }

    @Test
    public void testWithMsg() throws Exception {
        testCommand("-id", "1", "-d", conf.getJournalDirs()[0].getAbsolutePath());
        verifyNew(Journal.class, times(3))
            .withArguments(anyInt(), any(File.class), eq(conf), any(LedgerDirsManager.class));
        verifyNew(LedgerDirsManager.class, times(3))
            .withArguments(eq(conf), eq(conf.getLedgerDirs()), any(DiskChecker.class));
        verifyNew(DiskChecker.class, times(3))
            .withArguments(eq(conf.getDiskUsageThreshold()), eq(conf.getDiskUsageWarnThreshold()));
        verify(journal, times(1)).getJournalDirectory();
    }

    public void testCommand(String... args) throws Exception {
        ReadJournalCommand command = new ReadJournalCommand();
        Assert.assertTrue(command.apply(bkFlags, args));
    }

    @Test
    public void testWithoutArgs() {
        ReadJournalCommand command = new ReadJournalCommand();
        Assert.assertFalse(command.apply(bkFlags, new String[] { "" }));
    }
}
