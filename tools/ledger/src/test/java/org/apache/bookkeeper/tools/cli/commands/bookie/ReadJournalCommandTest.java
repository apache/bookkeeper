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
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test for read journal command.
 */
public class ReadJournalCommandTest extends BookieCommandTestBase {

    public ReadJournalCommandTest() {
        super(3, 0);
    }

    @Test
    public void testWithJournalId() throws Exception {
        AtomicInteger journalCount = new AtomicInteger();
        AtomicInteger ledgerDirsManagerCount = new AtomicInteger();
        AtomicInteger diskCheckerCount = new AtomicInteger();
        mockServerConfigurationConstruction(conf -> {
            doReturn(new String[] {new File(journalDirsName[0]).getAbsolutePath()}).when(conf).getJournalDirNames();
        });
        mockConstruction(DiskChecker.class, (c, context) -> {

            final ServerConfiguration defaultConf = new ServerConfiguration();
            assertEquals(defaultConf.getDiskUsageThreshold(), context.arguments().get(0));
            assertEquals(defaultConf.getDiskUsageWarnThreshold(), context.arguments().get(1));
            diskCheckerCount.incrementAndGet();

        });
        mockConstruction(LedgerDirsManager.class, (c, context) -> {
            ledgerDirsManagerCount.incrementAndGet();
        });
        mockConstruction(Journal.class, (journal, context) -> {
            doAnswer(invocation ->
                    getMockedConstruction(ServerConfiguration.class).constructed().get(0).getJournalDirs()[0]
            ).when(journal).getJournalDirectory();
            journalCount.incrementAndGet();
        });

        testCommand("-id", "1");
        assertEquals(1, diskCheckerCount.get());
        assertEquals(1, ledgerDirsManagerCount.get());
        assertEquals(1, journalCount.get());
    }

    @Test
    public void testWithFilename() throws Exception {
        AtomicInteger journalCount = new AtomicInteger();
        AtomicInteger ledgerDirsManagerCount = new AtomicInteger();
        AtomicInteger diskCheckerCount = new AtomicInteger();
        mockServerConfigurationConstruction(conf -> {
            doReturn(new String[] {new File(journalDirsName[0]).getAbsolutePath()}).when(conf).getJournalDirNames();
        });
        mockConstruction(DiskChecker.class, (c, context) -> {
            final ServerConfiguration defaultConf = new ServerConfiguration();
            assertEquals(defaultConf.getDiskUsageThreshold(), context.arguments().get(0));
            assertEquals(defaultConf.getDiskUsageWarnThreshold(), context.arguments().get(1));
            diskCheckerCount.incrementAndGet();

        });
        mockConstruction(LedgerDirsManager.class, (c, context) -> {
            ledgerDirsManagerCount.incrementAndGet();
        });
        mockConstruction(Journal.class, (journal, context) -> {
            doAnswer(invocation ->
                    getMockedConstruction(ServerConfiguration.class).constructed().get(0).getJournalDirs()[0]
            ).when(journal).getJournalDirectory();
            journalCount.incrementAndGet();
        });
        File file = testDir.newFile("1.txn");
        testCommand("-f", file.getAbsolutePath(), "-m");
        assertEquals(1, diskCheckerCount.get());
        assertEquals(1, ledgerDirsManagerCount.get());
        assertEquals(1, journalCount.get());
    }

    @Test
    public void testWithMsg() throws Exception {
        AtomicInteger journalCount = new AtomicInteger();
        AtomicInteger ledgerDirsManagerCount = new AtomicInteger();
        AtomicInteger diskCheckerCount = new AtomicInteger();
        mockServerConfigurationConstruction();
        mockConstruction(DiskChecker.class, (c, context) -> {

            final ServerConfiguration defaultConf = new ServerConfiguration();
            assertEquals(defaultConf.getDiskUsageThreshold(), context.arguments().get(0));
            assertEquals(defaultConf.getDiskUsageWarnThreshold(), context.arguments().get(1));
            diskCheckerCount.incrementAndGet();

        });
        mockConstruction(LedgerDirsManager.class, (c, context) -> {
            ledgerDirsManagerCount.incrementAndGet();
        });
        mockConstruction(Journal.class, (journal, context) -> {
            doAnswer(invocation ->
                    getMockedConstruction(ServerConfiguration.class).constructed().get(0).getJournalDirs()[0]
            ).when(journal).getJournalDirectory();
            journalCount.incrementAndGet();
        });
        testCommand("-id", "1", "-d", new File(journalDirsName[0]).getAbsolutePath());
        assertEquals(3, journalCount.get());
        assertEquals(3, ledgerDirsManagerCount.get());
        assertEquals(3, diskCheckerCount.get());
        verify(getMockedConstruction(Journal.class).constructed().get(0), times(1)).getJournalDirectory();
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
