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

import static org.mockito.Mockito.doReturn;

import java.io.File;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test for {@link ListFilesOnDiscCommand}.
 */
public class ListFilesOnDiscCommandTest extends BookieCommandTestBase {

    public ListFilesOnDiscCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        File journals = testDir.newFolder("journals");
        journals.mkdir();
        File ledgers = testDir.newFolder("ledgers");
        ledgers.mkdir();
        File index = testDir.newFolder("index");
        index.mkdir();

        for (int i = 0; i < 10; i++) {
            File.createTempFile("journal-" + i, ".txn", journals);
            File.createTempFile("ledger-" + i, ".log", ledgers);
            File.createTempFile("index-" + i, ".idx", index);
        }
        mockServerConfigurationConstruction(conf -> {
            doReturn(new String[] { index.getAbsolutePath() }).when(conf).getIndexDirNames();
            doReturn(new String[] { ledgers.getAbsolutePath() }).when(conf).getLedgerDirNames();
            doReturn(new String[] { journals.getAbsolutePath() }).when(conf).getJournalDirNames();
        });
    }

    @Test
    public void testListJournalCommand() {
        testCommand("-txn");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(
                getMockedConstruction(ServerConfiguration.class).constructed().get(0).getJournalDirs(), "txn").size());
    }

    @Test
    public void testListJournalLongCommand() {
        testCommand("--journal");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(
                getMockedConstruction(ServerConfiguration.class).constructed().get(0).getJournalDirs(), "txn").size());
    }

    @Test
    public void testListEntryLogCommand() {
        testCommand("-log");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(
                getMockedConstruction(ServerConfiguration.class).constructed().get(0).getLedgerDirs(), "log").size());
    }

    @Test
    public void testListEntryLogLongCommand() {
        testCommand("--entrylog");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(
                getMockedConstruction(ServerConfiguration.class).constructed().get(0).getLedgerDirs(), "log").size());
    }

    @Test
    public void testListIndexCommand() {
        testCommand("-idx");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(
                getMockedConstruction(ServerConfiguration.class).constructed().get(0).getIndexDirs(), "idx").size());
    }

    @Test
    public void testListIndexLongCommand() {
        testCommand("--index");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(
                getMockedConstruction(ServerConfiguration.class).constructed().get(0).getIndexDirs(), "idx").size());
    }

    private void testCommand(String... args) {
        ListFilesOnDiscCommand cmd = new ListFilesOnDiscCommand();
        Assert.assertTrue(cmd.apply(bkFlags, args));
    }
}
