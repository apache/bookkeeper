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

import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.File;
import java.io.IOException;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


/**
 * Unit test for {@link ListFilesOnDiscCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ListFilesOnDiscCommand.class })
public class ListFilesOnDiscCommandTest extends BookieCommandTestBase {

    @Rule
    private TemporaryFolder folder = new TemporaryFolder();

    public ListFilesOnDiscCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        createTmpDirs();
        whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
    }

    private void createTmpDirs() throws IOException {
        File journals = folder.newFolder("journals");
        conf.setJournalDirsName(new String[] { journals.getAbsolutePath() });
        journals.mkdir();
        File ledgers = folder.newFolder("ledgers");
        conf.setLedgerDirNames(new String[] { ledgers.getAbsolutePath() });
        ledgers.mkdir();
        File index = folder.newFolder("index");
        conf.setIndexDirName(new String[] { index.getAbsolutePath() });
        index.mkdir();

        for (int i = 0; i < 10; i++) {
            File.createTempFile("journal-" + i, ".txn", journals);
            File.createTempFile("ledger-" + i, ".log", ledgers);
            File.createTempFile("index-" + i, ".idx", index);
        }
        System.out.println("over");
    }

    @Test
    public void testListJournalCommand() {
        testCommand("-txn");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(conf.getJournalDirs(), "txn").size());
    }

    @Test
    public void testListJournalLongCommand() {
        testCommand("--journal");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(conf.getJournalDirs(), "txn").size());
    }

    @Test
    public void testListEntryLogCommand() {
        testCommand("-log");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(conf.getLedgerDirs(), "log").size());
    }

    @Test
    public void testListEntryLogLongCommand() {
        testCommand("--entrylog");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(conf.getLedgerDirs(), "log").size());
    }

    @Test
    public void testListIndexCommand() {
        testCommand("-idx");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(conf.getIndexDirs(), "idx").size());
    }

    @Test
    public void testListIndexLongCommand() {
        testCommand("--index");
        Assert.assertEquals(10, BookieShell.listFilesAndSort(conf.getIndexDirs(), "idx").size());
    }

    private void testCommand(String... args) {
        ListFilesOnDiscCommand cmd = new ListFilesOnDiscCommand();
        Assert.assertTrue(cmd.apply(bkFlags, args));
    }
}
