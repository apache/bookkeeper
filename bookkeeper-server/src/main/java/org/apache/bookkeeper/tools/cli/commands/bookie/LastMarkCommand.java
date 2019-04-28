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

import java.io.File;

import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LogMark;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.DiskChecker;

/**
 * A bookie command to print the last log marker.
 */
public class LastMarkCommand extends BookieCommand<CliFlags> {

    private static final String NAME = "lastmark";
    private static final String DESC = "Print last log marker";

    public LastMarkCommand() {
        super(CliSpec.newBuilder()
            .withName(NAME)
            .withFlags(new CliFlags())
            .withDescription(DESC)
            .build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, CliFlags flags) {
        LedgerDirsManager dirsManager = new LedgerDirsManager(
            conf, conf.getLedgerDirs(),
            new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        File[] journalDirs = conf.getJournalDirs();

        for (int idx = 0; idx < journalDirs.length; idx++) {
            Journal journal = new Journal(idx, journalDirs[idx], conf, dirsManager);
            LogMark lastLogMark = journal.getLastLogMark().getCurMark();
            System.out.println("LastLogMark : Journal Id - " + lastLogMark.getLogFileId() + "("
                + Long.toHexString(lastLogMark.getLogFileId()) + ".txn), Pos - "
                + lastLogMark.getLogFileOffset());
        }
        return true;
    }
}
