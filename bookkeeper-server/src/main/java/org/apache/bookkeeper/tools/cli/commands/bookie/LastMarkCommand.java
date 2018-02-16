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

import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LogMark;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.util.DiskChecker;

/**
 * A bookie command to print the last log marker.
 */
@Parameters(commandDescription = "Print last log marker")
public class LastMarkCommand extends BookieCommand {

    @Override
    public String name() {
        return "lastmark";
    }

    @Override
    public void run(ServerConfiguration conf) throws Exception {
        LedgerDirsManager dirsManager = new LedgerDirsManager(
            conf, conf.getJournalDirs(),
            new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        List<Journal> journals = Lists.transform(
            Lists.newArrayList(conf.getJournalDirs()),
            dir -> new Journal(
                dir,
                conf,
                dirsManager)
        );
        for (Journal journal : journals) {
            LogMark lastLogMark = journal.getLastLogMark().getCurMark();
            System.out.println("LastLogMark : Journal Id - " + lastLogMark.getLogFileId() + "("
                + Long.toHexString(lastLogMark.getLogFileId()) + ".txn), Pos - "
                + lastLogMark.getLogFileOffset());
        }
    }
}
