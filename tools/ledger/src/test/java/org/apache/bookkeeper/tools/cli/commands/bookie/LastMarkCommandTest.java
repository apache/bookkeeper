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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.File;
import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.Journal.LastLogMark;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LogMark;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test of {@link LastMarkCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ LastMarkCommand.class })
public class LastMarkCommandTest extends BookieCommandTestBase {

    private Journal journal;
    private LastLogMark lastLogMark;
    private LogMark logMark;

    public LastMarkCommandTest() {
        super(3, 0);
    }

    @Before
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class)
            .withNoArguments()
            .thenReturn(conf);

        PowerMockito.whenNew(LedgerDirsManager.class)
            .withParameterTypes(
                ServerConfiguration.class,
                File[].class,
                DiskChecker.class)
            .withArguments(
                eq(conf),
                any(File[].class),
                any(DiskChecker.class))
            .thenReturn(mock(LedgerDirsManager.class));

        this.journal = mock(Journal.class);
        this.lastLogMark = mock(LastLogMark.class);
        this.logMark = mock(LogMark.class);
        when(lastLogMark.getCurMark()).thenReturn(logMark);
        when(journal.getLastLogMark()).thenReturn(lastLogMark);
        PowerMockito.whenNew(Journal.class)
            .withParameterTypes(
                int.class,
                File.class,
                ServerConfiguration.class,
                LedgerDirsManager.class)
            .withArguments(
                any(int.class),
                any(File.class),
                eq(conf),
                any(LedgerDirsManager.class))
            .thenReturn(journal);
    }

    @Test
    public void testCommand() throws Exception {
        LastMarkCommand cmd = new LastMarkCommand();
        cmd.apply(bkFlags, new String[] { "" });

        PowerMockito.verifyNew(LedgerDirsManager.class, times(1))
            .withArguments(eq(conf), any(File[].class), any(DiskChecker.class));
        PowerMockito.verifyNew(Journal.class, times(3))
            .withArguments(any(int.class), any(File.class), eq(conf), any(LedgerDirsManager.class));
        verify(journal, times(3)).getLastLogMark();
        verify(lastLogMark, times(3)).getCurMark();
        verify(logMark, times(3 * 2)).getLogFileId();
        verify(logMark, times(3)).getLogFileOffset();
    }




}
