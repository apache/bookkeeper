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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.Journal.LastLogMark;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LogMark;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link LastMarkCommand}.
 */
public class LastMarkCommandTest extends BookieCommandTestBase {

    private LastLogMark lastLogMark;
    private LogMark logMark;
    private static final int NUM_JOURNAL_DIRS = 3;

    public LastMarkCommandTest() {
        super(NUM_JOURNAL_DIRS, 0);
    }

    @Before
    public void setup() throws Exception {
        super.setup();

        mockServerConfigurationConstruction(conf -> {
            doReturn(0.95f).when(conf).getDiskUsageThreshold();
        });
        mockConstruction(LedgerDirsManager.class);

        this.lastLogMark = mock(LastLogMark.class);
        this.logMark = mock(LogMark.class);
        when(lastLogMark.getCurMark()).thenReturn(logMark);

        mockConstruction(Journal.class, (journal, context) -> {
            when(journal.getLastLogMark()).thenReturn(lastLogMark);
        });
    }

    @Test
    public void testCommand() throws Exception {
        LastMarkCommand cmd = new LastMarkCommand();
        cmd.apply(bkFlags, new String[] { "" });

        for (int i = 0; i < NUM_JOURNAL_DIRS; i++) {
            verify(getMockedConstruction(Journal.class).constructed().get(i), times(1)).getLastLogMark();
        }

        verify(lastLogMark, times(3)).getCurMark();
        verify(logMark, times(3 * 2)).getLogFileId();
        verify(logMark, times(3)).getLogFileOffset();
    }




}
