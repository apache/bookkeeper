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

import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.Enumeration;
import java.util.Vector;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test for sanity command.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ SanityTestCommand.class, LedgerEntry.class})
public class SanityTestCommandTest extends BookieCommandTestBase {

    private LedgerHandle lh;

    public SanityTestCommandTest() {
        super(3, 1);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        ClientConfiguration clientConf = mock(ClientConfiguration.class);
        PowerMockito.whenNew(ClientConfiguration.class).withNoArguments().thenReturn(clientConf);

        BookKeeper bk = mock(BookKeeper.class);
        lh = mock(LedgerHandle.class);
        PowerMockito.whenNew(BookKeeper.class).withParameterTypes(ClientConfiguration.class)
            .withArguments(any(ClientConfiguration.class)).thenReturn(bk);
        when(bk.createLedger(anyInt(), anyInt(), any(BookKeeper.DigestType.class), eq(new byte[0]))).thenReturn(lh);
        when(bk.openLedger(anyLong(), any(BookKeeper.DigestType.class), eq(new byte[0]))).thenReturn(lh);
        when(lh.getLastAddConfirmed()).thenReturn(9L);
        Enumeration<LedgerEntry> entryEnumeration = getEntry();
        when(lh.readEntries(anyLong(), anyLong())).thenReturn(entryEnumeration);
        when(lh.getId()).thenReturn(1L);

    }

    private Enumeration<LedgerEntry> getEntry() {
        Vector<LedgerEntry> entries = new Vector<>();
        for (int i = 0; i < 10; i++) {
            LedgerEntry ledgerEntry = mock(LedgerEntry.class);
            String payload = "entry-" + i;
            when(ledgerEntry.getEntry()).thenReturn(payload.getBytes(UTF_8));
            entries.add(ledgerEntry);
        }
        return entries.elements();
    }

    @Test
    public void testDefaultArgs() {
        testSanityCommand("");
    }

    @Test
    public void testEntriesShortArgs() {
        when(lh.getLastAddConfirmed()).thenReturn(0L);
        testSanityCommand("-e", "1");
    }

    @Test
    public void testEntriesLongArgs() {
        when(lh.getLastAddConfirmed()).thenReturn(0L);
        testSanityCommand("--entries", "1");
    }

    @Test
    public void testTimeoutShortArgs() {
        testSanityCommand("-t", "10");
    }

    @Test
    public void testTimeoutLongArgs() {
        testSanityCommand("--timeout", "10");
    }

    public void testSanityCommand(String... args) {
        SanityTestCommand cmd = new SanityTestCommand();
        assertTrue(cmd.apply(bkFlags, args));
    }
}
