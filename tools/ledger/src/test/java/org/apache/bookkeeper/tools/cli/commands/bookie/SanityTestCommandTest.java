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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Enumeration;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.bookie.LocalBookieEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test for sanity command.
 */
public class SanityTestCommandTest extends BookieCommandTestBase {

    private LedgerHandle lh;

    public SanityTestCommandTest() {
        super(3, 1);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        lh = mock(LedgerHandle.class);
        mockClientConfigurationConstruction();
        mockConstruction(BookKeeper.class, (bk, context) -> {
            doAnswer(new Answer<Void>() {
                public Void answer(InvocationOnMock invocation) {
                    ((CreateCallback) invocation.getArguments()[4]).createComplete(BKException.Code.OK, lh,
                            null);
                    return null;
                }
            }).when(bk).asyncCreateLedger(anyInt(), anyInt(), any(BookKeeper.DigestType.class), eq(new byte[0]),
                    any(CreateCallback.class), any());
            doAnswer(new Answer<Void>() {
                public Void answer(InvocationOnMock invocation) {
                    ((OpenCallback) invocation.getArguments()[3]).openComplete(BKException.Code.OK, lh,
                            null);
                    return null;
                }
            }).when(bk).asyncOpenLedger(anyLong(), any(BookKeeper.DigestType.class), eq(new byte[0]),
                    any(OpenCallback.class), any());
        });
        when(lh.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(lh.getLastAddConfirmed()).thenReturn(9L);
        Enumeration<LedgerEntry> entryEnumeration = getEntry();
        when(lh.getId()).thenReturn(1L);

        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                ((ReadCallback) invocation.getArguments()[2]).readComplete(BKException.Code.OK, lh,
                        entryEnumeration, null);
                return null;
            }
        }).when(lh).asyncReadEntries(anyLong(), anyLong(), any(ReadCallback.class), any());
        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                ((AddCallback) invocation.getArguments()[1]).addComplete(BKException.Code.OK, lh,
                        0, null);
                return null;
            }
        }).when(lh).asyncAddEntry(any(byte[].class), any(AddCallback.class), any());
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
        verifyFunc();
    }

    @Test
    public void testEntriesLongArgs() {
        when(lh.getLastAddConfirmed()).thenReturn(0L);
        testSanityCommand("--entries", "1");
        verifyFunc();
    }

    private void verifyFunc() {
        try {
            final ClientConfiguration clientConf =
                    getMockedConstruction(ClientConfiguration.class).constructed().get(0);
            verify(clientConf, times(1)).setAddEntryTimeout(1);
            verify(clientConf, times(1)).setReadEntryTimeout(1);
            verify(lh, times(1)).asyncAddEntry(any(byte[].class), any(AddCallback.class), any());
            verify(lh, times(1)).asyncReadEntries(eq(0L), eq(0L), any(ReadCallback.class), any());
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
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
        try {
            final ClientConfiguration clientConf =
                    getMockedConstruction(ClientConfiguration.class).constructed().get(0);
            verify(clientConf, times(1))
                    .addConfiguration(any(Configuration.class));
            verify(clientConf, times(1))
                    .setEnsemblePlacementPolicy(LocalBookieEnsemblePlacementPolicy.class);
            final BookKeeper bk = getMockedConstruction(BookKeeper.class).constructed().get(0);
            verify(bk, times(1)).asyncCreateLedger(eq(1), eq(1), eq(BookKeeper.DigestType.MAC), eq(new byte[0]),
                    any(CreateCallback.class), any());
            verify(bk, times(1)).asyncOpenLedger(anyLong(), eq(BookKeeper.DigestType.MAC), eq(new byte[0]),
                    any(OpenCallback.class), any());
            verify(lh, times(1)).getLastAddConfirmed();
            verify(bk, times(1)).asyncDeleteLedger(anyLong(), any(DeleteCallback.class), any());
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }
}
