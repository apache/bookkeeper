/*
 *
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
 *
 */
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.powermock.reflect.Whitebox;

/**
 * Unit test for {@link GarbageCollectorThread}.
 */
public class GarbageCollectorThreadTest {

    @InjectMocks
    @Spy
    private GarbageCollectorThread mockGCThread;

    @Mock
    private LedgerManager ledgerManager;
    @Mock
    private StatsLogger statsLogger;
    @Mock
    private ScheduledExecutorService gcExecutor;

    private ServerConfiguration conf = spy(new ServerConfiguration());
    private CompactableLedgerStorage ledgerStorage = mock(CompactableLedgerStorage.class);

    @Before
    public void setUp() throws Exception {
        when(ledgerStorage.getEntryLogger()).thenReturn(mock(EntryLogger.class));
        initMocks(this);
    }

    @Test
    public void testCompactEntryLogWithException() throws Exception {
        AbstractLogCompactor mockCompactor = mock(AbstractLogCompactor.class);
        when(mockCompactor.compact(any(EntryLogMetadata.class)))
                .thenThrow(new RuntimeException("Unexpected compaction error"));
        Whitebox.setInternalState(mockGCThread, "compactor", mockCompactor);

        // Although compaction of an entry log fails due to an unexpected error,
        // the `compacting` flag should return to false
        AtomicBoolean compacting = Whitebox.getInternalState(mockGCThread, "compacting");
        assertFalse(compacting.get());
        mockGCThread.compactEntryLog(new EntryLogMetadata(9999));
        assertFalse(compacting.get());
    }
}
