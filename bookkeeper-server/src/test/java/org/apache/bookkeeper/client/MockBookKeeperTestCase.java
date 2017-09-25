/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import com.google.common.base.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.junit.After;
import org.junit.Before;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for Mock-based Client testcases
 */
public abstract class MockBookKeeperTestCase {

    ScheduledExecutorService scheduler;
    OrderedSafeExecutor executor;
    BookKeeper bk;
    BookieWatcher bookieWatcher;
    BookieClient bookieClient;
    LedgerManager ledgerManager;
    LedgerIdGenerator ledgerIdGenerator;

    @Before
    public void setup() {
        scheduler = Executors.newScheduledThreadPool(4);
        executor = OrderedSafeExecutor.newBuilder().build();
        bookieWatcher = mock(BookieWatcher.class);

        bookieClient = mock(BookieClient.class);
        ledgerManager = mock(LedgerManager.class);
        ledgerIdGenerator = mock(LedgerIdGenerator.class);

        bk = mock(BookKeeper.class);
        NullStatsLogger nullStatsLogger = new NullStatsLogger();
        when(bk.getOpenOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getRecoverOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getReadOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getDeleteOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getCreateOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getCloseLock()).thenReturn(new ReentrantReadWriteLock());
        when(bk.getRecoverAddCountLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getRecoverReadCountLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.isClosed()).thenReturn(false);
        when(bk.getBookieWatcher()).thenReturn(bookieWatcher);
        when(bk.getExplicitLacInterval()).thenReturn(0);
        when(bk.getMainWorkerPool()).thenReturn(executor);
        when(bk.getBookieClient()).thenReturn(bookieClient);
        when(bk.getScheduler()).thenReturn(scheduler);
        when(bk.getReadSpeculativeRequestPolicy()).thenReturn(Optional.absent());
        when(bk.getConf()).thenReturn(new ClientConfiguration());
        when(bk.getStatsLogger()).thenReturn(nullStatsLogger);
        when(bk.getLedgerManager()).thenReturn(ledgerManager);
        when(bk.getLedgerIdGenerator()).thenReturn(ledgerIdGenerator);
    }

    @After
    public void tearDown() {
        scheduler.shutdown();
        executor.shutdown();
    }
}
