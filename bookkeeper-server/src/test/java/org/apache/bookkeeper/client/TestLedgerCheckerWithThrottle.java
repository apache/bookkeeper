/**
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
package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.common.util.concurrent.RateLimiter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.Semaphore;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieClient;
import org.junit.Test;

/**
 * Tests the functionality of LedgerChecker with throttle.
 */
public class TestLedgerCheckerWithThrottle {

    @Test
    public void testThrottle() throws Exception {
        ServerConfiguration conf;
        LedgerChecker checker;
        BookieClient bookieClientMock = mock(BookieClient.class);
        BookieWatcher bookieWatcherMock = mock(BookieWatcher.class);

        Field semaphoreField = LedgerChecker.class.getDeclaredField("semaphore");
        semaphoreField.setAccessible(true);

        Field rateLimiterField = LedgerChecker.class.getDeclaredField("rateLimiter");
        rateLimiterField.setAccessible(true);

        Method acquirePermitMethod = LedgerChecker.class.getDeclaredMethod("acquirePermit");
        acquirePermitMethod.setAccessible(true);

        Method releasePermitMethod = LedgerChecker.class.getDeclaredMethod("releasePermit");
        releasePermitMethod.setAccessible(true);

        conf = spy(new ServerConfiguration());
        doReturn(-1).when(conf).getInFlightReadEntryNumInLedgerChecker();
        doReturn(-1).when(conf).getReadEntryRateInLedgerChecker();
        checker = new LedgerChecker(bookieClientMock, bookieWatcherMock, conf);
        assertNull(semaphoreField.get(checker));
        assertNull(rateLimiterField.get(checker));

        conf = spy(new ServerConfiguration());
        doReturn(20).when(conf).getInFlightReadEntryNumInLedgerChecker();
        doReturn(10).when(conf).getReadEntryRateInLedgerChecker();
        checker = new LedgerChecker(bookieClientMock, bookieWatcherMock, conf);
        assertNull(semaphoreField.get(checker));
        assertNotNull(rateLimiterField.get(checker));
        RateLimiter rateLimiter = (RateLimiter) rateLimiterField.get(checker);
        assertEquals(10.0, rateLimiter.getRate(), 0.001);

        conf = spy(new ServerConfiguration());
        doReturn(20).when(conf).getInFlightReadEntryNumInLedgerChecker();
        doReturn(-1).when(conf).getReadEntryRateInLedgerChecker();
        checker = new LedgerChecker(bookieClientMock, bookieWatcherMock, conf);
        assertNotNull(semaphoreField.get(checker));
        assertNull(rateLimiterField.get(checker));
        Semaphore semaphore = (Semaphore) semaphoreField.get(checker);
        assertEquals(20, semaphore.availablePermits());

        acquirePermitMethod.invoke(checker);
        assertEquals(19, semaphore.availablePermits());

        releasePermitMethod.invoke(checker);
        assertEquals(20, semaphore.availablePermits());
    }

}
