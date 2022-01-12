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

package org.apache.bookkeeper.bookie.datainteg;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for DataIntegrityService.
 */
public class DataIntegrityServiceTest {
    private static DataIntegrityService newLowIntervalService(DataIntegrityCheck check) {
        return new DataIntegrityService(
                new BookieConfiguration(new ServerConfiguration()),
                NullStatsLogger.INSTANCE, check) {
            @Override
            public int interval() {
                return 1;
            }
            @Override
            public TimeUnit intervalUnit() {
                return TimeUnit.MICROSECONDS;
            }
        };
    }

    @Test
    public void testFullCheckRunsIfRequested() throws Exception {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        MockDataIntegrityCheck check = new MockDataIntegrityCheck() {
                @Override
                public boolean needsFullCheck() {
                    return true;
                }
                @Override
                public CompletableFuture<Void> runFullCheck() {
                    promise.complete(null);
                    return super.runFullCheck();
                }
            };
        DataIntegrityService service = newLowIntervalService(check);
        try {
            service.start();

            promise.get(5, TimeUnit.SECONDS);
        } finally {
            service.stop();
        }
    }

    @Test
    public void testFullCheckDoesntRunIfNotRequested() throws Exception {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        MockDataIntegrityCheck check = new MockDataIntegrityCheck() {
                @Override
                public boolean needsFullCheck() {
                    return false;
                }
                @Override
                public CompletableFuture<Void> runFullCheck() {
                    promise.complete(null);
                    return super.runFullCheck();
                }
            };
        DataIntegrityService service = newLowIntervalService(check);
        try {
            service.start();

            try {
                // timeout set very low, so hard to tell if
                // it's waiting to run or not running, but it
                // should be the latter on any modern machine
                promise.get(100, TimeUnit.MILLISECONDS);
                Assert.fail("Shouldn't have run");
            } catch (TimeoutException te) {
                // expected
            }
        } finally {
            service.stop();
        }
    }

    @Test
    public void testFullCheckRunsMultipleTimes() throws Exception {
        AtomicInteger count = new AtomicInteger(0);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        MockDataIntegrityCheck check = new MockDataIntegrityCheck() {
                @Override
                public boolean needsFullCheck() {
                    return true;
                }
                @Override
                public CompletableFuture<Void> runFullCheck() {
                    if (count.incrementAndGet() == 10) {
                        promise.complete(null);
                    }
                    return super.runFullCheck();
                }
            };
        DataIntegrityService service = newLowIntervalService(check);
        try {
            service.start();

            promise.get(10, TimeUnit.SECONDS);
        } finally {
            service.stop();
        }
    }

    @Test
    public void testRunDontRunThenRunAgain() throws Exception {
        AtomicBoolean needsFullCheck = new AtomicBoolean(true);
        Semaphore semaphore = new Semaphore(1);
        semaphore.acquire(); // increment the count, can only be released by a check
        MockDataIntegrityCheck check = new MockDataIntegrityCheck() {
                @Override
                public boolean needsFullCheck() {
                    return needsFullCheck.getAndSet(false);
                }
                @Override
                public CompletableFuture<Void> runFullCheck() {
                    semaphore.release();
                    return super.runFullCheck();
                }
            };
        DataIntegrityService service = newLowIntervalService(check);
        try {
            service.start();

            Assert.assertTrue("Check should have run",
                              semaphore.tryAcquire(10, TimeUnit.SECONDS));
            Assert.assertFalse("Check shouldn't run again",
                               semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
            needsFullCheck.set(true);
            Assert.assertTrue("Check should run again",
                              semaphore.tryAcquire(10, TimeUnit.SECONDS));
        } finally {
            service.stop();
        }
    }
}
