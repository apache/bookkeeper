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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import com.google.common.collect.Lists;

import io.reactivex.rxjava3.schedulers.Schedulers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.MockLedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.Versioned;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for MetadataAsyncIterator.
 */
public class MetadataAsyncIteratorTest {
    private static Logger log = LoggerFactory.getLogger(MetadataAsyncIteratorTest.class);

    private LedgerMetadata newRandomMetadata(long randBit) throws Exception {
        return LedgerMetadataBuilder.create()
            .withId(1)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(1)
            .withWriteQuorumSize(1)
            .withAckQuorumSize(1)
            .newEnsembleEntry(0, Lists.newArrayList(
                                      BookieId.parse("foobar-" + randBit + ":3181")))
            .build();
    }

    private ConcurrentHashMap<Long, LedgerMetadata> addLedgers(LedgerManager lm, int count)
            throws Exception {
        ConcurrentHashMap<Long, LedgerMetadata> added = new ConcurrentHashMap<>();
        for (long i = 0; i < count; i++) {
            LedgerMetadata metadata = newRandomMetadata(i);
            lm.createLedgerMetadata(i, metadata).get();
            added.put(i, metadata);
        }
        return added;
    }

    private static CompletableFuture<Void> removeFromMap(
            ConcurrentHashMap<Long, LedgerMetadata> map,
            long ledgerId, LedgerMetadata metadata) {
        log.debug("removing ledger {}", ledgerId);
        if (map.remove(ledgerId, metadata)) {
            return CompletableFuture.completedFuture(null);
        } else {
            log.debug("ledger {} already removed", ledgerId);
            return FutureUtils.exception(new Exception("ledger already removed"));
        }
    }

    @Test
    public void testIteratorOverAll() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        ConcurrentHashMap<Long, LedgerMetadata> added = addLedgers(lm, 10000);
        MetadataAsyncIterator iterator = new MetadataAsyncIterator(Schedulers.io(),
                                                                   lm, 100 /* inflight */,
                                                                   3 /* timeout */, TimeUnit.SECONDS);
        iterator.forEach((ledgerId, metadata) -> removeFromMap(added, ledgerId, metadata))
            .get(10, TimeUnit.SECONDS);
        assertThat(added.isEmpty(), equalTo(true));
    }

    @Test
    public void testSingleLedger() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        LedgerMetadata single = newRandomMetadata(0xdeadbeef);
        MetadataAsyncIterator iterator = new MetadataAsyncIterator(Schedulers.io(),
                                                                   lm, 100 /* inflight */,
                                                                   3 /* timeout */, TimeUnit.SECONDS);
        iterator.forEach((ledgerId, metadata) -> {
                if (ledgerId == 0xdeadbeef && metadata.equals(single)) {
                    return CompletableFuture.completedFuture(null);
                } else {
                    return FutureUtils.exception(new Exception("Unexpected metadata"));
                }
            }).get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testEmptyRange() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        MetadataAsyncIterator iterator = new MetadataAsyncIterator(Schedulers.io(),
                                                                   lm, 100 /* inflight */,
                                                                   3 /* timeout */, TimeUnit.SECONDS);
        iterator.forEach((ledgerId, metadata) -> FutureUtils.exception(new Exception("Should be empty")))
            .get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testOneLedgerErrorsOnRead() throws Exception {
        MockLedgerManager lm = new MockLedgerManager() {
                @Override
                public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
                    if (ledgerId == 403) {
                        return FutureUtils.exception(new BKException.ZKException());
                    } else {
                        return super.readLedgerMetadata(ledgerId);
                    }
                }
            };
        ConcurrentHashMap<Long, LedgerMetadata> added = addLedgers(lm, 10000);
        MetadataAsyncIterator iterator = new MetadataAsyncIterator(Schedulers.io(),
                                                                   lm, 100 /* inflight */,
                                                                   3 /* timeout */, TimeUnit.SECONDS);
        try {
            iterator.forEach((ledgerId, metadata) -> removeFromMap(added, ledgerId, metadata))
                .get(10, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(BKException.ZKException.class));
        }
    }

    @Test
    public void testOneLedgerErrorsOnProcessing() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        ConcurrentHashMap<Long, LedgerMetadata> added = addLedgers(lm, 10000);
        MetadataAsyncIterator iterator = new MetadataAsyncIterator(Schedulers.io(),
                                                                   lm, 100 /* inflight */,
                                                                   3 /* timeout */, TimeUnit.SECONDS);
        try {
            iterator.forEach((ledgerId, metadata) -> {
                    if (ledgerId == 403) {
                        log.info("IKDEBUG erroring");
                        return FutureUtils.exception(new Exception("foobar"));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }).get(10, TimeUnit.SECONDS);
            Assert.fail("shouldn't succeed");
        } catch (ExecutionException ee) {
            assertThat(ee.getCause().getMessage(), equalTo("foobar"));
        }
    }

    @Test
    public void testAllLedgersErrorOnRead() throws Exception {
        MockLedgerManager lm = new MockLedgerManager() {
                @Override
                public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
                    CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
                    promise.completeExceptionally(new BKException.ZKException());
                    return promise;
                }
            };
        ConcurrentHashMap<Long, LedgerMetadata> added = addLedgers(lm, 10000);
        MetadataAsyncIterator iterator = new MetadataAsyncIterator(Schedulers.io(),
                                                                   lm, 100 /* inflight */,
                                                                   3 /* timeout */, TimeUnit.SECONDS);
        try {
            iterator.forEach((ledgerId, metadata) -> CompletableFuture.completedFuture(null))
                .get(10, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(BKException.ZKException.class));
        }
    }

    @Test
    public void testAllLedgersErrorOnProcessing() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        ConcurrentHashMap<Long, LedgerMetadata> added = addLedgers(lm, 10000);
        MetadataAsyncIterator iterator = new MetadataAsyncIterator(Schedulers.io(),
                                                                   lm, 100 /* inflight */,
                                                                   3 /* timeout */, TimeUnit.SECONDS);
        try {
            iterator.forEach((ledgerId, metadata) -> FutureUtils.exception(new Exception("foobar")))
                .get(10, TimeUnit.SECONDS);
            Assert.fail("shouldn't succeed");
        } catch (ExecutionException ee) {
            assertThat(ee.getCause().getMessage(), equalTo("foobar"));
        }
    }

    @Test
    public void testOneLedgerDisappearsBetweenListAndRead() throws Exception {
        MockLedgerManager lm = new MockLedgerManager() {
                @Override
                public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
                    if (ledgerId == 501) {
                        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
                        promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                        return promise;
                    } else {
                        return super.readLedgerMetadata(ledgerId);
                    }
                }
            };
        ConcurrentHashMap<Long, LedgerMetadata> added = addLedgers(lm, 10000);
        MetadataAsyncIterator iterator = new MetadataAsyncIterator(Schedulers.io(),
                                                                   lm, 100 /* inflight */,
                                                                   3 /* timeout */, TimeUnit.SECONDS);
        iterator.forEach((ledgerId, metadata) -> removeFromMap(added, ledgerId, metadata))
            .get(10, TimeUnit.SECONDS);
        assertThat(added.size(), equalTo(1));
        log.info("IKDEBUG {} {}", added, added.containsKey(5L));
        assertThat(added.containsKey(501L), equalTo(true));
    }

    @Test
    public void testEverySecondLedgerDisappearsBetweenListAndRead() throws Exception {
        MockLedgerManager lm = new MockLedgerManager() {
                @Override
                public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
                    if (ledgerId % 2 == 0) {
                        return FutureUtils.exception(
                                new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                    } else {
                        return super.readLedgerMetadata(ledgerId);
                    }
                }
            };
        int numLedgers = 10000;
        ConcurrentHashMap<Long, LedgerMetadata> added = addLedgers(lm, numLedgers);
        MetadataAsyncIterator iterator = new MetadataAsyncIterator(Schedulers.io(),
                                                                   lm, 100,
                                                                   3, TimeUnit.SECONDS);
        iterator.forEach((ledgerId, metadata) -> removeFromMap(added, ledgerId, metadata))
            .get(10, TimeUnit.SECONDS);
        assertThat(added.size(), equalTo(numLedgers / 2));
        assertThat(added.keySet().stream().allMatch(k -> k % 2 == 0), equalTo(true));
        assertThat(added.keySet().stream().noneMatch(k -> k % 2 == 1), equalTo(true));
    }

    @Test
    public void testEveryLedgerDisappearsBetweenListAndRead() throws Exception {
        MockLedgerManager lm = new MockLedgerManager() {
                @Override
                public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
                    return FutureUtils.exception(
                            new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                }
            };
        int numLedgers = 10000;
        ConcurrentHashMap<Long, LedgerMetadata> added = addLedgers(lm, numLedgers);
        MetadataAsyncIterator iterator = new MetadataAsyncIterator(Schedulers.io(),
                                                                   lm, 100,
                                                                   3, TimeUnit.SECONDS);
        iterator.forEach((ledgerId, metadata) -> removeFromMap(added, ledgerId, metadata))
            .get(10, TimeUnit.SECONDS);
        assertThat(added.size(), equalTo(numLedgers));
    }

    @Test
    public void testMaxOutInFlight() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        int numLedgers = 1000;
        ConcurrentHashMap<Long, LedgerMetadata> added = addLedgers(lm, numLedgers);
        MetadataAsyncIterator iterator = new MetadataAsyncIterator(Schedulers.io(),
                                                                   lm, 10,
                                                                   3, TimeUnit.SECONDS);
        CompletableFuture<Void> blocker = new CompletableFuture<>();
        CompletableFuture<Void> iterFuture = iterator.forEach(
                (ledgerId, metadata) ->
                blocker.thenCompose(ignore -> removeFromMap(added, ledgerId, metadata)));
        assertThat(iterFuture.isDone(), equalTo(false));
        blocker.complete(null);
        iterFuture.get(10, TimeUnit.SECONDS);
        assertThat(added.isEmpty(), equalTo(true));
    }

}
