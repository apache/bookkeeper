/*
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

package org.apache.bookkeeper.metadata.etcd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.coreos.jetcd.Client;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.metadata.etcd.testing.EtcdTestBase;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallbackFuture;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test {@link Etcd64bitIdGenerator}.
 */
@Slf4j
public class Etcd64bitIdGeneratorTest extends EtcdTestBase {

    private String scope;
    private Etcd64bitIdGenerator generator;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.scope = "/" + RandomStringUtils.randomAlphabetic(8);
        this.generator = new Etcd64bitIdGenerator(etcdClient.getKVClient(), scope);
        log.info("Setup id generator under scope {}", scope);
    }

    @Test
    public void testGenerateIdSequence() throws Exception {
        Map<Integer, Long> buckets = new HashMap<>();

        int numIterations = 10;

        for (int i = 0; i < numIterations; i++) {
            log.info("Id generation iteration : {}", i);
            for (int j = 0; j < Etcd64bitIdGenerator.NUM_BUCKETS; j++) {
                GenericCallbackFuture<Long> future = new GenericCallbackFuture<>();
                generator.generateLedgerId(future);
                long lid = future.get();
                int bucketId = Etcd64bitIdGenerator.getBucketId(lid);
                long idInBucket = Etcd64bitIdGenerator.getIdInBucket(lid);
                Long prevIdInBucket = buckets.put(bucketId, idInBucket);
                if (null == prevIdInBucket) {
                    assertEquals(1, idInBucket);
                } else {
                    assertEquals(prevIdInBucket + 1, idInBucket);
                }
            }
        }

        assertEquals(Etcd64bitIdGenerator.NUM_BUCKETS, buckets.size());
        for (Map.Entry<Integer, Long> bucketEntry : buckets.entrySet()) {
            assertEquals(numIterations, bucketEntry.getValue().intValue());
        }
    }

    /**
     * Test generating id in parallel and ensure there is no duplicated id.
     */
    @Test
    public void testGenerateIdParallel() throws Exception {
        final int numThreads = 10;
        @Cleanup("shutdown")
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        final int numIds = 10000;
        final AtomicLong totalIds = new AtomicLong(numIds);
        final Set<Long> ids = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final RateLimiter limiter = RateLimiter.create(1000);
        final CompletableFuture<Void> doneFuture = new CompletableFuture<>();
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                Client client = Client.builder()
                    .endpoints(etcdContainer.getClientEndpoint())
                    .build();
                Etcd64bitIdGenerator gen = new Etcd64bitIdGenerator(
                    client.getKVClient(),
                    scope
                );

                AtomicBoolean running = new AtomicBoolean(true);

                while (running.get()) {
                    limiter.acquire();

                    GenericCallbackFuture<Long> genFuture = new GenericCallbackFuture<>();
                    gen.generateLedgerId(genFuture);

                    genFuture
                        .thenAccept(lid -> {
                            boolean duplicatedFound = !(ids.add(lid));
                            if (duplicatedFound) {
                                running.set(false);
                                doneFuture.completeExceptionally(
                                    new IllegalStateException("Duplicated id " + lid + " generated : " + ids));
                                return;
                            } else {
                                if (totalIds.decrementAndGet() <= 0) {
                                    running.set(false);
                                    doneFuture.complete(null);
                                }
                            }
                        })
                        .exceptionally(cause -> {
                            running.set(false);
                            doneFuture.completeExceptionally(cause);
                            return null;
                        });
                }
            });
        }

        FutureUtils.result(doneFuture);
        assertTrue(totalIds.get() <= 0);
        assertTrue(ids.size() >= numIds);
    }

}
