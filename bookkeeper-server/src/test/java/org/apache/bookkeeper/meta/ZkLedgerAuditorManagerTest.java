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
package org.apache.bookkeeper.meta;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests election cancellation without requiring a ZooKeeper server or predecessor deletion.
 */
public class ZkLedgerAuditorManagerTest {
    private static final String BASE_PATH = "/ledgers/underreplication";
    private static final String ELECTION_PATH = BASE_PATH + "/auditorelection";
    private static final String VOTE_PREFIX = ELECTION_PATH + "/V_";
    private static final String MY_VOTE = VOTE_PREFIX + "0000000001";
    private static final String PREDECESSOR = VOTE_PREFIX + "0000000000";

    private ZooKeeper zkc;
    private ZkLedgerAuditorManager manager;
    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        zkc = Mockito.mock(ZooKeeper.class);
        ServerConfiguration conf = new ServerConfiguration()
                .setMetadataServiceUri("zk://127.0.0.1/ledgers");
        manager = new ZkLedgerAuditorManager(zkc, conf, NullStatsLogger.INSTANCE);
        executor = Executors.newCachedThreadPool();
        when(zkc.exists(BASE_PATH, false)).thenReturn(new Stat());
        when(zkc.exists(ELECTION_PATH, false)).thenReturn(new Stat());
        when(zkc.create(eq(VOTE_PREFIX), any(byte[].class), anyList(), eq(CreateMode.EPHEMERAL_SEQUENTIAL)))
                .thenReturn(MY_VOTE);
        when(zkc.getChildren(ELECTION_PATH, false))
                .thenAnswer(invocation -> new ArrayList<>(Arrays.asList("V_0000000000", "V_0000000001")));
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
        assertTrue("Election test threads did not terminate", executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test(timeout = 10000)
    public void testCloseReleasesFollowerWait() throws Exception {
        CountDownLatch watchRegistered = new CountDownLatch(1);
        when(zkc.exists(eq(PREDECESSOR), any(Watcher.class))).thenAnswer(invocation -> {
            watchRegistered.countDown();
            return new Stat();
        });
        Future<?> election = elect();
        await(watchRegistered);

        manager.close();
        election.get(5, TimeUnit.SECONDS);

        verify(zkc).delete(MY_VOTE, -1);
        verify(zkc, never()).setData(eq(ELECTION_PATH), any(byte[].class), eq(-1));
    }

    @Test(timeout = 10000)
    public void testCloseBeforeFollowerWaitRegistration() throws Exception {
        CountDownLatch listingEntered = new CountDownLatch(1);
        CountDownLatch finishListing = new CountDownLatch(1);
        when(zkc.getChildren(ELECTION_PATH, false)).thenAnswer(invocation -> {
            listingEntered.countDown();
            await(finishListing);
            return new ArrayList<>(Arrays.asList("V_0000000000", "V_0000000001"));
        });
        Future<?> election = elect();
        await(listingEntered);

        try {
            manager.close();
        } finally {
            finishListing.countDown();
        }
        election.get(5, TimeUnit.SECONDS);

        verify(zkc).delete(MY_VOTE, -1);
        verify(zkc, never()).exists(eq(PREDECESSOR), any(Watcher.class));
    }

    @Test(timeout = 10000)
    public void testCloseBeforeVoteCreation() throws Exception {
        CountDownLatch initializationEntered = new CountDownLatch(1);
        CountDownLatch finishInitialization = new CountDownLatch(1);
        when(zkc.exists(BASE_PATH, false)).thenAnswer(invocation -> {
            initializationEntered.countDown();
            await(finishInitialization);
            return new Stat();
        });
        Future<?> election = elect();
        await(initializationEntered);

        try {
            manager.close();
        } finally {
            finishInitialization.countDown();
        }
        election.get(5, TimeUnit.SECONDS);

        verify(zkc, never()).create(eq(VOTE_PREFIX), any(byte[].class), anyList(),
                eq(CreateMode.EPHEMERAL_SEQUENTIAL));
    }

    @Test(timeout = 10000)
    public void testCloseDeletesVoteWhoseCreationIsInProgress() throws Exception {
        CountDownLatch creationEntered = new CountDownLatch(1);
        CountDownLatch finishCreation = new CountDownLatch(1);
        when(zkc.create(eq(VOTE_PREFIX), any(byte[].class), anyList(), eq(CreateMode.EPHEMERAL_SEQUENTIAL)))
                .thenAnswer(invocation -> {
                    creationEntered.countDown();
                    await(finishCreation);
                    return MY_VOTE;
                });
        // Allow the election to wait if it runs ahead of the closing thread after creation.
        when(zkc.exists(eq(PREDECESSOR), any(Watcher.class))).thenReturn(new Stat());
        Future<?> election = elect();
        await(creationEntered);
        Future<?> closing = executor.submit(() -> {
            manager.close();
            return null;
        });

        try {
            closing.get(200, TimeUnit.MILLISECONDS);
            fail("Close must wait for the admitted vote creation before deleting its vote");
        } catch (TimeoutException expected) {
            // Vote creation owns the lifecycle lock until its path can be deleted by close.
        } finally {
            finishCreation.countDown();
        }
        closing.get(5, TimeUnit.SECONDS);
        election.get(5, TimeUnit.SECONDS);

        verify(zkc).delete(MY_VOTE, -1);
    }

    @Test(timeout = 10000)
    public void testElectionAfterCloseDoesNotAccessZooKeeper() throws Exception {
        manager.close();

        manager.tryToBecomeAuditor("bookie", event -> { });

        verifyNoInteractions(zkc);
    }

    private Future<?> elect() {
        return executor.submit(() -> {
            manager.tryToBecomeAuditor("bookie", event -> { });
            return null;
        });
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue("Timed out waiting for election test barrier", latch.await(5, TimeUnit.SECONDS));
    }
}
