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
package org.apache.bookkeeper.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerAuditorManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLedgerAuditorManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test verifies the auditor bookie scenarios which will be monitoring the
 * bookie failures.
 */
public class AuditorBookieTest extends BookKeeperClusterTestCase {
    // Depending on the taste, select the amount of logging
    // by decommenting one of the two lines below
    // private static final Logger LOG = Logger.getRootLogger();
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorBookieTest.class);
    private String electionPath;
    private HashMap<String, AuditorElector> auditorElectors = new HashMap<String, AuditorElector>();
    private List<ZooKeeper> zkClients = new LinkedList<ZooKeeper>();

    public AuditorBookieTest() {
        super(6);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        startAuditorElectors();

        electionPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf)
            + "/underreplication/auditorelection";
    }

    @Override
    public void tearDown() throws Exception {
        stopAuditorElectors();
        for (ZooKeeper zk : zkClients) {
            zk.close();
        }
        zkClients.clear();
        super.tearDown();
    }

    /**
     * Test should ensure only one should act as Auditor. Starting/shutdown
     * other than auditor bookie shouldn't initiate re-election and multiple
     * auditors.
     */
    @Test
    public void testEnsureOnlySingleAuditor() throws Exception {
        BookieServer auditor = verifyAuditor();

        // shutdown bookie which is not an auditor
        int indexOf = indexOfServer(auditor);
        int bkIndexDownBookie;
        if (indexOf < lastBookieIndex()) {
            bkIndexDownBookie = indexOf + 1;
        } else {
            bkIndexDownBookie = indexOf - 1;
        }
        shutdownBookie(serverByIndex(bkIndexDownBookie));

        startNewBookie();
        startNewBookie();
        // grace period for the auditor re-election if any
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);
    }

    /**
     * Test Auditor crashes should trigger re-election and another bookie should
     * take over the auditor ship.
     */
    @Test
    public void testSuccessiveAuditorCrashes() throws Exception {
        BookieServer auditor = verifyAuditor();
        shutdownBookie(auditor);

        BookieServer newAuditor1 = waitForNewAuditor(auditor);
        shutdownBookie(newAuditor1);
        BookieServer newAuditor2 = waitForNewAuditor(newAuditor1);
        assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor2);
    }

    /**
     * Test restarting the entire bookie cluster. It shouldn't create multiple
     * bookie auditors.
     */
    @Test
    public void testBookieClusterRestart() throws Exception {
        BookieServer auditor = verifyAuditor();
        for (AuditorElector auditorElector : auditorElectors.values()) {
            assertTrue("Auditor elector is not running!", auditorElector
                    .isRunning());
        }
        stopBKCluster();
        stopAuditorElectors();

        startBKCluster(zkUtil.getMetadataServiceUri());
        startAuditorElectors();
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);
    }

    /**
     * Test the vote is deleting from the ZooKeeper during shutdown.
     */
    @Test
    public void testShutdown() throws Exception {
        BookieServer auditor = verifyAuditor();
        shutdownBookie(auditor);

        // waiting for new auditor
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);

        List<String> children = zkc.getChildren(electionPath, false);
        for (String child : children) {
            byte[] data = zkc.getData(electionPath + '/' + child, false, null);
            String bookieIP = new String(data);
            String addr = auditor.getBookieId().toString();
            assertFalse("AuditorElection cleanup fails", bookieIP
                    .contains(addr));
        }
    }

    /**
     * Test that shutting down the elector of a bookie that is not the auditor
     * doesn't wait for its election, which only ends when the bookie becomes
     * the auditor.
     */
    @Test
    public void testShutdownFollowerElector() throws Exception {
        BookieServer auditor = verifyAuditor();
        String addr = addressByIndex(indexOfServer(auditor) == 0 ? 1 : 0).toString();
        AuditorElector follower = auditorElectors.get(addr);

        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(() -> {
            try {
                follower.shutdown();
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
        });
        // well below the 10 seconds shutdown() gives the election before interrupting it
        shutdown.get(5, TimeUnit.SECONDS);

        assertFalse("Vote of the stopped elector is still there", hasVote(addr));
        assertSame("Auditor changed", auditor, verifyAuditor());
    }

    /**
     * Test that closing the auditor manager of a candidate waiting for its turn
     * ends the wait, and the candidate doesn't become the auditor.
     */
    @Test
    public void testCloseReleasesWaitingCandidate() throws Exception {
        verifyAuditor();
        String candidate = "waiting-candidate";
        LedgerAuditorManager lam = bkc.getLedgerManagerFactory().newLedgerAuditorManager();
        CompletableFuture<Void> election = CompletableFuture.runAsync(() -> {
            try {
                lam.tryToBecomeAuditor(candidate, e -> {});
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
        Awaitility.await().until(() -> hasVote(candidate));

        lam.close();
        try {
            election.get(5, TimeUnit.SECONDS);
            fail("A closed candidate became the auditor");
        } catch (ExecutionException e) {
            assertTrue("Unexpected failure: " + e.getCause(), e.getCause() instanceof IllegalStateException);
        }
        assertFalse("Vote of the closed candidate is still there", hasVote(candidate));
    }

    /**
     * Test that a candidate whose auditor manager closes while it is elected
     * neither publishes itself as the auditor nor reports that it became the auditor.
     */
    @Test
    public void testCloseWhileCandidateIsElected() throws Exception {
        stopAuditorElectors();
        String candidate = "closing-candidate";
        // the candidate waits for the close once it listed the votes, its own first
        CountDownLatch listed = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        ZooKeeper zk = spy(zkc);
        doAnswer(invocation -> {
            Object votes = invocation.callRealMethod();
            listed.countDown();
            closed.await();
            return votes;
        }).when(zk).getChildren(electionPath, false);
        LedgerAuditorManager lam = new ZkLedgerAuditorManager(zk, baseConf, NullStatsLogger.INSTANCE);
        CompletableFuture<Void> election = CompletableFuture.runAsync(() -> {
            try {
                lam.tryToBecomeAuditor(candidate, e -> {});
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
        assertTrue(listed.await(10, TimeUnit.SECONDS));

        lam.close();
        closed.countDown();
        try {
            election.get(5, TimeUnit.SECONDS);
            fail("A closed candidate became the auditor");
        } catch (ExecutionException e) {
            assertTrue("Unexpected failure: " + e.getCause(), e.getCause() instanceof IllegalStateException);
        }
        assertFalse("A closed candidate published itself as the auditor",
                new String(zkc.getData(electionPath, false, null)).contains(candidate));
    }

    /**
     * Test that an interrupted shutdown of the auditor's elector still stops the
     * auditor and deletes the vote, then propagates the interruption.
     */
    @Test
    public void testInterruptedShutdownStopsAuditor() throws Exception {
        String addr = verifyAuditor().getBookieId().toString();
        AuditorElector elector = auditorElectors.get(addr);
        Awaitility.await().until(() -> elector.getAuditor() != null);
        Auditor auditor = elector.getAuditor();

        CompletableFuture<Throwable> shutdown = new CompletableFuture<>();
        new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                elector.shutdown();
                shutdown.complete(null);
            } catch (Throwable t) {
                shutdown.complete(t);
            }
        }).start();
        Throwable failure = shutdown.get(30, TimeUnit.SECONDS);
        assertTrue("Unexpected result: " + failure, failure instanceof InterruptedException);

        assertFalse("Auditor still running", auditor.isRunning());
        Awaitility.await().until(() -> !hasVote(addr));
    }

    /**
     * Test that interrupting a shutdown that waits for the auditor manager to close
     * interrupts the close too.
     */
    @Test
    public void testInterruptedShutdownInterruptsManagerClose() throws Exception {
        LedgerAuditorManager lam = mock(LedgerAuditorManager.class);
        // a bookie that is not the auditor: its election waits until the shutdown interrupts it
        doAnswer(invocation -> {
            new CountDownLatch(1).await();
            return null;
        }).when(lam).tryToBecomeAuditor(any(), any());
        // the close waits like for an unreachable ZooKeeper, until it is interrupted
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        CompletableFuture<Void> closeInterrupted = new CompletableFuture<>();
        doAnswer(invocation -> {
            closing.countDown();
            try {
                releaseClose.await();
            } catch (InterruptedException e) {
                closeInterrupted.complete(null);
                throw e;
            }
            return null;
        }).when(lam).close();
        LedgerManagerFactory lmf = mock(LedgerManagerFactory.class);
        when(lmf.newLedgerAuditorManager()).thenReturn(lam);
        BookKeeper client = mock(BookKeeper.class);
        when(client.getLedgerManagerFactory()).thenReturn(lmf);
        AuditorElector elector = new AuditorElector("blocked-close", baseConf, client, false);
        elector.start();

        CompletableFuture<Throwable> shutdown = new CompletableFuture<>();
        Thread shutdownThread = new Thread(() -> {
            try {
                elector.shutdown();
                shutdown.complete(null);
            } catch (Throwable t) {
                shutdown.complete(t);
            }
        });
        try {
            shutdownThread.start();
            assertTrue(closing.await(10, TimeUnit.SECONDS));
            shutdownThread.interrupt();

            Throwable failure = shutdown.get(5, TimeUnit.SECONDS);
            assertTrue("Unexpected result: " + failure, failure instanceof InterruptedException);
            // well below the 10 seconds after which shutdown() interrupts the close anyway
            closeInterrupted.get(5, TimeUnit.SECONDS);
        } finally {
            releaseClose.countDown();
        }
    }

    /**
     * Test restart of the previous Auditor bookie shouldn't initiate
     * re-election and should create new vote after restarting.
     */
    @Test
    public void testRestartAuditorBookieAfterCrashing() throws Exception {
        BookieServer auditor = verifyAuditor();

        String addr = auditor.getBookieId().toString();

        // restarting Bookie with same configurations.
        ServerConfiguration serverConfiguration = shutdownBookie(auditor);

        auditorElectors.remove(addr);
        startBookie(serverConfiguration);
        // starting corresponding auditor elector

        if (LOG.isDebugEnabled()) {
            LOG.debug("Performing Auditor Election:" + addr);
        }
        startAuditorElector(addr);

        // waiting for new auditor to come
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);
        assertFalse("No relection after old auditor rejoins", auditor
                .getBookieId().equals(newAuditor.getBookieId()));
    }

    private void startAuditorElector(String addr) throws Exception {
        AuditorElector auditorElector = new AuditorElector(addr,
                                                           baseConf);
        auditorElectors.put(addr, auditorElector);
        auditorElector.start();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Starting Auditor Elector");
        }
    }

    private void startAuditorElectors() throws Exception {
        for (BookieId addr : bookieAddresses()) {
            startAuditorElector(addr.toString());
        }
    }

    private void stopAuditorElectors() throws Exception {
        for (AuditorElector auditorElector : auditorElectors.values()) {
            auditorElector.shutdown();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Stopping Auditor Elector!");
            }
        }
    }

    private BookieServer verifyAuditor() throws Exception {
        List<BookieServer> auditors = getAuditorBookie();
        assertEquals("Multiple Bookies acting as Auditor!", 1, auditors
                .size());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Bookie running as Auditor:" + auditors.get(0));
        }
        return auditors.get(0);
    }

    private List<BookieServer> getAuditorBookie() throws Exception {
        List<BookieServer> auditors = new LinkedList<BookieServer>();
        byte[] data = zkc.getData(electionPath, false, null);
        assertNotNull("Auditor election failed", data);
        for (int i = 0; i < bookieCount(); i++) {
            BookieServer bks = serverByIndex(i);
            if (new String(data).contains(bks.getBookieId() + "")) {
                auditors.add(bks);
            }
        }
        return auditors;
    }

    private boolean hasVote(String bookieId) throws Exception {
        for (String child : zkc.getChildren(electionPath, false)) {
            byte[] data = zkc.getData(electionPath + '/' + child, false, null);
            if (new String(data).contains(bookieId)) {
                return true;
            }
        }
        return false;
    }

    private ServerConfiguration shutdownBookie(BookieServer bkServer) throws Exception {
        int index = indexOfServer(bkServer);
        String addr = addressByIndex(index).toString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Shutting down bookie:" + addr);
        }

        // shutdown bookie which is an auditor
        ServerConfiguration conf = killBookie(index);

        // stopping corresponding auditor elector
        auditorElectors.get(addr).shutdown();
        return conf;
    }

    private BookieServer waitForNewAuditor(BookieServer auditor)
            throws Exception {
        BookieServer newAuditor = null;
        int retryCount = 8;
        while (retryCount > 0) {
            try {
                List<BookieServer> auditors = getAuditorBookie();
                if (auditors.size() > 0) {
                    newAuditor = auditors.get(0);
                    if (auditor != newAuditor) {
                        break;
                    }
                }
            } catch (Exception ignore) {
            }
            Thread.sleep(500);
            retryCount--;
        }
        assertNotNull(
                "New Auditor is not reelected after auditor crashes",
                newAuditor);
        verifyAuditor();
        return newAuditor;
    }
}
