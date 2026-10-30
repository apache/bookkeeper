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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerAuditorManager;
import org.apache.bookkeeper.meta.LedgerAuditorManager.AuditorEvent;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for election and shutdown lifecycle ordering without a metadata service.
 */
public class AuditorElectorTest {
    private static final String BOOKIE_ID = "127.0.0.1:3181";
    private static final int TIMEOUT_SECONDS = 5;

    private final List<CountDownLatch> releaseLatches = new ArrayList<>();
    private final List<ShutdownCall> shutdownCalls = new ArrayList<>();
    private BookKeeper bkc;
    private LedgerAuditorManager manager;
    private Auditor auditor;
    private AuditorElector elector;

    @Before
    public void setUp() throws Exception {
        bkc = mock(BookKeeper.class);
        manager = mock(LedgerAuditorManager.class);
        auditor = mock(Auditor.class);
        LedgerManagerFactory factory = mock(LedgerManagerFactory.class);
        when(bkc.getLedgerManagerFactory()).thenReturn(factory);
        when(factory.newLedgerAuditorManager()).thenReturn(manager);
    }

    @After
    public void tearDown() throws Exception {
        releaseLatches.forEach(CountDownLatch::countDown);
        if (elector != null) {
            startShutdown().assertSuccessful();
        }
        for (ShutdownCall call : shutdownCalls) {
            call.awaitCompletion();
        }
    }

    @Test(timeout = 15000)
    public void testShutdownInterruptsFollowerAndWaitsForElectionExit() throws Exception {
        createElector(true);
        CountDownLatch electionEntered = new CountDownLatch(1);
        CountDownLatch electionExited = new CountDownLatch(1);
        CountDownLatch electionInterrupted = new CountDownLatch(1);
        CountDownLatch electionRelease = releaseLatch();
        AtomicBoolean clientClosedBeforeElectionExit = new AtomicBoolean();
        doAnswer(invocation -> {
            electionEntered.countDown();
            try {
                electionRelease.await();
            } catch (InterruptedException e) {
                electionInterrupted.countDown();
                throw e;
            } finally {
                electionExited.countDown();
            }
            return null;
        }).when(manager).tryToBecomeAuditor(eq(BOOKIE_ID), any());
        doAnswer(invocation -> {
            clientClosedBeforeElectionExit.set(electionExited.getCount() != 0);
            return null;
        }).when(bkc).close();

        elector.start();
        await(electionEntered);
        ShutdownCall shutdown = startShutdown();
        assertTrue("Shutdown should promptly interrupt the election wait",
                electionInterrupted.await(2, TimeUnit.SECONDS));
        shutdown.assertSuccessful();

        assertFalse(clientClosedBeforeElectionExit.get());
        verify(elector, never()).createAuditor();
        verify(manager).close();
        verify(bkc).close();
        assertFalse(elector.isRunning());
    }

    @Test(timeout = 15000)
    public void testManagerCloseCanWakeFollowerIndependentlyOfElectionWorker() throws Exception {
        createElector(true);
        CountDownLatch electionEntered = new CountDownLatch(1);
        CountDownLatch electionRelease = releaseLatch();
        CountDownLatch managerClosed = new CountDownLatch(1);
        doAnswer(invocation -> {
            electionEntered.countDown();
            awaitUninterruptibly(electionRelease, new AtomicBoolean());
            return null;
        }).when(manager).tryToBecomeAuditor(eq(BOOKIE_ID), any());
        doAnswer(invocation -> {
            managerClosed.countDown();
            electionRelease.countDown();
            return null;
        }).when(manager).close();

        elector.start();
        await(electionEntered);
        ShutdownCall shutdown = startShutdown();
        assertTrue("Manager close must not queue behind the follower wait",
                managerClosed.await(2, TimeUnit.SECONDS));
        shutdown.assertSuccessful();

        verify(elector, never()).createAuditor();
        verify(bkc).close();
    }

    @Test(timeout = 15000)
    public void testManagerCloseFinishesBeforeElectionInterruptFallback() throws Exception {
        createElector(true);
        CountDownLatch electionEntered = new CountDownLatch(1);
        CountDownLatch electionRelease = releaseLatch();
        CountDownLatch electionInterrupted = new CountDownLatch(1);
        CountDownLatch closeEntered = new CountDownLatch(1);
        CountDownLatch closeRelease = releaseLatch();
        doAnswer(invocation -> {
            electionEntered.countDown();
            try {
                electionRelease.await();
            } catch (InterruptedException e) {
                electionInterrupted.countDown();
                throw e;
            }
            return null;
        }).when(manager).tryToBecomeAuditor(eq(BOOKIE_ID), any());
        doAnswer(invocation -> {
            closeEntered.countDown();
            awaitUninterruptibly(closeRelease, new AtomicBoolean());
            return null;
        }).when(manager).close();

        elector.start();
        await(electionEntered);
        ShutdownCall shutdown = startShutdown();
        await(closeEntered);
        assertFalse("Election must not be interrupted while manager close is still running",
                electionInterrupted.await(100, TimeUnit.MILLISECONDS));
        shutdown.assertPending();
        closeRelease.countDown();
        assertTrue("Fallback interruption should cancel a wait left after manager close",
                electionInterrupted.await(2, TimeUnit.SECONDS));
        shutdown.assertSuccessful();

        verify(elector, never()).createAuditor();
        verify(manager).close();
        verify(bkc).close();
    }

    @Test(timeout = 15000)
    public void testElectionReturningAfterStoppingCannotConstructAuditor() throws Exception {
        createElector(true);
        CountDownLatch electionEntered = new CountDownLatch(1);
        CountDownLatch electionRelease = releaseLatch();
        CountDownLatch managerClosed = signalManagerClose();
        doAnswer(invocation -> {
            electionEntered.countDown();
            awaitUninterruptibly(electionRelease, new AtomicBoolean());
            return null;
        }).when(manager).tryToBecomeAuditor(eq(BOOKIE_ID), any());

        Future<?> election = elector.start();
        await(electionEntered);
        ShutdownCall shutdown = startShutdown();
        await(managerClosed);
        shutdown.assertPending();
        verify(bkc, never()).close();
        electionRelease.countDown();
        shutdown.assertSuccessful();
        election.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        verify(elector, never()).createAuditor();
        verify(auditor, never()).start();
    }

    @Test(timeout = 15000)
    public void testShutdownDuringConstructionWaitsAndSkipsStartup() throws Exception {
        createElector(true);
        CountDownLatch constructionEntered = new CountDownLatch(1);
        CountDownLatch constructionRelease = releaseLatch();
        CountDownLatch managerClosed = signalManagerClose();
        AtomicBoolean constructionInterrupted = new AtomicBoolean();
        doAnswer(invocation -> {
            constructionEntered.countDown();
            awaitUninterruptibly(constructionRelease, constructionInterrupted);
            return auditor;
        }).when(elector).createAuditor();

        Future<?> election = elector.start();
        await(constructionEntered);
        ShutdownCall shutdown = startShutdown();
        await(managerClosed);
        shutdown.assertPending();
        verify(auditor, never()).shutdown();
        verify(bkc, never()).close();
        constructionRelease.countDown();
        shutdown.assertSuccessful();
        election.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertFalse("Admitted construction must not be interrupted", constructionInterrupted.get());
        verify(auditor, never()).start();
        verify(auditor).shutdown();
        verify(bkc).close();
    }

    @Test(timeout = 15000)
    public void testShutdownDuringStartupWaitsBeforeClosingResources() throws Exception {
        createElector(true);
        CountDownLatch startupEntered = new CountDownLatch(1);
        CountDownLatch startupRelease = releaseLatch();
        CountDownLatch managerClosed = signalManagerClose();
        AtomicBoolean startupInterrupted = new AtomicBoolean();
        doAnswer(invocation -> {
            startupEntered.countDown();
            awaitUninterruptibly(startupRelease, startupInterrupted);
            return null;
        }).when(auditor).start();

        Future<?> election = elector.start();
        await(startupEntered);
        ShutdownCall shutdown = startShutdown();
        await(managerClosed);
        shutdown.assertPending();
        assertFalse(elector.isRunning());
        verify(auditor, never()).shutdown();
        verify(bkc, never()).close();
        startupRelease.countDown();
        shutdown.assertSuccessful();
        election.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertFalse("Admitted startup must not be interrupted", startupInterrupted.get());
        verify(auditor).shutdown();
        verify(bkc).close();
    }

    @Test(timeout = 15000)
    public void testConcurrentAndRepeatedShutdownShareCleanupCompletion() throws Exception {
        createElector(true);
        CountDownLatch cleanupEntered = new CountDownLatch(1);
        CountDownLatch cleanupRelease = releaseLatch();
        doAnswer(invocation -> {
            cleanupEntered.countDown();
            awaitUninterruptibly(cleanupRelease, new AtomicBoolean());
            return null;
        }).when(auditor).shutdown();
        elector.start().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        ShutdownCall first = startShutdown();
        await(cleanupEntered);
        ShutdownCall second = startShutdown();
        await(second.entered);
        first.assertPending();
        second.assertPending();
        verify(bkc, never()).close();
        cleanupRelease.countDown();
        first.assertSuccessful();
        second.assertSuccessful();
        elector.shutdown();

        verify(manager).close();
        verify(auditor).shutdown();
        verify(bkc).close();
    }

    @Test(timeout = 15000)
    public void testLateCallbacksAndRestartCannotAdmitElection() throws Exception {
        createElector(true);
        AtomicReference<Consumer<AuditorEvent>> listener = new AtomicReference<>();
        CountDownLatch cleanupEntered = new CountDownLatch(1);
        CountDownLatch cleanupRelease = releaseLatch();
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(1));
            return null;
        }).when(manager).tryToBecomeAuditor(eq(BOOKIE_ID), any());
        doAnswer(invocation -> {
            cleanupEntered.countDown();
            awaitUninterruptibly(cleanupRelease, new AtomicBoolean());
            return null;
        }).when(auditor).shutdown();
        elector.start().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        ShutdownCall shutdown = startShutdown();
        await(cleanupEntered);
        listener.get().accept(AuditorEvent.VoteWasDeleted);
        listener.get().accept(AuditorEvent.SessionLost);
        elector.start().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        elector.submitElectionTask().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertFalse(elector.isRunning());
        cleanupRelease.countDown();
        shutdown.assertSuccessful();
        listener.get().accept(AuditorEvent.VoteWasDeleted);
        listener.get().accept(AuditorEvent.SessionLost);

        verify(manager).tryToBecomeAuditor(eq(BOOKIE_ID), any());
        verify(elector).createAuditor();
        verify(auditor).start();
        verify(manager).close();
    }

    @Test(timeout = 15000)
    public void testInterruptedShutdownWaiterStillWaitsForCleanup() throws Exception {
        createElector(true);
        CountDownLatch cleanupEntered = new CountDownLatch(1);
        CountDownLatch cleanupRelease = releaseLatch();
        doAnswer(invocation -> {
            cleanupEntered.countDown();
            awaitUninterruptibly(cleanupRelease, new AtomicBoolean());
            return null;
        }).when(auditor).shutdown();
        elector.start().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        ShutdownCall interrupted = startShutdown();
        await(cleanupEntered);
        interrupted.thread.interrupt();
        interrupted.assertPending();
        ShutdownCall other = startShutdown();
        await(other.entered);
        other.assertPending();
        cleanupRelease.countDown();
        interrupted.awaitCompletion();
        assertTrue(interrupted.failure.get() instanceof InterruptedException);
        other.assertSuccessful();
        elector.shutdown();

        verify(manager).close();
        verify(auditor).shutdown();
        verify(bkc).close();
    }

    @Test(timeout = 15000)
    public void testCleanupFailuresDoNotSkipRemainingCloses() throws Exception {
        createElector(true);
        doThrow(new IOException("manager close failed")).when(manager).close();
        doThrow(new IllegalStateException("auditor close failed")).when(auditor).shutdown();
        elector.start().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        startShutdown().assertSuccessful();
        elector.shutdown();

        verify(manager).close();
        verify(auditor).shutdown();
        verify(bkc).close();
    }

    @Test(timeout = 15000)
    public void testManagerCloseInterruptionDoesNotAbandonSubsequentCloses() throws Exception {
        createElector(true);
        AtomicBoolean auditorCloseInterrupted = new AtomicBoolean();
        AtomicBoolean clientCloseInterrupted = new AtomicBoolean();
        doAnswer(invocation -> {
            Thread.currentThread().interrupt();
            throw new InterruptedException("manager close interrupted");
        }).when(manager).close();
        doAnswer(invocation -> {
            auditorCloseInterrupted.set(Thread.currentThread().isInterrupted());
            return null;
        }).when(auditor).shutdown();
        doAnswer(invocation -> {
            clientCloseInterrupted.set(Thread.currentThread().isInterrupted());
            return null;
        }).when(bkc).close();
        elector.start().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        startShutdown().assertSuccessful();

        assertFalse(auditorCloseInterrupted.get());
        assertFalse(clientCloseInterrupted.get());
        verify(auditor).shutdown();
        verify(bkc).close();
    }

    @Test(timeout = 15000)
    public void testExternalBookKeeperClientIsPreserved() throws Exception {
        createElector(false);
        elector.start().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        startShutdown().assertSuccessful();

        verify(manager).close();
        verify(auditor).shutdown();
        verify(bkc, never()).close();
    }

    @Test(timeout = 15000)
    public void testSessionLostCallbackOnElectionWorkerDoesNotDeadlock() throws Exception {
        createElector(true);
        CountDownLatch callbackReturned = new CountDownLatch(1);
        doAnswer(invocation -> {
            Consumer<AuditorEvent> listener = invocation.getArgument(1);
            listener.accept(AuditorEvent.SessionLost);
            callbackReturned.countDown();
            return null;
        }).when(manager).tryToBecomeAuditor(eq(BOOKIE_ID), any());

        Future<?> election = elector.start();
        await(callbackReturned);
        startShutdown().assertSuccessful();
        election.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        verify(elector, never()).createAuditor();
        verify(manager).close();
        verify(bkc).close();
    }

    @Test(timeout = 15000)
    public void testShutdownCalledDuringConstructionDoesNotAwaitOwnWorker() throws Exception {
        createElector(true);
        CountDownLatch shutdownReturned = new CountDownLatch(1);
        doAnswer(invocation -> {
            elector.shutdown();
            shutdownReturned.countDown();
            return auditor;
        }).when(elector).createAuditor();

        Future<?> election = elector.start();
        await(shutdownReturned);
        startShutdown().assertSuccessful();
        election.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        verify(auditor, never()).start();
        verify(auditor).shutdown();
        verify(bkc).close();
    }

    @Test(timeout = 15000)
    public void testShutdownCalledFromManagerCloseDoesNotAwaitOwnCleanup() throws Exception {
        createElector(true);
        CountDownLatch shutdownReturned = new CountDownLatch(1);
        doAnswer(invocation -> {
            elector.shutdown();
            shutdownReturned.countDown();
            return null;
        }).when(manager).close();
        elector.start().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        startShutdown().assertSuccessful();

        assertEquals(0, shutdownReturned.getCount());
        verify(auditor).shutdown();
        verify(bkc).close();
    }

    @Test(timeout = 15000)
    public void testShutdownBeforeStartStillClosesManagerAndPreventsStart() throws Exception {
        createElector(true);

        startShutdown().assertSuccessful();
        elector.start().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        elector.shutdown();

        verify(manager).close();
        verify(manager, never()).tryToBecomeAuditor(eq(BOOKIE_ID), any());
        verify(elector, never()).createAuditor();
        verify(bkc, times(1)).close();
        assertFalse(elector.isRunning());
    }

    private void createElector(boolean ownBkc) throws Exception {
        elector = spy(new AuditorElector(BOOKIE_ID, new ServerConfiguration(), bkc, ownBkc));
        doReturn(auditor).when(elector).createAuditor();
    }

    private CountDownLatch releaseLatch() {
        CountDownLatch latch = new CountDownLatch(1);
        releaseLatches.add(latch);
        return latch;
    }

    private CountDownLatch signalManagerClose() throws Exception {
        CountDownLatch closed = new CountDownLatch(1);
        doAnswer(invocation -> {
            closed.countDown();
            return null;
        }).when(manager).close();
        return closed;
    }

    private ShutdownCall startShutdown() {
        ShutdownCall call = new ShutdownCall(elector);
        shutdownCalls.add(call);
        call.thread.start();
        return call;
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue("Timed out waiting for lifecycle event", latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    private static void awaitUninterruptibly(CountDownLatch latch, AtomicBoolean interrupted) {
        while (true) {
            try {
                latch.await();
                return;
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        }
    }

    private static final class ShutdownCall {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch completed = new CountDownLatch(1);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final Thread thread;

        private ShutdownCall(AuditorElector elector) {
            thread = new Thread(() -> {
                entered.countDown();
                try {
                    elector.shutdown();
                } catch (Throwable t) {
                    failure.set(t);
                } finally {
                    completed.countDown();
                }
            }, "AuditorElectorTest-shutdown");
            thread.setDaemon(true);
        }

        private void awaitCompletion() throws InterruptedException {
            assertTrue("Shutdown did not complete", completed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }

        private void assertSuccessful() throws InterruptedException {
            awaitCompletion();
            assertNull("Shutdown failed", failure.get());
        }

        private void assertPending() throws InterruptedException {
            assertFalse("Shutdown returned before cleanup completed", completed.await(100, TimeUnit.MILLISECONDS));
        }
    }
}
