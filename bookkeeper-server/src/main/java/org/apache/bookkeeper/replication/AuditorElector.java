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

import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerAuditorManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * Performing auditor election using Apache ZooKeeper. Using ZooKeeper as a
 * coordination service, when a bookie bids for auditor, it creates an ephemeral
 * sequential file (znode) on ZooKeeper and considered as their vote. Vote
 * format is 'V_sequencenumber'. Election will be done by comparing the
 * ephemeral sequential numbers and the bookie which has created the least znode
 * will be elected as Auditor. All the other bookies will be watching on their
 * predecessor znode according to the ephemeral sequence numbers.
 */
@StatsDoc(
    name = AUDITOR_SCOPE,
    help = "Auditor related stats"
)
@CustomLog
public class AuditorElector {

    private final String bookieId;
    private final ServerConfiguration conf;
    private final BookKeeper bkc;
    private final boolean ownBkc;
    private final ExecutorService executor;
    private final LedgerAuditorManager ledgerAuditorManager;

    volatile Auditor auditor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Object lifecycleLock = new Object();
    private final CompletableFuture<Void> shutdownComplete = new CompletableFuture<>();
    // Guarded by lifecycleLock. Only the election wait may be interrupted; construction and
    // startup must finish before cleanup releases the resources they use.
    private boolean stopping;
    private boolean electionWaiting;
    private volatile Thread electionThread;
    private volatile Thread shutdownThread;

    private final StatsLogger statsLogger;


    @VisibleForTesting
    public AuditorElector(final String bookieId, ServerConfiguration conf) throws UnavailableException {
        this(
            bookieId,
            conf,
            Auditor.createBookKeeperClientThrowUnavailableException(conf),
            true);
    }

    /**
     * AuditorElector for performing the auditor election.
     *
     * @param bookieId
     *            - bookie identifier, comprises HostAddress:Port
     * @param conf
     *            - configuration
     * @param bkc
     *            - bookkeeper instance
     * @throws UnavailableException
     *             throws unavailable exception while initializing the elector
     */
    public AuditorElector(final String bookieId,
                          ServerConfiguration conf,
                          BookKeeper bkc,
                          boolean ownBkc) throws UnavailableException {
        this(bookieId, conf, bkc, NullStatsLogger.INSTANCE, ownBkc);
    }

    /**
     * AuditorElector for performing the auditor election.
     *
     * @param bookieId
     *            - bookie identifier, comprises HostAddress:Port
     * @param conf
     *            - configuration
     * @param bkc
     *            - bookkeeper instance
     * @param statsLogger
     *            - stats logger
     * @throws UnavailableException
     *             throws unavailable exception while initializing the elector
     */
    public AuditorElector(final String bookieId,
                          ServerConfiguration conf,
                          BookKeeper bkc,
                          StatsLogger statsLogger,
                          boolean ownBkc) throws UnavailableException {
        this.bookieId = bookieId;
        this.conf = conf;
        this.bkc = bkc;
        this.ownBkc = ownBkc;
        this.statsLogger = statsLogger;
        try {
            this.ledgerAuditorManager = bkc.getLedgerManagerFactory().newLedgerAuditorManager();
        } catch (Exception e) {
            throw new UnavailableException("Failed to instantiate the ledger auditor manager", e);
        }
        executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "AuditorElector-" + bookieId);
                }
            });
    }

    public Future<?> start() {
        synchronized (lifecycleLock) {
            if (stopping) {
                return CompletableFuture.completedFuture(null);
            }
            running.set(true);
            return submitElectionTask();
        }
    }

    /**
     * Request cleanup independently of the executor, which may be waiting for leadership.
     */
    private void requestShutdown() {
        synchronized (lifecycleLock) {
            if (stopping) {
                return;
            }
            stopping = true;
            running.set(false);
            executor.shutdown();
            shutdownThread = new Thread(this::completeShutdown, "AuditorElectorShutdown-" + bookieId);
            shutdownThread.start();
        }
    }

    /**
     * Performing the auditor election using the ZooKeeper ephemeral sequential
     * znode. The bookie which has created the least sequential will be elect as
     * Auditor.
     */
    @VisibleForTesting
    Future<?> submitElectionTask() {

        synchronized (lifecycleLock) {
            if (!running.get() || stopping) {
                return CompletableFuture.completedFuture(null);
            }
            try {
                return executor.submit(this::electAuditor);
            } catch (RejectedExecutionException e) {
                log.debug("Executor was already closed");
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    private void electAuditor() {
        synchronized (lifecycleLock) {
            electionThread = Thread.currentThread();
            if (stopping) {
                return;
            }
            electionWaiting = true;
        }
        try {
            ledgerAuditorManager.tryToBecomeAuditor(bookieId, this::handleAuditorEvent);
            synchronized (lifecycleLock) {
                electionWaiting = false;
                if (stopping) {
                    return;
                }
                // Construction is now admitted. Shutdown must let it finish without interruption.
            }

            Auditor newAuditor = createAuditor();
            synchronized (lifecycleLock) {
                // Publish even when stopping so cleanup can close a constructed, unstarted Auditor.
                auditor = newAuditor;
                if (stopping) {
                    return;
                }
                // Startup is now admitted. Cleanup waits for this task to exit before closing it.
            }
            newAuditor.start();
        } catch (InterruptedException e) {
            if (running.get()) {
                log.error().exception(e).log("Interrupted while performing auditor election");
            }
            Thread.currentThread().interrupt();
            requestShutdown();
        } catch (Exception e) {
            log.error().exception(e).log("Exception while performing auditor election");
            requestShutdown();
        } finally {
            synchronized (lifecycleLock) {
                electionWaiting = false;
            }
        }
    }

    @VisibleForTesting
    Auditor createAuditor() throws UnavailableException {
        return new Auditor(bookieId, conf, bkc, false, statsLogger);
    }

    private void handleAuditorEvent(LedgerAuditorManager.AuditorEvent e) {
        switch (e) {
            case SessionLost:
                log.error("Lost ZK connection, shutting down");
                requestShutdown();
                break;

            case VoteWasDeleted:
                submitElectionTask();
                break;
        }
    }

    @VisibleForTesting
    Auditor getAuditor() {
        return auditor;
    }


    public BookieId getCurrentAuditor() throws IOException, InterruptedException {
        return ledgerAuditorManager.getCurrentAuditor();
    }

    /**
     * Shut down the elector and wait for election work and resource cleanup to finish.
     * Concurrent and repeated callers wait for the same cleanup. Close failures are logged.
     * An interrupted caller waits for cleanup before receiving {@link InterruptedException}.
     * Calls from the election or cleanup thread only request shutdown to avoid waiting for themselves.
     */
    public void shutdown() throws InterruptedException {
        requestShutdown();
        if (Thread.currentThread() == electionThread || Thread.currentThread() == shutdownThread) {
            return;
        }
        boolean interrupted = false;
        while (true) {
            try {
                shutdownComplete.get();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            } catch (ExecutionException e) {
                // The shared failure has already been logged by the cleanup thread.
                break;
            }
        }
        if (interrupted) {
            throw new InterruptedException("Interrupted while waiting for auditor elector shutdown");
        }
    }

    private void completeShutdown() {
        Throwable failure = null;
        boolean interrupted = false;
        try {
            ledgerAuditorManager.close();
        } catch (Throwable t) {
            failure = t;
        }
        synchronized (lifecycleLock) {
            // Close first: interrupting an in-flight metadata create could lose its result and
            // leave a vote that the manager cannot delete. Managers that do not wake on close
            // must instead support interruption of their remaining election wait.
            if (electionWaiting) {
                electionThread.interrupt();
            }
        }
        // Some close implementations restore interruption. Do not pass it on to Auditor.shutdown(),
        // which would otherwise abandon its resource cleanup when awaiting its executor.
        interrupted |= Thread.interrupted();
        while (true) {
            try {
                if (executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                interrupted = true;
                failure = recordShutdownFailure(failure, e);
            }
        }

        Auditor currentAuditor = auditor;
        if (currentAuditor != null) {
            try {
                currentAuditor.shutdown();
            } catch (Throwable t) {
                failure = recordShutdownFailure(failure, t);
            } finally {
                auditor = null;
            }
        }
        interrupted |= Thread.interrupted();
        if (ownBkc) {
            try {
                bkc.close();
            } catch (Throwable t) {
                failure = recordShutdownFailure(failure, t);
            }
        }
        if (failure == null) {
            shutdownComplete.complete(null);
        } else {
            log.warn().exception(failure).log("Failed to close auditor elector");
            shutdownComplete.completeExceptionally(failure);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static Throwable recordShutdownFailure(Throwable previous, Throwable failure) {
        if (previous == null) {
            return failure;
        }
        if (previous != failure) {
            previous.addSuppressed(failure);
        }
        return previous;
    }

    /**
     * If current bookie is running as auditor, return the status of the
     * auditor. Otherwise return the status of elector.
     *
     * @return
     */
    public boolean isRunning() {
        if (!running.get()) {
            return false;
        }
        Auditor currentAuditor = auditor;
        return currentAuditor == null || currentAuditor.isRunning();
    }

    @Override
    public String toString() {
        return "AuditorElector for " + bookieId;
    }
}
