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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerAuditorManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class AuditorElector {
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorElector.class);

    private final String bookieId;
    private final ServerConfiguration conf;
    private final BookKeeper bkc;
    private final boolean ownBkc;
    private final ExecutorService executor;
    private final LedgerAuditorManager ledgerAuditorManager;

    Auditor auditor;
    private AtomicBoolean running = new AtomicBoolean(false);


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
        running.set(true);
        return submitElectionTask();
    }

    /**
     * Run cleanup operations for the auditor elector.
     */
    private void submitShutdownTask() {
        executor.submit(new Runnable() {
                @Override
                public void run() {
                    if (!running.compareAndSet(true, false)) {
                        return;
                    }

                    try {
                        ledgerAuditorManager.close();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.warn("InterruptedException while closing ledger auditor manager", ie);
                    } catch (Exception ke) {
                        LOG.error("Exception while closing ledger auditor manager", ke);
                    }
                }
            });
    }

    /**
     * Performing the auditor election using the ZooKeeper ephemeral sequential
     * znode. The bookie which has created the least sequential will be elect as
     * Auditor.
     */
    @VisibleForTesting
    Future<?> submitElectionTask() {

        Runnable r = new Runnable() {
                @Override
                public void run() {
                    if (!running.get()) {
                        return;
                    }
                    try {
                        ledgerAuditorManager.tryToBecomeAuditor(bookieId, e -> handleAuditorEvent(e));

                        auditor = new Auditor(bookieId, conf, bkc, false, statsLogger);
                        auditor.start();
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted while performing auditor election", e);
                        Thread.currentThread().interrupt();
                        submitShutdownTask();
                    } catch (Exception e) {
                        LOG.error("Exception while performing auditor election", e);
                        submitShutdownTask();
                    }
                }
            };
        try {
            return executor.submit(r);
        } catch (RejectedExecutionException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Executor was already closed");
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private void handleAuditorEvent(LedgerAuditorManager.AuditorEvent e) {
        switch (e) {
            case SessionLost:
                LOG.error("Lost ZK connection, shutting down");
                submitShutdownTask();
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
     * Shutting down AuditorElector.
     */
    public void shutdown() throws InterruptedException {
        synchronized (this) {
            if (executor.isShutdown()) {
                return;
            }
            // close auditor manager
            submitShutdownTask();
            executor.shutdown();
        }

        if (auditor != null) {
            auditor.shutdown();
            auditor = null;
        }
        if (ownBkc) {
            try {
                bkc.close();
            } catch (BKException e) {
                LOG.warn("Failed to close bookkeeper client", e);
            }
        }
    }

    /**
     * If current bookie is running as auditor, return the status of the
     * auditor. Otherwise return the status of elector.
     *
     * @return
     */
    public boolean isRunning() {
        if (auditor != null) {
            return auditor.isRunning();
        }
        return running.get();
    }

    @Override
    public String toString() {
        return "AuditorElector for " + bookieId;
    }
}
