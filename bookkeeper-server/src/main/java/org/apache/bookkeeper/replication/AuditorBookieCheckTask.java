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
package org.apache.bookkeeper.replication;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditorBookieCheckTask extends AuditorTask {
    private static final Logger LOG = LoggerFactory.getLogger(AuditorBookieCheckTask.class);

    private final BookieLedgerIndexer bookieLedgerIndexer;
    private final BiConsumer<Void, Throwable> submitCheckTask;

    public AuditorBookieCheckTask(ServerConfiguration conf,
                                  AuditorStats auditorStats,
                                  BookKeeperAdmin admin,
                                  LedgerManager ledgerManager,
                                  LedgerUnderreplicationManager ledgerUnderreplicationManager,
                                  ShutdownTaskHandler shutdownTaskHandler,
                                  BookieLedgerIndexer bookieLedgerIndexer,
                                  BiConsumer<AtomicBoolean, Throwable> hasAuditCheckTask,
                                  BiConsumer<Void, Throwable> submitCheckTask) {
        super(conf, auditorStats, admin, ledgerManager,
                ledgerUnderreplicationManager, shutdownTaskHandler, hasAuditCheckTask);
        this.bookieLedgerIndexer = bookieLedgerIndexer;
        this.submitCheckTask = submitCheckTask;
    }

    @Override
    protected void runTask() {
        if (!hasBookieCheckTask()) {
            startAudit(true);
        } else {
            // if due to a lost bookie an audit task was scheduled,
            // let us not run this periodic bookie check now, if we
            // went ahead, we'll report under replication and the user
            // wanted to avoid that(with lostBookieRecoveryDelay option)
            LOG.info("Audit already scheduled; skipping periodic bookie check");
            auditorStats.getNumSkippingCheckTaskTimes().inc();
        }
    }

    @Override
    public void shutdown() {

    }

    /**
     * Start running the actual audit task.
     *
     * @param shutDownTask A boolean that indicates whether or not to schedule shutdown task on any failure
     */
    void startAudit(boolean shutDownTask) {
        try {
            auditBookies();
            shutDownTask = false;
        } catch (BKException bke) {
            LOG.error("Exception getting bookie list", bke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while watching available bookies ", ie);
        } catch (ReplicationException.BKAuditException bke) {
            LOG.error("Exception while watching available bookies", bke);
        }
        if (shutDownTask) {
            submitShutdownTask();
        }
    }

    void auditBookies()
            throws ReplicationException.BKAuditException, InterruptedException, BKException {
        try {
            waitIfLedgerReplicationDisabled();
        } catch (ReplicationException.NonRecoverableReplicationException nre) {
            LOG.error("Non Recoverable Exception while reading from ZK", nre);
            submitShutdownTask();
            return;
        } catch (ReplicationException.UnavailableException ue) {
            LOG.error("Underreplication unavailable, skipping audit."
                    + "Will retry after a period");
            return;
        }
        LOG.info("Starting auditBookies");
        Stopwatch stopwatch = Stopwatch.createStarted();
        // put exit cases here
        Map<String, Set<Long>> ledgerDetails = generateBookie2LedgersIndex();
        try {
            if (!isLedgerReplicationEnabled()) {
                // has been disabled while we were generating the index
                // discard this run, and schedule a new one
                submitCheckTask.accept(null, null);
                return;
            }
        } catch (ReplicationException.UnavailableException ue) {
            LOG.error("Underreplication unavailable, skipping audit."
                    + "Will retry after a period");
            return;
        }

        List<String> availableBookies = getAvailableBookies();
        // find lost bookies
        Set<String> knownBookies = ledgerDetails.keySet();
        Collection<String> lostBookies = CollectionUtils.subtract(knownBookies,
                availableBookies);

        auditorStats.getBookieToLedgersMapCreationTime()
                .registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MILLISECONDS),
                TimeUnit.MILLISECONDS);
        if (lostBookies.size() > 0) {
            try {
                FutureUtils.result(
                        handleLostBookiesAsync(lostBookies, ledgerDetails), ReplicationException.EXCEPTION_HANDLER);
            } catch (ReplicationException e) {
                throw new ReplicationException.BKAuditException(e.getMessage(), e.getCause());
            }
            auditorStats.getURLPublishTimeForLostBookies()
                    .registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MILLISECONDS),
                    TimeUnit.MILLISECONDS);
        }
        LOG.info("Completed auditBookies");
        auditorStats.getAuditBookiesTime().registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MILLISECONDS),
                TimeUnit.MILLISECONDS);
    }

    private Map<String, Set<Long>> generateBookie2LedgersIndex()
            throws ReplicationException.BKAuditException {
        return bookieLedgerIndexer.getBookieToLedgerIndex();
    }

    private CompletableFuture<?> handleLostBookiesAsync(Collection<String> lostBookies,
                                                        Map<String, Set<Long>> ledgerDetails) {
        LOG.info("Following are the failed bookies: {},"
                + " and searching its ledgers for re-replication", lostBookies);

        return FutureUtils.processList(
                Lists.newArrayList(lostBookies),
                bookieIP -> publishSuspectedLedgersAsync(
                        Lists.newArrayList(bookieIP), ledgerDetails.get(bookieIP)),
                null
        );
    }

    protected void waitIfLedgerReplicationDisabled() throws ReplicationException.UnavailableException,
            InterruptedException {
        if (!isLedgerReplicationEnabled()) {
            LOG.info("LedgerReplication is disabled externally through Zookeeper, "
                    + "since DISABLE_NODE ZNode is created, so waiting untill it is enabled");
            ReplicationEnableCb cb = new ReplicationEnableCb();
            ledgerUnderreplicationManager.notifyLedgerReplicationEnabled(cb);
            cb.await();
        }
    }
}
