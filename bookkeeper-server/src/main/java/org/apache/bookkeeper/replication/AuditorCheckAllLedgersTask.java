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
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerChecker;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.zookeeper.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditorCheckAllLedgersTask extends AuditorTask {
    private static final Logger LOG = LoggerFactory.getLogger(AuditorBookieCheckTask.class);

    private final Semaphore openLedgerNoRecoverySemaphore;
    private final int openLedgerNoRecoverySemaphoreWaitTimeoutMSec;
    private final ExecutorService ledgerCheckerExecutor;

    AuditorCheckAllLedgersTask(ServerConfiguration conf,
                               AuditorStats auditorStats,
                               BookKeeperAdmin admin,
                               LedgerManager ledgerManager,
                               LedgerUnderreplicationManager ledgerUnderreplicationManager,
                               ShutdownTaskHandler shutdownTaskHandler,
                               BiConsumer<AtomicBoolean, Throwable> hasAuditCheckTask)
            throws UnavailableException {
        super(conf, auditorStats, admin, ledgerManager,
                ledgerUnderreplicationManager, shutdownTaskHandler, hasAuditCheckTask);

        if (conf.getAuditorMaxNumberOfConcurrentOpenLedgerOperations() <= 0) {
            LOG.error("auditorMaxNumberOfConcurrentOpenLedgerOperations should be greater than 0");
            throw new UnavailableException("auditorMaxNumberOfConcurrentOpenLedgerOperations should be greater than 0");
        }
        this.openLedgerNoRecoverySemaphore =
                new Semaphore(conf.getAuditorMaxNumberOfConcurrentOpenLedgerOperations());

        if (conf.getAuditorAcquireConcurrentOpenLedgerOperationsTimeoutMSec() < 0) {
            LOG.error("auditorAcquireConcurrentOpenLedgerOperationsTimeoutMSec should be greater than or equal to 0");
            throw new UnavailableException("auditorAcquireConcurrentOpenLedgerOperationsTimeoutMSec "
                    + "should be greater than or equal to 0");
        }
        this.openLedgerNoRecoverySemaphoreWaitTimeoutMSec =
                conf.getAuditorAcquireConcurrentOpenLedgerOperationsTimeoutMSec();

        this.ledgerCheckerExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "AuditorCheckAllLedgers-LedgerChecker");
                t.setDaemon(true);
                return t;
            }
        });
    }

    @Override
    protected void runTask() {
        if (hasBookieCheckTask()) {
            LOG.info("Audit bookie task already scheduled; skipping periodic all ledgers check task");
            auditorStats.getNumSkippingCheckTaskTimes().inc();
            return;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean checkSuccess = false;
        try {
            if (!isLedgerReplicationEnabled()) {
                LOG.info("Ledger replication disabled, skipping checkAllLedgers");
                checkSuccess = true;
                return;
            }

            LOG.info("Starting checkAllLedgers");
            checkAllLedgers();
            long checkAllLedgersDuration = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            LOG.info("Completed checkAllLedgers in {} milliSeconds", checkAllLedgersDuration);
            auditorStats.getCheckAllLedgersTime()
                    .registerSuccessfulEvent(checkAllLedgersDuration, TimeUnit.MILLISECONDS);
            checkSuccess = true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while running periodic check", ie);
        } catch (BKException bke) {
            LOG.error("Exception running periodic check", bke);
        } catch (IOException ioe) {
            LOG.error("I/O exception running periodic check", ioe);
        } catch (ReplicationException.NonRecoverableReplicationException nre) {
            LOG.error("Non Recoverable Exception while reading from ZK", nre);
            submitShutdownTask();
        } catch (ReplicationException.UnavailableException ue) {
            LOG.error("Underreplication manager unavailable running periodic check", ue);
        } finally {
            if (!checkSuccess) {
                long checkAllLedgersDuration = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
                auditorStats.getCheckAllLedgersTime()
                        .registerFailedEvent(checkAllLedgersDuration, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down AuditorCheckAllLedgersTask");
        ledgerCheckerExecutor.shutdown();
        try {
            while (!ledgerCheckerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("Executor for ledger checker not shutting down, interrupting");
                ledgerCheckerExecutor.shutdownNow();
            }

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while shutting down AuditorCheckAllLedgersTask", ie);
        }
    }

    /**
     * List all the ledgers and check them individually. This should not
     * be run very often.
     */
    void checkAllLedgers() throws BKException, IOException, InterruptedException {
        final BookKeeper localClient = getBookKeeper(conf);
        final BookKeeperAdmin localAdmin = getBookKeeperAdmin(localClient);
        try {
            final LedgerChecker checker = new LedgerChecker(localClient, conf.getInFlightReadEntryNumInLedgerChecker());

            final CompletableFuture<Void> processFuture = new CompletableFuture<>();

            BookkeeperInternalCallbacks.Processor<Long> checkLedgersProcessor = (ledgerId, callback) -> {
                try {
                    if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
                        LOG.info("Ledger rereplication has been disabled, aborting periodic check");
                        FutureUtils.complete(processFuture, null);
                        return;
                    }
                } catch (ReplicationException.NonRecoverableReplicationException nre) {
                    LOG.error("Non Recoverable Exception while reading from ZK", nre);
                    submitShutdownTask();
                    return;
                } catch (ReplicationException.UnavailableException ue) {
                    LOG.error("Underreplication manager unavailable running periodic check", ue);
                    FutureUtils.complete(processFuture, null);
                    return;
                }

                try {
                    if (!openLedgerNoRecoverySemaphore.tryAcquire(openLedgerNoRecoverySemaphoreWaitTimeoutMSec,
                            TimeUnit.MILLISECONDS)) {
                        LOG.warn("Failed to acquire semaphore for {} ms, ledgerId: {}",
                                openLedgerNoRecoverySemaphoreWaitTimeoutMSec, ledgerId);
                        FutureUtils.complete(processFuture, null);
                        return;
                    }
                } catch (InterruptedException e) {
                    LOG.error("Unable to acquire open ledger operation semaphore ", e);
                    Thread.currentThread().interrupt();
                    FutureUtils.complete(processFuture, null);
                    return;
                }

                localAdmin.asyncOpenLedgerNoRecovery(ledgerId, (rc, lh, ctx) -> {
                    openLedgerNoRecoverySemaphore.release();
                    if (BKException.Code.OK == rc) {
                        // BookKeeperClientWorker-OrderedExecutor threads should not execute LedgerChecker#checkLedger
                        // as this can lead to deadlocks
                        ledgerCheckerExecutor.execute(() -> {
                            checker.checkLedger(lh,
                                    // the ledger handle will be closed after checkLedger is done.
                                    new ProcessLostFragmentsCb(lh, callback),
                                    conf.getAuditorLedgerVerificationPercentage());
                            // we collect the following stats to get a measure of the
                            // distribution of a single ledger within the bk cluster
                            // the higher the number of fragments/bookies, the more distributed it is
                            auditorStats.getNumFragmentsPerLedger().registerSuccessfulValue(lh.getNumFragments());
                            auditorStats.getNumBookiesPerLedger().registerSuccessfulValue(lh.getNumBookies());
                            auditorStats.getNumLedgersChecked().inc();
                        });
                    } else if (BKException.Code.NoSuchLedgerExistsOnMetadataServerException == rc) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ledger {} was deleted before we could check it", ledgerId);
                        }
                        callback.processResult(BKException.Code.OK, null, null);
                    } else {
                        LOG.error("Couldn't open ledger {} to check : {}", ledgerId, BKException.getMessage(rc));
                        callback.processResult(rc, null, null);
                    }
                }, null);
            };

            ledgerManager.asyncProcessLedgers(checkLedgersProcessor,
                    (rc, path, ctx) -> {
                        if (BKException.Code.OK == rc) {
                            FutureUtils.complete(processFuture, null);
                        } else {
                            FutureUtils.completeExceptionally(processFuture, BKException.create(rc));
                        }
                    }, null, BKException.Code.OK, BKException.Code.ReadException);
            FutureUtils.result(processFuture, BKException.HANDLER);
            try {
                ledgerUnderreplicationManager.setCheckAllLedgersCTime(System.currentTimeMillis());
            } catch (ReplicationException.NonRecoverableReplicationException nre) {
                LOG.error("Non Recoverable Exception while reading from ZK", nre);
                submitShutdownTask();
            } catch (ReplicationException.UnavailableException ue) {
                LOG.error("Got exception while trying to set checkAllLedgersCTime", ue);
            }
        } finally {
            localAdmin.close();
            localClient.close();
        }
    }

    /**
     * Process the result returned from checking a ledger.
     */
    private class ProcessLostFragmentsCb implements BookkeeperInternalCallbacks.GenericCallback<Set<LedgerFragment>> {
        final LedgerHandle lh;
        final AsyncCallback.VoidCallback callback;

        ProcessLostFragmentsCb(LedgerHandle lh, AsyncCallback.VoidCallback callback) {
            this.lh = lh;
            this.callback = callback;
        }

        @Override
        public void operationComplete(int rc, Set<LedgerFragment> fragments) {
            if (rc == BKException.Code.OK) {
                Set<BookieId> bookies = Sets.newHashSet();
                for (LedgerFragment f : fragments) {
                    bookies.addAll(f.getAddresses());
                }
                if (bookies.isEmpty()) {
                    // no missing fragments
                    callback.processResult(BKException.Code.OK, null, null);
                } else {
                    publishSuspectedLedgersAsync(bookies.stream().map(BookieId::toString).collect(Collectors.toList()),
                            Sets.newHashSet(lh.getId())
                    ).whenComplete((result, cause) -> {
                        if (null != cause) {
                            LOG.error("Auditor exception publishing suspected ledger {} with lost bookies {}",
                                    lh.getId(), bookies, cause);
                            callback.processResult(BKException.Code.ReplicationException, null, null);
                        } else {
                            callback.processResult(BKException.Code.OK, null, null);
                        }
                    });
                }
            } else {
                callback.processResult(rc, null, null);
            }
            lh.closeAsync().whenComplete((result, cause) -> {
                if (null != cause) {
                    LOG.warn("Error closing ledger {} : {}", lh.getId(), cause.getMessage());
                }
            });
        }
    }
}
