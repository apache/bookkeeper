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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import lombok.Getter;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class AuditorPlacementPolicyCheckTask extends AuditorTask {
    private static final Logger LOG = LoggerFactory.getLogger(AuditorPlacementPolicyCheckTask.class);

    private final long underreplicatedLedgerRecoveryGracePeriod;

    private final AtomicInteger numOfLedgersFoundNotAdheringInPlacementPolicyCheck;
    private final AtomicInteger numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck;
    private final AtomicInteger numOfClosedLedgersAuditedInPlacementPolicyCheck;
    private final AtomicInteger numOfURLedgersElapsedRecoveryGracePeriod;

    AuditorPlacementPolicyCheckTask(ServerConfiguration conf,
                                    AuditorStats auditorStats,
                                    BookKeeperAdmin admin,
                                    LedgerManager ledgerManager,
                                    LedgerUnderreplicationManager ledgerUnderreplicationManager,
                                    ShutdownTaskHandler shutdownTaskHandler,
                                    BiConsumer<AtomicBoolean, Throwable> hasAuditCheckTask) {
        super(conf, auditorStats, admin, ledgerManager,
                ledgerUnderreplicationManager, shutdownTaskHandler, hasAuditCheckTask);
        this.underreplicatedLedgerRecoveryGracePeriod = conf.getUnderreplicatedLedgerRecoveryGracePeriod();
        this.numOfLedgersFoundNotAdheringInPlacementPolicyCheck = new AtomicInteger(0);
        this.numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck = new AtomicInteger(0);
        this.numOfClosedLedgersAuditedInPlacementPolicyCheck = new AtomicInteger(0);
        this.numOfURLedgersElapsedRecoveryGracePeriod = new AtomicInteger(0);
    }

    @Override
    protected void runTask() {
        if (hasBookieCheckTask()) {
            LOG.info("Audit bookie task already scheduled; skipping periodic placement policy check task");
            auditorStats.getNumSkippingCheckTaskTimes().inc();
            return;
        }

        try {
            if (!isLedgerReplicationEnabled()) {
                LOG.info("Ledger replication disabled, skipping placementPolicyCheck");
                return;
            }

            Stopwatch stopwatch = Stopwatch.createStarted();
            LOG.info("Starting PlacementPolicyCheck");
            placementPolicyCheck();
            long placementPolicyCheckDuration = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            int numOfLedgersFoundNotAdheringInPlacementPolicyCheckValue =
                    numOfLedgersFoundNotAdheringInPlacementPolicyCheck.get();
            int numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue =
                    numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck.get();
            int numOfClosedLedgersAuditedInPlacementPolicyCheckValue =
                    numOfClosedLedgersAuditedInPlacementPolicyCheck.get();
            int numOfURLedgersElapsedRecoveryGracePeriodValue =
                    numOfURLedgersElapsedRecoveryGracePeriod.get();
            LOG.info(
                    "Completed placementPolicyCheck in {} milliSeconds."
                            + " numOfClosedLedgersAuditedInPlacementPolicyCheck {}"
                            + " numOfLedgersNotAdheringToPlacementPolicy {}"
                            + " numOfLedgersSoftlyAdheringToPlacementPolicy {}"
                            + " numOfURLedgersElapsedRecoveryGracePeriod {}",
                    placementPolicyCheckDuration, numOfClosedLedgersAuditedInPlacementPolicyCheckValue,
                    numOfLedgersFoundNotAdheringInPlacementPolicyCheckValue,
                    numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue,
                    numOfURLedgersElapsedRecoveryGracePeriodValue);
            auditorStats.getLedgersNotAdheringToPlacementPolicyGuageValue()
                    .set(numOfLedgersFoundNotAdheringInPlacementPolicyCheckValue);
            auditorStats.getLedgersSoftlyAdheringToPlacementPolicyGuageValue()
                    .set(numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue);
            auditorStats.getNumOfURLedgersElapsedRecoveryGracePeriodGuageValue()
                    .set(numOfURLedgersElapsedRecoveryGracePeriodValue);
            auditorStats.getPlacementPolicyCheckTime().registerSuccessfulEvent(placementPolicyCheckDuration,
                    TimeUnit.MILLISECONDS);
        } catch (ReplicationException.BKAuditException e) {
            int numOfLedgersFoundInPlacementPolicyCheckValue =
                    numOfLedgersFoundNotAdheringInPlacementPolicyCheck.get();
            if (numOfLedgersFoundInPlacementPolicyCheckValue > 0) {
                /*
                 * Though there is BKAuditException while doing
                 * placementPolicyCheck, it found few ledgers not
                 * adhering to placement policy. So reporting it.
                 */
                auditorStats.getLedgersNotAdheringToPlacementPolicyGuageValue()
                        .set(numOfLedgersFoundInPlacementPolicyCheckValue);
            }

            int numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue =
                    numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck.get();
            if (numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue > 0) {
                /*
                 * Though there is BKAuditException while doing
                 * placementPolicyCheck, it found few ledgers softly
                 * adhering to placement policy. So reporting it.
                 */
                auditorStats.getLedgersSoftlyAdheringToPlacementPolicyGuageValue()
                        .set(numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue);
            }

            int numOfURLedgersElapsedRecoveryGracePeriodValue =
                    numOfURLedgersElapsedRecoveryGracePeriod.get();
            if (numOfURLedgersElapsedRecoveryGracePeriodValue > 0) {
                /*
                 * Though there is BKAuditException while doing
                 * placementPolicyCheck, it found few urledgers have
                 * elapsed recovery graceperiod. So reporting it.
                 */
                auditorStats.getNumOfURLedgersElapsedRecoveryGracePeriodGuageValue()
                        .set(numOfURLedgersElapsedRecoveryGracePeriodValue);
            }

            LOG.error(
                    "BKAuditException running periodic placementPolicy check."
                            + "numOfLedgersNotAdheringToPlacementPolicy {}, "
                            + "numOfLedgersSoftlyAdheringToPlacementPolicy {},"
                            + "numOfURLedgersElapsedRecoveryGracePeriod {}",
                    numOfLedgersFoundInPlacementPolicyCheckValue,
                    numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue,
                    numOfURLedgersElapsedRecoveryGracePeriodValue, e);
        } catch (ReplicationException.UnavailableException ue) {
            LOG.error("Underreplication manager unavailable running periodic check", ue);
        }
    }

    @Override
    public void shutdown() {

    }

    void placementPolicyCheck() throws ReplicationException.BKAuditException {
        final CountDownLatch placementPolicyCheckLatch = new CountDownLatch(1);
        numOfLedgersFoundNotAdheringInPlacementPolicyCheck.set(0);
        numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck.set(0);
        numOfClosedLedgersAuditedInPlacementPolicyCheck.set(0);
        numOfURLedgersElapsedRecoveryGracePeriod.set(0);
        if (this.underreplicatedLedgerRecoveryGracePeriod > 0) {
            Iterator<UnderreplicatedLedger> underreplicatedLedgersInfo = ledgerUnderreplicationManager
                    .listLedgersToRereplicate(null);
            List<Long> urLedgersElapsedRecoveryGracePeriod = new ArrayList<Long>();
            while (underreplicatedLedgersInfo.hasNext()) {
                UnderreplicatedLedger underreplicatedLedger = underreplicatedLedgersInfo.next();
                long underreplicatedLedgerMarkTimeInMilSecs = underreplicatedLedger.getCtime();
                if (underreplicatedLedgerMarkTimeInMilSecs != UnderreplicatedLedger.UNASSIGNED_CTIME) {
                    long elapsedTimeInSecs =
                            (System.currentTimeMillis() - underreplicatedLedgerMarkTimeInMilSecs) / 1000;
                    if (elapsedTimeInSecs > this.underreplicatedLedgerRecoveryGracePeriod) {
                        urLedgersElapsedRecoveryGracePeriod.add(underreplicatedLedger.getLedgerId());
                        numOfURLedgersElapsedRecoveryGracePeriod.incrementAndGet();
                    }
                }
            }
            if (urLedgersElapsedRecoveryGracePeriod.isEmpty()) {
                LOG.info("No Underreplicated ledger has elapsed recovery graceperiod: {}",
                        urLedgersElapsedRecoveryGracePeriod);
            } else {
                LOG.error("Following Underreplicated ledgers have elapsed recovery graceperiod: {}",
                        urLedgersElapsedRecoveryGracePeriod);
            }
        }
        BookkeeperInternalCallbacks.Processor<Long> ledgerProcessor =
                new BookkeeperInternalCallbacks.Processor<Long>() {
                    @Override
                    public void process(Long ledgerId, AsyncCallback.VoidCallback iterCallback) {
                        ledgerManager.readLedgerMetadata(ledgerId).whenComplete((metadataVer, exception) -> {
                            if (exception == null) {
                                doPlacementPolicyCheck(ledgerId, iterCallback, metadataVer);
                            } else if (BKException.getExceptionCode(exception)
                                    == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Ignoring replication of already deleted ledger {}",
                                            ledgerId);
                                }
                                iterCallback.processResult(BKException.Code.OK, null, null);
                            } else {
                                LOG.warn("Unable to read the ledger: {} information", ledgerId);
                                iterCallback.processResult(BKException.getExceptionCode(exception), null, null);
                            }
                        });
                    }
                };
        // Reading the result after processing all the ledgers
        final List<Integer> resultCode = new ArrayList<Integer>(1);
        ledgerManager.asyncProcessLedgers(ledgerProcessor, new AsyncCallback.VoidCallback() {

            @Override
            public void processResult(int rc, String s, Object obj) {
                resultCode.add(rc);
                placementPolicyCheckLatch.countDown();
            }
        }, null, BKException.Code.OK, BKException.Code.ReadException);
        try {
            placementPolicyCheckLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.BKAuditException("Exception while doing placementPolicy check", e);
        }
        if (!resultCode.contains(BKException.Code.OK)) {
            throw new ReplicationException.BKAuditException("Exception while doing placementPolicy check",
                    BKException.create(resultCode.get(0)));
        }
        try {
            ledgerUnderreplicationManager.setPlacementPolicyCheckCTime(System.currentTimeMillis());
        } catch (ReplicationException.NonRecoverableReplicationException nre) {
            LOG.error("Non Recoverable Exception while reading from ZK", nre);
            submitShutdownTask();
        } catch (ReplicationException.UnavailableException ue) {
            LOG.error("Got exception while trying to set PlacementPolicyCheckCTime", ue);
        }
    }

    void doPlacementPolicyCheck(Long ledgerId,
                                AsyncCallback.VoidCallback iterCallback,
                                Versioned<LedgerMetadata> metadataVer) {
        LedgerMetadata metadata = metadataVer.getValue();
        int writeQuorumSize = metadata.getWriteQuorumSize();
        int ackQuorumSize = metadata.getAckQuorumSize();
        if (metadata.isClosed()) {
            boolean foundSegmentNotAdheringToPlacementPolicy = false;
            boolean foundSegmentSoftlyAdheringToPlacementPolicy = false;
            for (Map.Entry<Long, ? extends List<BookieId>> ensemble : metadata
                    .getAllEnsembles().entrySet()) {
                long startEntryIdOfSegment = ensemble.getKey();
                List<BookieId> ensembleOfSegment = ensemble.getValue();
                EnsemblePlacementPolicy.PlacementPolicyAdherence segmentAdheringToPlacementPolicy = admin
                        .isEnsembleAdheringToPlacementPolicy(ensembleOfSegment, writeQuorumSize,
                                ackQuorumSize);
                if (segmentAdheringToPlacementPolicy == EnsemblePlacementPolicy.PlacementPolicyAdherence.FAIL) {
                    foundSegmentNotAdheringToPlacementPolicy = true;
                    LOG.warn(
                            "For ledger: {}, Segment starting at entry: {}, with ensemble: {} having "
                                    + "writeQuorumSize: {} and ackQuorumSize: {} is not adhering to "
                                    + "EnsemblePlacementPolicy",
                            ledgerId, startEntryIdOfSegment, ensembleOfSegment, writeQuorumSize,
                            ackQuorumSize);
                } else if (segmentAdheringToPlacementPolicy
                        == EnsemblePlacementPolicy.PlacementPolicyAdherence.MEETS_SOFT) {
                    foundSegmentSoftlyAdheringToPlacementPolicy = true;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "For ledger: {}, Segment starting at entry: {}, with ensemble: {}"
                                        + " having writeQuorumSize: {} and ackQuorumSize: {} is"
                                        + " softly adhering to EnsemblePlacementPolicy",
                                ledgerId, startEntryIdOfSegment, ensembleOfSegment, writeQuorumSize,
                                ackQuorumSize);
                    }
                }
            }
            if (foundSegmentNotAdheringToPlacementPolicy) {
                numOfLedgersFoundNotAdheringInPlacementPolicyCheck.incrementAndGet();
                //If user enable repaired, mark this ledger to under replication manager.
                if (conf.isRepairedPlacementPolicyNotAdheringBookieEnable()) {
                    ledgerUnderreplicationManager.markLedgerUnderreplicatedAsync(ledgerId,
                            Collections.emptyList()).whenComplete((res, e) -> {
                        if (e != null) {
                            LOG.error("For ledger: {}, the placement policy not adhering bookie "
                                            + "storage, mark it to under replication manager failed.",
                                    ledgerId, e);
                            return;
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("For ledger: {}, the placement policy not adhering bookie"
                                    + " storage, mark it to under replication manager", ledgerId);
                        }
                    });
                }
            } else if (foundSegmentSoftlyAdheringToPlacementPolicy) {
                numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck
                        .incrementAndGet();
            }
            numOfClosedLedgersAuditedInPlacementPolicyCheck.incrementAndGet();
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ledger: {} is not yet closed, so skipping the placementPolicy"
                        + "check analysis for now", ledgerId);
            }
        }
        iterCallback.processResult(BKException.Code.OK, null, null);
    }
}
