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
import lombok.CustomLog;
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

@Getter
@CustomLog
public class AuditorPlacementPolicyCheckTask extends AuditorTask {

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
            log.info("Audit bookie task already scheduled; skipping periodic placement policy check task");
            auditorStats.getNumSkippingCheckTaskTimes().inc();
            return;
        }

        try {
            if (!isLedgerReplicationEnabled()) {
                log.info("Ledger replication disabled, skipping placementPolicyCheck");
                return;
            }

            Stopwatch stopwatch = Stopwatch.createStarted();
            log.info("Starting PlacementPolicyCheck");
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
            log.info().attr("durationMs", placementPolicyCheckDuration)
                    .attr("numOfClosedLedgersAudited", numOfClosedLedgersAuditedInPlacementPolicyCheckValue)
                    .attr("numOfLedgersNotAdhering", numOfLedgersFoundNotAdheringInPlacementPolicyCheckValue)
                    .attr("numOfLedgersSoftlyAdhering", numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue)
                    .attr("numOfURLedgersElapsedGracePeriod", numOfURLedgersElapsedRecoveryGracePeriodValue)
                    .log("Completed placementPolicyCheck");
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

            log.error().attr("numOfLedgersNotAdhering", numOfLedgersFoundInPlacementPolicyCheckValue)
                    .attr("numOfLedgersSoftlyAdhering", numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue)
                    .attr("numOfURLedgersElapsedGracePeriod", numOfURLedgersElapsedRecoveryGracePeriodValue)
                    .exception(e)
                    .log("BKAuditException running periodic placementPolicy check");
        } catch (ReplicationException.UnavailableException ue) {
            log.error().exception(ue).log("Underreplication manager unavailable running periodic check");
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
                log.info()
                        .attr("urLedgers", urLedgersElapsedRecoveryGracePeriod)
                        .log("No Underreplicated ledger has elapsed recovery grace period");
            } else {
                log.error()
                        .attr("urLedgers", urLedgersElapsedRecoveryGracePeriod)
                        .log("Following Underreplicated ledgers have elapsed recovery grace period");
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
                                    log.debug()
                                            .attr("ledgerId", ledgerId)
                                            .log("Ignoring replication of already deleted ledger");
                                iterCallback.processResult(BKException.Code.OK, null, null);
                            } else {
                                log.warn().attr("ledgerId", ledgerId).log("Unable to read the ledger information");
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
            log.error().exception(nre).log("Non Recoverable Exception while reading from ZK");
            submitShutdownTask();
        } catch (ReplicationException.UnavailableException ue) {
            log.error().exception(ue).log("Got exception while trying to set PlacementPolicyCheckCTime");
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
                    log.warn()
                            .attr("ledgerId", ledgerId)
                            .attr("startEntryId", startEntryIdOfSegment)
                            .attr("ensemble", ensembleOfSegment)
                            .attr("writeQuorumSize", writeQuorumSize)
                            .attr("ackQuorumSize", ackQuorumSize)
                            .log("Segment is not adhering to EnsemblePlacementPolicy");
                } else if (segmentAdheringToPlacementPolicy
                        == EnsemblePlacementPolicy.PlacementPolicyAdherence.MEETS_SOFT) {
                    foundSegmentSoftlyAdheringToPlacementPolicy = true;
                    log.debug()
                            .attr("ledgerId", ledgerId)
                            .attr("startEntryId", startEntryIdOfSegment)
                            .attr("ensemble", ensembleOfSegment)
                            .attr("writeQuorumSize", writeQuorumSize)
                            .attr("ackQuorumSize", ackQuorumSize)
                            .log("Segment is softly adhering to EnsemblePlacementPolicy");
                }
            }
            if (foundSegmentNotAdheringToPlacementPolicy) {
                numOfLedgersFoundNotAdheringInPlacementPolicyCheck.incrementAndGet();
                //If user enable repaired, mark this ledger to under replication manager.
                if (conf.isRepairedPlacementPolicyNotAdheringBookieEnable()) {
                    ledgerUnderreplicationManager.markLedgerUnderreplicatedAsync(ledgerId,
                            Collections.emptyList()).whenComplete((res, e) -> {
                        if (e != null) {
                            log.error()
                                    .attr("ledgerId", ledgerId)
                                    .exception(e)
                                    .log("Placement policy not adhering bookie"
                                            + " storage, mark to under replication"
                                            + " manager failed");
                            return;
                        }
                        log.debug()
                                .attr("ledgerId", ledgerId)
                                .log("Placement policy not adhering bookie"
                                        + " storage, marked to under replication"
                                        + " manager");
                    });
                }
            } else if (foundSegmentSoftlyAdheringToPlacementPolicy) {
                numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck
                        .incrementAndGet();
            }
            numOfClosedLedgersAuditedInPlacementPolicyCheck.incrementAndGet();
        } else {
            log.debug()
                    .attr("ledgerId", ledgerId)
                    .log("Ledger is not yet closed, so skipping the placementPolicy check analysis for now");
        }
        iterCallback.processResult(BKException.Code.OK, null, null);
    }
}
