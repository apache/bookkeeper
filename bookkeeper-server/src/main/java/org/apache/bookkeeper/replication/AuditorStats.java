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

import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.AUDIT_BOOKIES_TIME;
import static org.apache.bookkeeper.replication.ReplicationStats.BOOKIE_TO_LEDGERS_MAP_CREATION_TIME;
import static org.apache.bookkeeper.replication.ReplicationStats.CHECK_ALL_LEDGERS_TIME;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_BOOKIES_PER_LEDGER;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_BOOKIE_AUDITS_DELAYED;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_DELAYED_BOOKIE_AUDITS_DELAYES_CANCELLED;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_FRAGMENTS_PER_LEDGER;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_LEDGERS_CHECKED;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_LEDGERS_HAVING_LESS_THAN_AQ_REPLICAS_OF_AN_ENTRY;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_LEDGERS_HAVING_LESS_THAN_WQ_REPLICAS_OF_AN_ENTRY;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_LEDGERS_HAVING_NO_REPLICA_OF_AN_ENTRY;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_SKIPPING_CHECK_TASK_TIMES;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_UNDERREPLICATED_LEDGERS_ELAPSED_RECOVERY_GRACE_PERIOD;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_UNDER_REPLICATED_LEDGERS;
import static org.apache.bookkeeper.replication.ReplicationStats.PLACEMENT_POLICY_CHECK_TIME;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICAS_CHECK_TIME;
import static org.apache.bookkeeper.replication.ReplicationStats.UNDER_REPLICATED_LEDGERS_TOTAL_SIZE;
import static org.apache.bookkeeper.replication.ReplicationStats.URL_PUBLISH_TIME_FOR_LOST_BOOKIE;

import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

@StatsDoc(
        name = AUDITOR_SCOPE,
        help = "Auditor related stats"
)
@Getter
public class AuditorStats {

    private final AtomicInteger ledgersNotAdheringToPlacementPolicyGuageValue;
    private final AtomicInteger ledgersSoftlyAdheringToPlacementPolicyGuageValue;
    private final AtomicInteger numOfURLedgersElapsedRecoveryGracePeriodGuageValue;
    private final AtomicInteger numLedgersHavingNoReplicaOfAnEntryGuageValue;
    private final AtomicInteger numLedgersHavingLessThanAQReplicasOfAnEntryGuageValue;
    private final AtomicInteger numLedgersHavingLessThanWQReplicasOfAnEntryGuageValue;
    private final AtomicInteger underReplicatedLedgersGuageValue;
    private final StatsLogger statsLogger;
    @StatsDoc(
            name = NUM_UNDER_REPLICATED_LEDGERS,
            help = "the distribution of num under_replicated ledgers on each auditor run"
    )
    private final OpStatsLogger numUnderReplicatedLedger;

    @StatsDoc(
            name = UNDER_REPLICATED_LEDGERS_TOTAL_SIZE,
            help = "the distribution of under_replicated ledgers total size on each auditor run"
    )
    private final OpStatsLogger underReplicatedLedgerTotalSize;
    @StatsDoc(
            name = URL_PUBLISH_TIME_FOR_LOST_BOOKIE,
            help = "the latency distribution of publishing under replicated ledgers for lost bookies"
    )
    private final OpStatsLogger uRLPublishTimeForLostBookies;
    @StatsDoc(
            name = BOOKIE_TO_LEDGERS_MAP_CREATION_TIME,
            help = "the latency distribution of creating bookies-to-ledgers map"
    )
    private final OpStatsLogger bookieToLedgersMapCreationTime;
    @StatsDoc(
            name = CHECK_ALL_LEDGERS_TIME,
            help = "the latency distribution of checking all ledgers"
    )
    private final OpStatsLogger checkAllLedgersTime;
    @StatsDoc(
            name = PLACEMENT_POLICY_CHECK_TIME,
            help = "the latency distribution of placementPolicy check"
    )
    private final OpStatsLogger placementPolicyCheckTime;
    @StatsDoc(
            name = REPLICAS_CHECK_TIME,
            help = "the latency distribution of replicas check"
    )
    private final OpStatsLogger replicasCheckTime;
    @StatsDoc(
            name = AUDIT_BOOKIES_TIME,
            help = "the latency distribution of auditing all the bookies"
    )
    private final OpStatsLogger auditBookiesTime;
    @StatsDoc(
            name = NUM_LEDGERS_CHECKED,
            help = "the number of ledgers checked by the auditor"
    )
    private final Counter numLedgersChecked;
    @StatsDoc(
            name = NUM_FRAGMENTS_PER_LEDGER,
            help = "the distribution of number of fragments per ledger"
    )
    private final OpStatsLogger numFragmentsPerLedger;
    @StatsDoc(
            name = NUM_BOOKIES_PER_LEDGER,
            help = "the distribution of number of bookies per ledger"
    )
    private final OpStatsLogger numBookiesPerLedger;
    @StatsDoc(
            name = NUM_BOOKIE_AUDITS_DELAYED,
            help = "the number of bookie-audits delayed"
    )
    private final Counter numBookieAuditsDelayed;
    @StatsDoc(
            name = NUM_DELAYED_BOOKIE_AUDITS_DELAYES_CANCELLED,
            help = "the number of delayed-bookie-audits cancelled"
    )
    private final Counter numDelayedBookieAuditsCancelled;
    @StatsDoc(
            name = NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY,
            help = "Gauge for number of ledgers not adhering to placement policy found in placement policy check"
    )
    private final Gauge<Integer> numLedgersNotAdheringToPlacementPolicy;
    @StatsDoc(
            name = NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY,
            help = "Gauge for number of ledgers softly adhering to placement policy found in placement policy check"
    )
    private final Gauge<Integer> numLedgersSoftlyAdheringToPlacementPolicy;
    @StatsDoc(
            name = NUM_UNDERREPLICATED_LEDGERS_ELAPSED_RECOVERY_GRACE_PERIOD,
            help = "Gauge for number of underreplicated ledgers elapsed recovery grace period"
    )
    private final Gauge<Integer> numUnderreplicatedLedgersElapsedRecoveryGracePeriod;
    @StatsDoc(
            name = NUM_LEDGERS_HAVING_NO_REPLICA_OF_AN_ENTRY,
            help = "Gauge for number of ledgers having an entry with all the replicas missing"
    )
    private final Gauge<Integer> numLedgersHavingNoReplicaOfAnEntry;
    @StatsDoc(
            name = NUM_LEDGERS_HAVING_LESS_THAN_AQ_REPLICAS_OF_AN_ENTRY,
            help = "Gauge for number of ledgers having an entry with less than AQ number of replicas"
                    + ", this doesn't include ledgers counted towards numLedgersHavingNoReplicaOfAnEntry"
    )
    private final Gauge<Integer> numLedgersHavingLessThanAQReplicasOfAnEntry;
    @StatsDoc(
            name = NUM_LEDGERS_HAVING_LESS_THAN_WQ_REPLICAS_OF_AN_ENTRY,
            help = "Gauge for number of ledgers having an entry with less than WQ number of replicas"
                    + ", this doesn't include ledgers counted towards numLedgersHavingLessThanAQReplicasOfAnEntry"
    )
    private final Gauge<Integer> numLedgersHavingLessThanWQReplicasOfAnEntry;
    @StatsDoc(
            name = NUM_SKIPPING_CHECK_TASK_TIMES,
            help = "the times of auditor check task skipped"
    )
    private final Counter numSkippingCheckTaskTimes;

    public AuditorStats(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        this.ledgersNotAdheringToPlacementPolicyGuageValue = new AtomicInteger(0);
        this.ledgersSoftlyAdheringToPlacementPolicyGuageValue = new AtomicInteger(0);
        this.numOfURLedgersElapsedRecoveryGracePeriodGuageValue = new AtomicInteger(0);
        this.numLedgersHavingNoReplicaOfAnEntryGuageValue = new AtomicInteger(0);
        this.numLedgersHavingLessThanAQReplicasOfAnEntryGuageValue = new AtomicInteger(0);
        this.numLedgersHavingLessThanWQReplicasOfAnEntryGuageValue = new AtomicInteger(0);
        this.underReplicatedLedgersGuageValue = new AtomicInteger(0);
        numUnderReplicatedLedger = this.statsLogger.getOpStatsLogger(ReplicationStats.NUM_UNDER_REPLICATED_LEDGERS);
        underReplicatedLedgerTotalSize = this.statsLogger.getOpStatsLogger(UNDER_REPLICATED_LEDGERS_TOTAL_SIZE);
        uRLPublishTimeForLostBookies = this.statsLogger
                .getOpStatsLogger(ReplicationStats.URL_PUBLISH_TIME_FOR_LOST_BOOKIE);
        bookieToLedgersMapCreationTime = this.statsLogger
                .getOpStatsLogger(ReplicationStats.BOOKIE_TO_LEDGERS_MAP_CREATION_TIME);
        checkAllLedgersTime = this.statsLogger.getOpStatsLogger(ReplicationStats.CHECK_ALL_LEDGERS_TIME);
        placementPolicyCheckTime = this.statsLogger.getOpStatsLogger(ReplicationStats.PLACEMENT_POLICY_CHECK_TIME);
        replicasCheckTime = this.statsLogger.getOpStatsLogger(ReplicationStats.REPLICAS_CHECK_TIME);
        auditBookiesTime = this.statsLogger.getOpStatsLogger(ReplicationStats.AUDIT_BOOKIES_TIME);
        numLedgersChecked = this.statsLogger.getCounter(ReplicationStats.NUM_LEDGERS_CHECKED);
        numFragmentsPerLedger = this.statsLogger.getOpStatsLogger(ReplicationStats.NUM_FRAGMENTS_PER_LEDGER);
        numBookiesPerLedger = this.statsLogger.getOpStatsLogger(ReplicationStats.NUM_BOOKIES_PER_LEDGER);
        numBookieAuditsDelayed = this.statsLogger.getCounter(ReplicationStats.NUM_BOOKIE_AUDITS_DELAYED);
        numDelayedBookieAuditsCancelled = this.statsLogger
                .getCounter(ReplicationStats.NUM_DELAYED_BOOKIE_AUDITS_DELAYES_CANCELLED);
        numSkippingCheckTaskTimes = this.statsLogger.getCounter(NUM_SKIPPING_CHECK_TASK_TIMES);
        numLedgersNotAdheringToPlacementPolicy = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return ledgersNotAdheringToPlacementPolicyGuageValue.get();
            }
        };
        this.statsLogger.registerGauge(ReplicationStats.NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY,
                numLedgersNotAdheringToPlacementPolicy);
        numLedgersSoftlyAdheringToPlacementPolicy = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return ledgersSoftlyAdheringToPlacementPolicyGuageValue.get();
            }
        };
        this.statsLogger.registerGauge(ReplicationStats.NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY,
                numLedgersSoftlyAdheringToPlacementPolicy);

        numUnderreplicatedLedgersElapsedRecoveryGracePeriod = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return numOfURLedgersElapsedRecoveryGracePeriodGuageValue.get();
            }
        };
        this.statsLogger.registerGauge(ReplicationStats.NUM_UNDERREPLICATED_LEDGERS_ELAPSED_RECOVERY_GRACE_PERIOD,
                numUnderreplicatedLedgersElapsedRecoveryGracePeriod);

        numLedgersHavingNoReplicaOfAnEntry = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return numLedgersHavingNoReplicaOfAnEntryGuageValue.get();
            }
        };
        this.statsLogger.registerGauge(ReplicationStats.NUM_LEDGERS_HAVING_NO_REPLICA_OF_AN_ENTRY,
                numLedgersHavingNoReplicaOfAnEntry);
        numLedgersHavingLessThanAQReplicasOfAnEntry = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return numLedgersHavingLessThanAQReplicasOfAnEntryGuageValue.get();
            }
        };
        this.statsLogger.registerGauge(ReplicationStats.NUM_LEDGERS_HAVING_LESS_THAN_AQ_REPLICAS_OF_AN_ENTRY,
                numLedgersHavingLessThanAQReplicasOfAnEntry);
        numLedgersHavingLessThanWQReplicasOfAnEntry = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return numLedgersHavingLessThanWQReplicasOfAnEntryGuageValue.get();
            }
        };
        this.statsLogger.registerGauge(ReplicationStats.NUM_LEDGERS_HAVING_LESS_THAN_WQ_REPLICAS_OF_AN_ENTRY,
                numLedgersHavingLessThanWQReplicasOfAnEntry);
    }
}
