/*
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
 */

package org.apache.bookkeeper.client.impl;

import static org.apache.bookkeeper.client.BookKeeperClientStats.CATEGORY_CLIENT;
import static org.apache.bookkeeper.client.BookKeeperClientStats.CLIENT_SCOPE;

import org.apache.bookkeeper.client.BookKeeperClientStats;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * The default implementation of {@link BookKeeperClientStats}.
 */
@StatsDoc(
    name = CLIENT_SCOPE,
    category = CATEGORY_CLIENT,
    help = "BookKeeper client stats"
)
public class BookKeeperClientStatsImpl implements BookKeeperClientStats {
    private final StatsLogger stats;
    @StatsDoc(
        name = CREATE_OP,
        help = "operation stats of creating ledgers"
    )
    private final OpStatsLogger createOpLogger;
    @StatsDoc(
        name = DELETE_OP,
        help = "operation stats of deleting ledgers"
    )
    private final OpStatsLogger deleteOpLogger;
    @StatsDoc(
        name = OPEN_OP,
        help = "operation stats of opening ledgers"
    )
    private final OpStatsLogger openOpLogger;
    @StatsDoc(
        name = RECOVER_OP,
        help = "operation stats of recovering ledgers"
    )
    private final OpStatsLogger recoverOpLogger;
    @StatsDoc(
        name = READ_OP,
        help = "operation stats of reading entries requests"
    )
    private final OpStatsLogger readOpLogger;
    @StatsDoc(
        name = READ_OP_DM,
        help = "the number of read entries hitting DigestMismatch errors"
    )
    private final Counter readOpDmCounter;
    @StatsDoc(
        name = READ_LAST_CONFIRMED_AND_ENTRY,
        help = "operation stats of read_last_confirmed_and_entry requests"
    )
    private final OpStatsLogger readLacAndEntryOpLogger;
    @StatsDoc(
        name = READ_LAST_CONFIRMED_AND_ENTRY_RESPONSE,
        help = "operation stats of read_last_confirmed_and_entry responses"
    )
    private final OpStatsLogger readLacAndEntryRespLogger;
    @StatsDoc(
        name = ADD_OP,
        help = "operation stats of adding entries requests"
    )
    private final OpStatsLogger addOpLogger;
    @StatsDoc(
        name = FORCE_OP,
        help = "operation stats of force requests"
    )
    private final OpStatsLogger forceOpLogger;
    @StatsDoc(
        name = ADD_OP_UR,
        help = "the number of add entries under replication"
    )
    private final Counter addOpUrCounter;
    @StatsDoc(
        name = WRITE_LAC_OP,
        help = "operation stats of write_lac requests"
    )
    private final OpStatsLogger writeLacOpLogger;
    @StatsDoc(
        name = READ_LAC_OP,
        help = "operation stats of read_lac requests"
    )
    private final OpStatsLogger readLacOpLogger;
    @StatsDoc(
        name = LEDGER_RECOVER_ADD_ENTRIES,
        help = "the distribution of entries written in ledger recovery requests"
    )
    private final OpStatsLogger recoverAddEntriesStats;
    @StatsDoc(
        name = LEDGER_RECOVER_READ_ENTRIES,
        help = "the distribution of entries read in ledger recovery requests"
    )
    private final OpStatsLogger recoverReadEntriesStats;

    @StatsDoc(
        name = ENSEMBLE_CHANGES,
        help = "The number of ensemble changes"
    )
    private final Counter ensembleChangeCounter;
    @StatsDoc(
        name = LAC_UPDATE_HITS,
        help = "The number of successful lac updates on piggybacked responses"
    )
    private final Counter lacUpdateHitsCounter;
    @StatsDoc(
        name = LAC_UPDATE_MISSES,
        help = "The number of unsuccessful lac updates on piggybacked responses"
    )
    private final Counter lacUpdateMissesCounter;
    @StatsDoc(
        name = CLIENT_CHANNEL_WRITE_WAIT,
        help = " The latency distribution of waiting time on channel being writable"
    )
    private final OpStatsLogger clientChannelWriteWaitStats;
    @StatsDoc(
        name = SPECULATIVE_READ_COUNT,
        help = "The number of speculative read requests"
    )
    private final Counter speculativeReadCounter;

    @StatsDoc(
        name = WRITE_DELAYED_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS_LATENCY,
        help = "The delay in write completion because min number of fault domains was not reached"
    )
    private final OpStatsLogger writeDelayedDueToNotEnoughFaultDomainsLatency;

    @StatsDoc(
        name = WRITE_DELAYED_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS,
        help = "The number of times write completion was delayed because min number of fault domains was not reached"
    )
    private final Counter writeDelayedDueToNotEnoughFaultDomains;

    @StatsDoc(
        name = WRITE_TIMED_OUT_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS,
        help = "The number of times write completion timed out because min number of fault domains was not reached"
    )
    private final Counter writeTimedOutDueToNotEnoughFaultDomains;


    public BookKeeperClientStatsImpl(StatsLogger stats) {
        this.stats = stats;
        this.createOpLogger = stats.getOpStatsLogger(CREATE_OP);
        this.deleteOpLogger = stats.getOpStatsLogger(DELETE_OP);
        this.openOpLogger = stats.getOpStatsLogger(OPEN_OP);
        this.recoverOpLogger = stats.getOpStatsLogger(RECOVER_OP);
        this.readOpLogger = stats.getOpStatsLogger(READ_OP);
        this.readOpDmCounter = stats.getCounter(READ_OP_DM);
        this.readLacAndEntryOpLogger = stats.getOpStatsLogger(READ_LAST_CONFIRMED_AND_ENTRY);
        this.readLacAndEntryRespLogger = stats.getOpStatsLogger(READ_LAST_CONFIRMED_AND_ENTRY_RESPONSE);
        this.addOpLogger = stats.getOpStatsLogger(ADD_OP);
        this.forceOpLogger = stats.getOpStatsLogger(FORCE_OP);
        this.addOpUrCounter = stats.getCounter(ADD_OP_UR);
        this.writeLacOpLogger = stats.getOpStatsLogger(WRITE_LAC_OP);
        this.readLacOpLogger = stats.getOpStatsLogger(READ_LAC_OP);
        this.recoverAddEntriesStats = stats.getOpStatsLogger(LEDGER_RECOVER_ADD_ENTRIES);
        this.recoverReadEntriesStats = stats.getOpStatsLogger(LEDGER_RECOVER_READ_ENTRIES);

        this.ensembleChangeCounter = stats.getCounter(ENSEMBLE_CHANGES);
        this.lacUpdateHitsCounter = stats.getCounter(LAC_UPDATE_HITS);
        this.lacUpdateMissesCounter = stats.getCounter(LAC_UPDATE_MISSES);
        this.clientChannelWriteWaitStats = stats.getOpStatsLogger(CLIENT_CHANNEL_WRITE_WAIT);

        speculativeReadCounter = stats.getCounter(SPECULATIVE_READ_COUNT);

        this.writeDelayedDueToNotEnoughFaultDomainsLatency =
                stats.getOpStatsLogger(WRITE_DELAYED_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS_LATENCY);
        this.writeDelayedDueToNotEnoughFaultDomains = stats.getCounter(WRITE_DELAYED_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS);
        this.writeTimedOutDueToNotEnoughFaultDomains =
                stats.getCounter(WRITE_TIMED_OUT_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS);
    }

    @Override
    public OpStatsLogger getCreateOpLogger() {
        return createOpLogger;
    }
    @Override
    public OpStatsLogger getOpenOpLogger() {
        return openOpLogger;
    }
    @Override
    public OpStatsLogger getDeleteOpLogger() {
        return deleteOpLogger;
    }
    @Override
    public OpStatsLogger getRecoverOpLogger() {
        return recoverOpLogger;
    }
    @Override
    public OpStatsLogger getReadOpLogger() {
        return readOpLogger;
    }
    @Override
    public OpStatsLogger getReadLacAndEntryOpLogger() {
        return readLacAndEntryOpLogger;
    }
    @Override
    public OpStatsLogger getReadLacAndEntryRespLogger() {
        return readLacAndEntryRespLogger;
    }
    @Override
    public OpStatsLogger getAddOpLogger() {
        return addOpLogger;
    }
    @Override
    public OpStatsLogger getForceOpLogger() {
        return forceOpLogger;
    }
    @Override
    public OpStatsLogger getWriteLacOpLogger() {
        return writeLacOpLogger;
    }
    @Override
    public OpStatsLogger getReadLacOpLogger() {
        return readLacOpLogger;
    }
    @Override
    public OpStatsLogger getRecoverAddCountLogger() {
        return recoverAddEntriesStats;
    }
    @Override
    public OpStatsLogger getRecoverReadCountLogger() {
        return recoverReadEntriesStats;
    }
    @Override
    public Counter getReadOpDmCounter() {
        return readOpDmCounter;
    }
    @Override
    public Counter getAddOpUrCounter() {
        return addOpUrCounter;
    }
    @Override
    public Counter getSpeculativeReadCounter() {
        return speculativeReadCounter;
    }
    @Override
    public Counter getEnsembleChangeCounter() {
        return ensembleChangeCounter;
    }
    @Override
    public Counter getLacUpdateHitsCounter() {
        return lacUpdateHitsCounter;
    }
    @Override
    public Counter getLacUpdateMissesCounter() {
        return lacUpdateMissesCounter;
    }
    @Override
    public OpStatsLogger getClientChannelWriteWaitLogger() {
        return clientChannelWriteWaitStats;
    }
    @Override
    public Counter getEnsembleBookieDistributionCounter(String bookie) {
        return stats.getCounter(LEDGER_ENSEMBLE_BOOKIE_DISTRIBUTION + "-" + bookie);
    }
    @Override
    public OpStatsLogger getWriteDelayedDueToNotEnoughFaultDomainsLatency() {
        return writeDelayedDueToNotEnoughFaultDomainsLatency;
    }
    @Override
    public Counter getWriteDelayedDueToNotEnoughFaultDomains() {
        return writeDelayedDueToNotEnoughFaultDomains;
    }
    @Override
    public Counter getWriteTimedOutDueToNotEnoughFaultDomains() {
        return writeTimedOutDueToNotEnoughFaultDomains;
    }
    @Override
    public void registerPendingAddsGauge(Gauge<Integer> gauge) {
        stats.registerGauge(PENDING_ADDS, gauge);
    }
}
