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

package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.impl.BookKeeperClientStatsImpl;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * List of constants for defining client stats names.
 */
public interface BookKeeperClientStats {
    String CATEGORY_CLIENT = "client";

    String CLIENT_SCOPE = "bookkeeper_client";

    // Metadata Operations

    String CREATE_OP = "LEDGER_CREATE";
    String DELETE_OP = "LEDGER_DELETE";
    String OPEN_OP = "LEDGER_OPEN";
    String RECOVER_OP = "LEDGER_RECOVER";
    String LEDGER_RECOVER_READ_ENTRIES = "LEDGER_RECOVER_READ_ENTRIES";
    String LEDGER_RECOVER_ADD_ENTRIES = "LEDGER_RECOVER_ADD_ENTRIES";
    String LEDGER_ENSEMBLE_BOOKIE_DISTRIBUTION = "LEDGER_ENSEMBLE_BOOKIE_DISTRIBUTION";

    // Data Operations

    String ADD_OP = "ADD_ENTRY";
    String ADD_OP_UR = "ADD_ENTRY_UR"; // Under Replicated during AddEntry.
    String FORCE_OP = "FORCE"; // Number of force ledger operations
    String READ_OP = "READ_ENTRY";
    // Corrupted entry (Digest Mismatch/ Under Replication) detected during ReadEntry
    String READ_OP_DM = "READ_ENTRY_DM";
    String WRITE_LAC_OP = "WRITE_LAC";
    String READ_LAC_OP = "READ_LAC";
    String READ_LAST_CONFIRMED_AND_ENTRY = "READ_LAST_CONFIRMED_AND_ENTRY";
    String READ_LAST_CONFIRMED_AND_ENTRY_RESPONSE = "READ_LAST_CONFIRMED_AND_ENTRY_RESPONSE";
    String PENDING_ADDS = "NUM_PENDING_ADD";
    String ENSEMBLE_CHANGES = "NUM_ENSEMBLE_CHANGE";
    String LAC_UPDATE_HITS = "LAC_UPDATE_HITS";
    String LAC_UPDATE_MISSES = "LAC_UPDATE_MISSES";
    String GET_BOOKIE_INFO_OP = "GET_BOOKIE_INFO";
    String SPECULATIVE_READ_COUNT = "SPECULATIVE_READ_COUNT";
    String READ_REQUESTS_REORDERED = "READ_REQUESTS_REORDERED";
    String GET_LIST_OF_ENTRIES_OF_LEDGER_OP = "GET_LIST_OF_ENTRIES_OF_LEDGER";

    // per channel stats
    String CHANNEL_SCOPE = "per_channel_bookie_client";

    String CHANNEL_READ_OP = "READ_ENTRY";
    String CHANNEL_TIMEOUT_READ = "TIMEOUT_READ_ENTRY";
    String CHANNEL_ADD_OP = "ADD_ENTRY";
    String CHANNEL_TIMEOUT_ADD = "TIMEOUT_ADD_ENTRY";
    String CHANNEL_WRITE_LAC_OP = "WRITE_LAC";
    String CHANNEL_FORCE_OP = "FORCE";
    String CHANNEL_TIMEOUT_WRITE_LAC = "TIMEOUT_WRITE_LAC";
    String CHANNEL_TIMEOUT_FORCE = "TIMEOUT_FORCE";
    String CHANNEL_READ_LAC_OP = "READ_LAC";
    String CHANNEL_TIMEOUT_READ_LAC = "TIMEOUT_READ_LAC";
    String TIMEOUT_GET_BOOKIE_INFO = "TIMEOUT_GET_BOOKIE_INFO";
    String CHANNEL_START_TLS_OP = "START_TLS";
    String CHANNEL_TIMEOUT_START_TLS_OP = "TIMEOUT_START_TLS";
    String TIMEOUT_GET_LIST_OF_ENTRIES_OF_LEDGER = "TIMEOUT_GET_LIST_OF_ENTRIES_OF_LEDGER";

    String NETTY_EXCEPTION_CNT = "NETTY_EXCEPTION_CNT";
    String CLIENT_CHANNEL_WRITE_WAIT = "CLIENT_CHANNEL_WRITE_WAIT";
    String CLIENT_CONNECT_TIMER = "CLIENT_CONNECT_TIMER";
    String ADD_OP_OUTSTANDING = "ADD_OP_OUTSTANDING";
    String READ_OP_OUTSTANDING = "READ_OP_OUTSTANDING";
    String NETTY_OPS = "NETTY_OPS";
    String ACTIVE_NON_TLS_CHANNEL_COUNTER = "ACTIVE_NON_TLS_CHANNEL_COUNTER";
    String ACTIVE_TLS_CHANNEL_COUNTER = "ACTIVE_TLS_CHANNEL_COUNTER";
    String FAILED_CONNECTION_COUNTER = "FAILED_CONNECTION_COUNTER";
    String FAILED_TLS_HANDSHAKE_COUNTER = "FAILED_TLS_HANDSHAKE_COUNTER";

    // placementpolicy stats
    String NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK = "NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK";
    String WRITE_DELAYED_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS = "WRITE_DELAYED_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS";
    String WRITE_DELAYED_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS_LATENCY =
            "WRITE_DELAYED_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS_LATENCY";
    String WRITE_TIMED_OUT_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS = "WRITE_TIME_OUT_DUE_TO_NOT_ENOUGH_FAULT_DOMAINS";
    String NUM_WRITABLE_BOOKIES_IN_DEFAULT_FAULTDOMAIN = "NUM_WRITABLE_BOOKIES_IN_DEFAULT_FAULTDOMAIN";

    OpStatsLogger getCreateOpLogger();
    OpStatsLogger getOpenOpLogger();
    OpStatsLogger getDeleteOpLogger();
    OpStatsLogger getRecoverOpLogger();
    OpStatsLogger getReadOpLogger();
    OpStatsLogger getReadLacAndEntryOpLogger();
    OpStatsLogger getReadLacAndEntryRespLogger();
    OpStatsLogger getAddOpLogger();
    OpStatsLogger getForceOpLogger();
    OpStatsLogger getWriteLacOpLogger();
    OpStatsLogger getReadLacOpLogger();
    OpStatsLogger getRecoverAddCountLogger();
    OpStatsLogger getRecoverReadCountLogger();
    Counter getReadOpDmCounter();
    Counter getAddOpUrCounter();
    Counter getSpeculativeReadCounter();
    Counter getEnsembleBookieDistributionCounter(String bookie);
    Counter getEnsembleChangeCounter();
    Counter getLacUpdateHitsCounter();
    Counter getLacUpdateMissesCounter();
    OpStatsLogger getClientChannelWriteWaitLogger();
    OpStatsLogger getWriteDelayedDueToNotEnoughFaultDomainsLatency();
    Counter getWriteDelayedDueToNotEnoughFaultDomains();
    Counter getWriteTimedOutDueToNotEnoughFaultDomains();
    void registerPendingAddsGauge(Gauge<Integer> gauge);

    static BookKeeperClientStats newInstance(StatsLogger stats) {
        return new BookKeeperClientStatsImpl(stats);
    }

}
