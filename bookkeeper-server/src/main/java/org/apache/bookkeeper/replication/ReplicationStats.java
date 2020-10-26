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

/**
 * Replication statistics.
 */
public interface ReplicationStats {

    String REPLICATION_SCOPE = "replication";

    String AUDITOR_SCOPE = "auditor";
    String ELECTION_ATTEMPTS = "election_attempts";
    String NUM_UNDER_REPLICATED_LEDGERS = "NUM_UNDER_REPLICATED_LEDGERS";
    String URL_PUBLISH_TIME_FOR_LOST_BOOKIE = "URL_PUBLISH_TIME_FOR_LOST_BOOKIE";
    String BOOKIE_TO_LEDGERS_MAP_CREATION_TIME = "BOOKIE_TO_LEDGERS_MAP_CREATION_TIME";
    String CHECK_ALL_LEDGERS_TIME = "CHECK_ALL_LEDGERS_TIME";
    String PLACEMENT_POLICY_CHECK_TIME = "PLACEMENT_POLICY_CHECK_TIME";
    String REPLICAS_CHECK_TIME = "REPLICAS_CHECK_TIME";
    String AUDIT_BOOKIES_TIME = "AUDIT_BOOKIES_TIME";
    String NUM_FRAGMENTS_PER_LEDGER = "NUM_FRAGMENTS_PER_LEDGER";
    String NUM_BOOKIES_PER_LEDGER = "NUM_BOOKIES_PER_LEDGER";
    String NUM_LEDGERS_CHECKED = "NUM_LEDGERS_CHECKED";
    String NUM_BOOKIE_AUDITS_DELAYED = "NUM_BOOKIE_AUDITS_DELAYED";
    String NUM_DELAYED_BOOKIE_AUDITS_DELAYES_CANCELLED = "NUM_DELAYED_BOOKIE_AUDITS_CANCELLED";
    String NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY = "NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY";
    String NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY = "NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY";
    String NUM_UNDERREPLICATED_LEDGERS_ELAPSED_RECOVERY_GRACE_PERIOD =
            "NUM_UNDERREPLICATED_LEDGERS_ELAPSED_RECOVERY_GRACE_PERIOD";
    String NUM_LEDGERS_HAVING_NO_REPLICA_OF_AN_ENTRY = "NUM_LEDGERS_HAVING_NO_REPLICA_OF_AN_ENTRY";
    String NUM_LEDGERS_HAVING_LESS_THAN_AQ_REPLICAS_OF_AN_ENTRY =
            "NUM_LEDGERS_HAVING_LESS_THAN_AQ_REPLICAS_OF_AN_ENTRY";
    String NUM_LEDGERS_HAVING_LESS_THAN_WQ_REPLICAS_OF_AN_ENTRY =
            "NUM_LEDGERS_HAVING_LESS_THAN_WQ_REPLICAS_OF_AN_ENTRY";

    String REPLICATION_WORKER_SCOPE = "replication_worker";
    String REREPLICATE_OP = "rereplicate";
    String NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED = "NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED";
    String NUM_ENTRIES_READ = "NUM_ENTRIES_READ";
    String NUM_BYTES_READ = "NUM_BYTES_READ";
    String NUM_ENTRIES_WRITTEN = "NUM_ENTRIES_WRITTEN";
    String NUM_BYTES_WRITTEN = "NUM_BYTES_WRITTEN";
    String REPLICATE_EXCEPTION = "exceptions";
    String NUM_DEFER_LEDGER_LOCK_RELEASE_OF_FAILED_LEDGER = "NUM_DEFER_LEDGER_LOCK_RELEASE_OF_FAILED_LEDGER";
    String NUM_ENTRIES_UNABLE_TO_READ_FOR_REPLICATION = "NUM_ENTRIES_UNABLE_TO_READ_FOR_REPLICATION";
    String BK_CLIENT_SCOPE = "bk_client";

}
