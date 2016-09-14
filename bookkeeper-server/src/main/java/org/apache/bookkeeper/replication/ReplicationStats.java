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

public interface ReplicationStats {

    public final static String REPLICATION_SCOPE = "replication";

    public final static String AUDITOR_SCOPE = "auditor";
    public final static String ELECTION_ATTEMPTS = "election_attempts";
    public final static String NUM_UNDER_REPLICATED_LEDGERS = "NUM_UNDER_REPLICATED_LEDGERS";
    public final static String URL_PUBLISH_TIME_FOR_LOST_BOOKIE = "URL_PUBLISH_TIME_FOR_LOST_BOOKIE";
    public final static String BOOKIE_TO_LEDGERS_MAP_CREATION_TIME = "BOOKIE_TO_LEDGERS_MAP_CREATION_TIME";
    public final static String CHECK_ALL_LEDGERS_TIME = "CHECK_ALL_LEDGERS_TIME";
    public final static String NUM_FRAGMENTS_PER_LEDGER = "NUM_FRAGMENTS_PER_LEDGER";
    public final static String NUM_BOOKIES_PER_LEDGER = "NUM_BOOKIES_PER_LEDGER";
    public final static String NUM_LEDGERS_CHECKED = "NUM_LEDGERS_CHECKED";
    public final static String NUM_BOOKIE_AUDITS_DELAYED = "NUM_BOOKIE_AUDITS_DELAYED";
    public final static String NUM_DELAYED_BOOKIE_AUDITS_DELAYES_CANCELLED = "NUM_DELAYED_BOOKIE_AUDITS_CANCELLED";

    public final static String REPLICATION_WORKER_SCOPE = "replication_worker";
    public final static String REREPLICATE_OP = "rereplicate";
    public final static String NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED = "NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED";
    public final static String NUM_ENTRIES_READ = "NUM_ENTRIES_READ";
    public final static String NUM_BYTES_READ = "NUM_BYTES_READ";
    public final static String NUM_ENTRIES_WRITTEN = "NUM_ENTRIES_WRITTEN";
    public final static String NUM_BYTES_WRITTEN = "NUM_BYTES_WRITTEN";

    public final static String BK_CLIENT_SCOPE = "bk_client";

}
