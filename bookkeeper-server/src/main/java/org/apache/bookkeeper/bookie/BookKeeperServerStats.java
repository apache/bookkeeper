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
package org.apache.bookkeeper.bookie;

public interface BookKeeperServerStats {

    public final static String SERVER_SCOPE = "bookkeeper_server";
    public final static String BOOKIE_SCOPE = "bookie";

    public final static String SERVER_STATUS = "SERVER_STATUS";

    // Server Operations
    public final static String ADD_ENTRY_REQUEST = "ADD_ENTRY_REQUEST";
    public final static String ADD_ENTRY = "ADD_ENTRY";
    public final static String READ_ENTRY_REQUEST = "READ_ENTRY_REQUEST";
    public final static String READ_ENTRY = "READ_ENTRY";
    public final static String READ_ENTRY_FENCE_REQUEST = "READ_ENTRY_FENCE_REQUEST";
    public final static String READ_ENTRY_FENCE_WAIT = "READ_ENTRY_FENCE_WAIT";
    public final static String READ_ENTRY_FENCE_READ = "READ_ENTRY_FENCE_READ";
    public final static String WRITE_LAC = "WRITE_LAC";
    public final static String READ_LAC = "READ_LAC";

    // Bookie Operations
    public final static String BOOKIE_ADD_ENTRY_BYTES = "BOOKIE_ADD_ENTRY_BYTES";
    public final static String BOOKIE_READ_ENTRY_BYTES = "BOOKIE_READ_ENTRY_BYTES";
    public final static String BOOKIE_ADD_ENTRY = "BOOKIE_ADD_ENTRY";
    public final static String BOOKIE_RECOVERY_ADD_ENTRY = "BOOKIE_RECOVERY_ADD_ENTRY";
    public final static String BOOKIE_READ_ENTRY = "BOOKIE_READ_ENTRY";

    // Journal Stats
    public final static String JOURNAL_SCOPE = "journal";
    public final static String JOURNAL_ADD_ENTRY = "JOURNAL_ADD_ENTRY";
    public final static String JOURNAL_SYNC = "JOURNAL_SYNC";
    public final static String JOURNAL_MEM_ADD_ENTRY = "JOURNAL_MEM_ADD_ENTRY";
    public final static String JOURNAL_PREALLOCATION = "JOURNAL_PREALLOCATION";
    public final static String JOURNAL_FORCE_WRITE_LATENCY = "JOURNAL_FORCE_WRITE_LATENCY";
    public final static String JOURNAL_FORCE_WRITE_BATCH_ENTRIES = "JOURNAL_FORCE_WRITE_BATCH_ENTRIES";
    public final static String JOURNAL_FORCE_WRITE_BATCH_BYTES = "JOURNAL_FORCE_WRITE_BATCH_BYTES";
    public final static String JOURNAL_FLUSH_LATENCY = "JOURNAL_FLUSH_LATENCY";
    public final static String JOURNAL_QUEUE_LATENCY = "JOURNAL_QUEUE_LATENCY";
    public final static String JOURNAL_PROCESS_TIME_LATENCY = "JOURNAL_PROCESS_TIME_LATENCY";
    public final static String JOURNAL_CREATION_LATENCY = "JOURNAL_CREATION_LATENCY";

    // Ledger Storage Stats
    public final static String STORAGE_GET_OFFSET = "STORAGE_GET_OFFSET";
    public final static String STORAGE_GET_ENTRY = "STORAGE_GET_ENTRY";
    public final static String SKIP_LIST_GET_ENTRY = "SKIP_LIST_GET_ENTRY";
    public final static String SKIP_LIST_PUT_ENTRY = "SKIP_LIST_PUT_ENTRY";
    public final static String SKIP_LIST_SNAPSHOT = "SKIP_LIST_SNAPSHOT";

    // Counters
    public final static String JOURNAL_WRITE_BYTES = "JOURNAL_WRITE_BYTES";
    public final static String JOURNAL_QUEUE_SIZE = "JOURNAL_QUEUE_SIZE";
    public final static String READ_BYTES = "READ_BYTES";
    public final static String WRITE_BYTES = "WRITE_BYTES";
    public final static String NUM_MINOR_COMP = "NUM_MINOR_COMP";
    public final static String NUM_MAJOR_COMP = "NUM_MAJOR_COMP";
    public final static String JOURNAL_FORCE_WRITE_QUEUE_SIZE = "JOURNAL_FORCE_WRITE_QUEUE_SIZE";
    public final static String JOURNAL_NUM_FORCE_WRITES = "JOURNAL_NUM_FORCE_WRITES";
    public final static String JOURNAL_NUM_FLUSH_EMPTY_QUEUE = "JOURNAL_NUM_FLUSH_EMPTY_QUEUE";
    public final static String JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES = "JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES";
    public final static String JOURNAL_NUM_FLUSH_MAX_WAIT = "JOURNAL_NUM_FLUSH_MAX_WAIT";
    public final static String SKIP_LIST_FLUSH_BYTES = "SKIP_LIST_FLUSH_BYTES";
    public final static String SKIP_LIST_THROTTLING = "SKIP_LIST_THROTTLING";
    public final static String READ_LAST_ENTRY_NOENTRY_ERROR = "READ_LAST_ENTRY_NOENTRY_ERROR";
    public final static String LEDGER_CACHE_NUM_EVICTED_LEDGERS = "LEDGER_CACHE_NUM_EVICTED_LEDGERS";

    // Gauge
    public final static String NUM_INDEX_PAGES = "NUM_INDEX_PAGES";
    public final static String NUM_OPEN_LEDGERS = "NUM_OPEN_LEDGERS";
    public final static String JOURNAL_FORCE_WRITE_GROUPING_COUNT = "JOURNAL_FORCE_WRITE_GROUPING_COUNT";
    public final static String NUM_PENDING_READ = "NUM_PENDING_READ";
    public final static String NUM_PENDING_ADD = "NUM_PENDING_ADD";

    // LedgerDirs Stats
    public final static String LD_LEDGER_SCOPE = "ledger";
    public final static String LD_INDEX_SCOPE = "index";
    public final static String LD_WRITABLE_DIRS = "writable_dirs";

}
