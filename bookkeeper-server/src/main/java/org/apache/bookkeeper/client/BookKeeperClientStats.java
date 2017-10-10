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

/**
 * List of constants for defining client stats names.
 */
public interface BookKeeperClientStats {
    public final static String CLIENT_SCOPE = "bookkeeper_client";

    // Metadata Operations

    public final static String CREATE_OP = "LEDGER_CREATE";
    public final static String DELETE_OP = "LEDGER_DELETE";
    public final static String OPEN_OP = "LEDGER_OPEN";
    public final static String RECOVER_OP = "LEDGER_RECOVER";
    public final static String LEDGER_RECOVER_READ_ENTRIES = "LEDGER_RECOVER_READ_ENTRIES";
    public final static String LEDGER_RECOVER_ADD_ENTRIES = "LEDGER_RECOVER_ADD_ENTRIES";

    // Data Operations

    public final static String ADD_OP = "ADD_ENTRY";
    public final static String READ_OP = "READ_ENTRY";
    public final static String WRITE_LAC_OP = "WRITE_LAC";
    public final static String READ_LAC_OP = "READ_LAC";
    public final static String READ_LAST_CONFIRMED_AND_ENTRY = "READ_LAST_CONFIRMED_AND_ENTRY";
    public final static String READ_LAST_CONFIRMED_AND_ENTRY_RESPONSE = "READ_LAST_CONFIRMED_AND_ENTRY_RESPONSE";
    public final static String PENDING_ADDS = "NUM_PENDING_ADD";
    public final static String ENSEMBLE_CHANGES = "NUM_ENSEMBLE_CHANGE";
    public final static String LAC_UPDATE_HITS = "LAC_UPDATE_HITS";
    public final static String LAC_UPDATE_MISSES = "LAC_UPDATE_MISSES";
    public final static String GET_BOOKIE_INFO_OP = "GET_BOOKIE_INFO";

    // per channel stats
    public final static String CHANNEL_SCOPE = "per_channel_bookie_client";

    public final static String CHANNEL_READ_OP = "READ_ENTRY";
    public final static String CHANNEL_TIMEOUT_READ = "TIMEOUT_READ_ENTRY";
    public final static String CHANNEL_ADD_OP = "ADD_ENTRY";
    public final static String CHANNEL_TIMEOUT_ADD = "TIMEOUT_ADD_ENTRY";
    public final static String CHANNEL_WRITE_LAC_OP = "WRITE_LAC";
    public final static String CHANNEL_TIMEOUT_WRITE_LAC = "TIMEOUT_WRITE_LAC";
    public final static String CHANNEL_READ_LAC_OP = "READ_LAC";
    public final static String CHANNEL_TIMEOUT_READ_LAC = "TIMEOUT_READ_LAC";
    public final static String TIMEOUT_GET_BOOKIE_INFO = "TIMEOUT_GET_BOOKIE_INFO";
    public final static String CHANNEL_START_TLS_OP = "START_TLS";
    public final static String CHANNEL_TIMEOUT_START_TLS_OP = "TIMEOUT_START_TLS";
}
