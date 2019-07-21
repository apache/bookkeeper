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
package org.apache.bookkeeper.util;

/**
 * This class contains constants used in BookKeeper.
 */
public class BookKeeperConstants {

    // //////////////////////////
    // /////Basic constants//////
    // //////////////////////////
    public static final String LEDGER_NODE_PREFIX = "L";
    public static final String COLON = ":";
    public static final String VERSION_FILENAME = "VERSION";
    public static final String BOOKIE_STATUS_FILENAME = "BOOKIE_STATUS";
    public static final String PASSWD = "passwd";
    public static final String CURRENT_DIR = "current";
    public static final String READONLY = "readonly";

    // //////////////////////////
    // ///// Znodes//////////////
    // //////////////////////////
    public static final String AVAILABLE_NODE = "available";
    public static final String COOKIE_NODE = "cookies";
    public static final String UNDER_REPLICATION_NODE = "underreplication";
    public static final String UNDER_REPLICATION_LOCK = "locks";
    public static final String DISABLE_NODE = "disable";
    public static final String LOSTBOOKIERECOVERYDELAY_NODE = "lostBookieRecoveryDelay";
    public static final String CHECK_ALL_LEDGERS_CTIME = "checkallledgersctime";
    public static final String PLACEMENT_POLICY_CHECK_CTIME = "placementpolicycheckctime";
    public static final String REPLICAS_CHECK_CTIME = "replicascheckctime";
    public static final String DEFAULT_ZK_LEDGERS_ROOT_PATH = "/ledgers";
    public static final String LAYOUT_ZNODE = "LAYOUT";
    public static final String INSTANCEID = "INSTANCEID";

    /**
     * Set the max log size limit to 1GB. It makes extra room for entry log file before
     * hitting hard limit '2GB'. So we don't need to force roll entry log file when flushing
     * memtable (for performance consideration)
     */
    public static final long MAX_LOG_SIZE_LIMIT = 1 * 1024 * 1024 * 1024;

    public static final String FEATURE_REPP_DISABLE_DURABILITY_ENFORCEMENT = "repp_disable_durability_enforcement";
    public static final String FEATURE_DISABLE_ENSEMBLE_CHANGE = "disable_ensemble_change";

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
}
