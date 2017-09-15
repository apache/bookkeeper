/**
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
package org.apache.bookkeeper.http;

import java.util.HashMap;
import java.util.Map;

/**
 * Provide the mapping of http endpoints and handlers and function
 * to bind endpoint to the corresponding handler.
 */
public abstract class HttpRouter<Handler> {

  // Define endpoints here.
  public static final String HEARTBEAT                    = "/heartbeat";
  public static final String SERVER_CONFIG                = "/api/v1/config/server_config";
  // bookkeeper
  public static final String LIST_BOOKIES                 = "/api/v1/bookkeeper/list_bookies";
  public static final String UPDATE_COOKIE                = "/api/v1/bookkeeper/update_cookie";
  // ledger
  public static final String DELETE_LEDGER                = "/api/v1/ledger/delete";
  public static final String LIST_LEDGER                  = "/api/v1/ledger/list";
  public static final String GET_LEDGER_META              = "/api/v1/ledger/metadata";
  public static final String READ_LEDGER_ENTRY            = "/api/v1/ledger/read";
  // bookie
  public static final String LIST_BOOKIE_INFO             = "/api/v1/bookie/list_bookie_info";
  public static final String LAST_LOG_MARK                = "/api/v1/bookie/last_log_mark";
  public static final String LIST_DISK_FILE               = "/api/v1/bookie/list_disk_file";
  public static final String READ_ENTRY_LOG               = "/api/v1/bookie/read_entry_log";
  public static final String READ_JOURNAL_FILE            = "/api/v1/bookie/read_journal_file";
  public static final String EXPAND_STORAGE               = "/api/v1/bookie/expand_storage";
  // autorecovery
  public static final String RECOVERY_BOOKIE              = "/api/v1/autorecovery/bookie";
  public static final String LIST_UNDER_REPLICAETD_LEDGER = "/api/v1/autorecovery/list_under_replicated_ledger";
  public static final String WHO_IS_AUDITOR               = "/api/v1/autorecovery/who_is_auditor";
  public static final String TRIGGER_AUDIT                = "/api/v1/autorecovery/trigger_audit";
  public static final String LOST_BOOKIE_RECOVERY_DELAY   = "/api/v1/autorecovery/lost_bookie_recovery_delay";
  public static final String DECOMMISSION                 = "/api/v1/autorecovery/decommission";


  private final Map<String, Handler> endpointHandlers = new HashMap<>();

  public HttpRouter(AbstractHttpHandlerFactory<Handler> handlerFactory) {
    this.endpointHandlers.put(HEARTBEAT, handlerFactory.newHeartbeatHandler());
    this.endpointHandlers.put(SERVER_CONFIG, handlerFactory.newConfigurationHandler());

    // bookkeeper
    this.endpointHandlers.put(LIST_BOOKIES, handlerFactory.newListBookiesHandler());
    this.endpointHandlers.put(UPDATE_COOKIE, handlerFactory.newUpdataCookieHandler());

    // ledger
    this.endpointHandlers.put(DELETE_LEDGER, handlerFactory.newDeleteLedgerHandler());
    this.endpointHandlers.put(LIST_LEDGER, handlerFactory.newListLedgerHandler());
    this.endpointHandlers.put(GET_LEDGER_META, handlerFactory.newGetLedgerMetaHandler());
    this.endpointHandlers.put(READ_LEDGER_ENTRY, handlerFactory.newReadLedgerEntryHandler());

    // bookie
    this.endpointHandlers.put(LIST_BOOKIE_INFO, handlerFactory.newListBookieInfoHandler());
    this.endpointHandlers.put(LAST_LOG_MARK, handlerFactory.newGetLastLogMarkHandler());
    this.endpointHandlers.put(LIST_DISK_FILE, handlerFactory.newListDiskFileHandler());
    this.endpointHandlers.put(READ_ENTRY_LOG, handlerFactory.newReadEntryLogHandler());
    this.endpointHandlers.put(READ_JOURNAL_FILE, handlerFactory.newReadJournalFileHandler());
    this.endpointHandlers.put(EXPAND_STORAGE, handlerFactory.newExpandStorageHandler());

    // autorecovery
    this.endpointHandlers.put(RECOVERY_BOOKIE, handlerFactory.newRecoveryBookieHandler());
    this.endpointHandlers.put(LIST_UNDER_REPLICAETD_LEDGER, handlerFactory.newListUnderReplicatedLedgerHandler());
    this.endpointHandlers.put(WHO_IS_AUDITOR, handlerFactory.newWhoIsAuditorHandler());
    this.endpointHandlers.put(TRIGGER_AUDIT, handlerFactory.newTriggerAuditHandler());
    this.endpointHandlers.put(LOST_BOOKIE_RECOVERY_DELAY, handlerFactory.newLostBookieRecoveryDelayHandler());
    this.endpointHandlers.put(DECOMMISSION, handlerFactory.newDecommissionHandler());
  }

  /**
   * Bind all endpoints to corresponding handlers.
   */
  public void bindAll() {
    for (Map.Entry<String, Handler> entry : endpointHandlers.entrySet()) {
      bindHandler(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Bind the given endpoint to its corresponding handlers.
   * @param endpoint http endpoint
   * @param handler http handler
   */
  public abstract void bindHandler(String endpoint, Handler handler);

}
