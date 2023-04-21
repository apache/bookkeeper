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
    public static final String METRICS                      = "/metrics";

    // ledger
    public static final String DELETE_LEDGER                = "/api/v1/ledger/delete";
    public static final String LIST_LEDGER                  = "/api/v1/ledger/list";
    public static final String GET_LEDGER_META              = "/api/v1/ledger/metadata";
    public static final String READ_LEDGER_ENTRY            = "/api/v1/ledger/read";
    // bookie
    public static final String LIST_BOOKIES                 = "/api/v1/bookie/list_bookies";
    public static final String LIST_BOOKIE_INFO             = "/api/v1/bookie/list_bookie_info";
    public static final String LAST_LOG_MARK                = "/api/v1/bookie/last_log_mark";
    public static final String LIST_DISK_FILE               = "/api/v1/bookie/list_disk_file";
    public static final String EXPAND_STORAGE               = "/api/v1/bookie/expand_storage";
    public static final String GC                           = "/api/v1/bookie/gc";
    public static final String GC_DETAILS                   = "/api/v1/bookie/gc_details";
    public static final String SUSPEND_GC_COMPACTION        = "/api/v1/bookie/gc/suspend_compaction";
    public static final String RESUME_GC_COMPACTION         = "/api/v1/bookie/gc/resume_compaction";
    public static final String BOOKIE_STATE                 = "/api/v1/bookie/state";
    public static final String BOOKIE_SANITY                 = "/api/v1/bookie/sanity";
    public static final String BOOKIE_STATE_READONLY        = "/api/v1/bookie/state/readonly";
    public static final String BOOKIE_IS_READY              = "/api/v1/bookie/is_ready";
    public static final String BOOKIE_INFO                  = "/api/v1/bookie/info";
    public static final String CLUSTER_INFO                  = "/api/v1/bookie/cluster_info";
    public static final String ENTRY_LOCATION_COMPACT       = "/api/v1/bookie/entry_location_compact";
    // autorecovery
    public static final String AUTORECOVERY_STATUS          = "/api/v1/autorecovery/status";
    public static final String RECOVERY_BOOKIE              = "/api/v1/autorecovery/bookie";
    public static final String LIST_UNDER_REPLICATED_LEDGER = "/api/v1/autorecovery/list_under_replicated_ledger";
    public static final String WHO_IS_AUDITOR               = "/api/v1/autorecovery/who_is_auditor";
    public static final String TRIGGER_AUDIT                = "/api/v1/autorecovery/trigger_audit";
    public static final String LOST_BOOKIE_RECOVERY_DELAY   = "/api/v1/autorecovery/lost_bookie_recovery_delay";
    public static final String DECOMMISSION                 = "/api/v1/autorecovery/decommission";


    private final Map<String, Handler> endpointHandlers = new HashMap<>();

    public HttpRouter(AbstractHttpHandlerFactory<Handler> handlerFactory) {
        this.endpointHandlers.put(HEARTBEAT, handlerFactory.newHandler(HttpServer.ApiType.HEARTBEAT));
        this.endpointHandlers.put(SERVER_CONFIG, handlerFactory.newHandler(HttpServer.ApiType.SERVER_CONFIG));
        this.endpointHandlers.put(METRICS, handlerFactory.newHandler(HttpServer.ApiType.METRICS));

        // ledger
        this.endpointHandlers.put(DELETE_LEDGER, handlerFactory.newHandler(HttpServer.ApiType.DELETE_LEDGER));
        this.endpointHandlers.put(LIST_LEDGER, handlerFactory.newHandler(HttpServer.ApiType.LIST_LEDGER));
        this.endpointHandlers.put(GET_LEDGER_META, handlerFactory.newHandler(HttpServer.ApiType.GET_LEDGER_META));
        this.endpointHandlers.put(READ_LEDGER_ENTRY, handlerFactory.newHandler(HttpServer.ApiType.READ_LEDGER_ENTRY));

        // bookie
        this.endpointHandlers.put(LIST_BOOKIES, handlerFactory.newHandler(HttpServer.ApiType.LIST_BOOKIES));
        this.endpointHandlers.put(LIST_BOOKIE_INFO, handlerFactory.newHandler(HttpServer.ApiType.LIST_BOOKIE_INFO));
        this.endpointHandlers.put(LAST_LOG_MARK, handlerFactory.newHandler(HttpServer.ApiType.LAST_LOG_MARK));
        this.endpointHandlers.put(LIST_DISK_FILE, handlerFactory.newHandler(HttpServer.ApiType.LIST_DISK_FILE));
        this.endpointHandlers.put(EXPAND_STORAGE, handlerFactory.newHandler(HttpServer.ApiType.EXPAND_STORAGE));
        this.endpointHandlers.put(GC, handlerFactory.newHandler(HttpServer.ApiType.GC));
        this.endpointHandlers.put(GC_DETAILS, handlerFactory.newHandler(HttpServer.ApiType.GC_DETAILS));
        this.endpointHandlers.put(BOOKIE_STATE, handlerFactory.newHandler(HttpServer.ApiType.BOOKIE_STATE));
        this.endpointHandlers.put(BOOKIE_SANITY, handlerFactory.newHandler(HttpServer.ApiType.BOOKIE_SANITY));
        this.endpointHandlers.put(BOOKIE_STATE_READONLY,
                handlerFactory.newHandler(HttpServer.ApiType.BOOKIE_STATE_READONLY));
        this.endpointHandlers.put(BOOKIE_IS_READY, handlerFactory.newHandler(HttpServer.ApiType.BOOKIE_IS_READY));
        this.endpointHandlers.put(BOOKIE_INFO, handlerFactory.newHandler(HttpServer.ApiType.BOOKIE_INFO));
        this.endpointHandlers.put(CLUSTER_INFO, handlerFactory.newHandler(HttpServer.ApiType.CLUSTER_INFO));
        this.endpointHandlers.put(SUSPEND_GC_COMPACTION,
            handlerFactory.newHandler(HttpServer.ApiType.SUSPEND_GC_COMPACTION));
        this.endpointHandlers.put(RESUME_GC_COMPACTION,
            handlerFactory.newHandler(HttpServer.ApiType.RESUME_GC_COMPACTION));
        this.endpointHandlers.put(ENTRY_LOCATION_COMPACT,
                handlerFactory.newHandler(HttpServer.ApiType.TRIGGER_ENTRY_LOCATION_COMPACT));

        // autorecovery
        this.endpointHandlers.put(AUTORECOVERY_STATUS, handlerFactory
                .newHandler(HttpServer.ApiType.AUTORECOVERY_STATUS));
        this.endpointHandlers.put(RECOVERY_BOOKIE, handlerFactory.newHandler(HttpServer.ApiType.RECOVERY_BOOKIE));
        this.endpointHandlers.put(LIST_UNDER_REPLICATED_LEDGER,
            handlerFactory.newHandler(HttpServer.ApiType.LIST_UNDER_REPLICATED_LEDGER));
        this.endpointHandlers.put(WHO_IS_AUDITOR, handlerFactory.newHandler(HttpServer.ApiType.WHO_IS_AUDITOR));
        this.endpointHandlers.put(TRIGGER_AUDIT, handlerFactory.newHandler(HttpServer.ApiType.TRIGGER_AUDIT));
        this.endpointHandlers.put(LOST_BOOKIE_RECOVERY_DELAY,
            handlerFactory.newHandler(HttpServer.ApiType.LOST_BOOKIE_RECOVERY_DELAY));
        this.endpointHandlers.put(DECOMMISSION, handlerFactory.newHandler(HttpServer.ApiType.DECOMMISSION));
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
