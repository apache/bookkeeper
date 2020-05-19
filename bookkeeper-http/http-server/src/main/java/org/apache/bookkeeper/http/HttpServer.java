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

/**
 * Provide the interface to start, stop bookkeeper http server.
 * It also provide the interface to inject service provider,
 * which is the implementation of services for http endpoints.
 */
public interface HttpServer {

    /**
     * Http Status Code.
     */
    enum StatusCode {
        OK(200),
        REDIRECT(302),
        BAD_REQUEST(400),
        FORBIDDEN(403),
        NOT_FOUND(404),
        INTERNAL_ERROR(500),
        SERVICE_UNAVAILABLE(503);

        private int value;

        StatusCode(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Http Request Method.
     */
    enum Method {
        GET,
        POST,
        PUT,
        DELETE
    }

    /**
     * Http ApiTypes.
     */
    enum ApiType {
        HEARTBEAT,
        SERVER_CONFIG,
        METRICS,

        // ledger
        DELETE_LEDGER,
        LIST_LEDGER,
        GET_LEDGER_META,
        READ_LEDGER_ENTRY,
        // bookie
        LIST_BOOKIES,
        LIST_BOOKIE_INFO,
        LAST_LOG_MARK,
        LIST_DISK_FILE,
        EXPAND_STORAGE,
        GC,
        GC_DETAILS,
        BOOKIE_STATE,
        BOOKIE_IS_READY,
        BOOKIE_INFO,

        // autorecovery
        AUTORECOVERY_STATUS,
        RECOVERY_BOOKIE,
        LIST_UNDER_REPLICATED_LEDGER,
        WHO_IS_AUDITOR,
        TRIGGER_AUDIT,
        LOST_BOOKIE_RECOVERY_DELAY,
        DECOMMISSION
    }

    /**
     * Initialize the HTTP server with underline service provider.
     */
    void initialize(HttpServiceProvider httpServiceProvider);

    /**
     * Start the HTTP server on given port.
     */
    boolean startServer(int port);

    /**
     * Stop the HTTP server.
     */
    void stopServer();

    /**
     * Check whether the HTTP server is still running.
     */
    boolean isRunning();
}
