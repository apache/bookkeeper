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

public interface HttpServer {

    static final String HEARTBEAT             = "/heartbeat";
    static final String SERVER_CONFIG         = "/api/config/serverConfig";
    static final String BOOKIE_STATUS         = "/api/bookie/bookieStatus";

    static enum StatusCode {
        OK(200),
        REDIRECT(302),
        NOT_FOUND(404),
        INTERNAL_ERROR(500);

        private int value;

        StatusCode(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Initialize the HTTP server with the given options
     */
    void initialize(ServerOptions serverOptions);

    /**
     * Start the HTTP server
     */
    void startServer();

    /**
     * Stop the HTTP server
     */
    void stopServer();

    /**
     * Check whether the HTTP is still running
     */
    boolean isRunning();
}
