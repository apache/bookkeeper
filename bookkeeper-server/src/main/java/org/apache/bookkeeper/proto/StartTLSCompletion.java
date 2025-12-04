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

package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.client.BKException;

class StartTLSCompletion extends CompletionValue {
    final BookkeeperInternalCallbacks.StartTLSCallback cb;

    public StartTLSCompletion(final CompletionKey key, PerChannelBookieClient perChannelBookieClient) {
        super("StartTLS", null, -1, -1, perChannelBookieClient);
        this.opLogger = perChannelBookieClient.startTLSOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.startTLSTimeoutOpLogger;
        this.cb = new BookkeeperInternalCallbacks.StartTLSCallback() {
            @Override
            public void startTLSComplete(int rc, Object ctx) {
                logOpResult(rc);
                key.release();
            }
        };
    }

    @Override
    public void errorOut() {
        errorOut(BKException.Code.BookieHandleNotAvailableException);
    }

    @Override
    public void errorOut(final int rc) {
        perChannelBookieClient.failTLS(rc);
    }

    @Override
    public void handleV3Response(BookkeeperProtocol.Response response) {
        BookkeeperProtocol.StatusCode status = response.getStatus();

        if (LOG.isDebugEnabled()) {
            logResponse(status);
        }

        int rc = convertStatus(status, BKException.Code.SecurityException);

        // Cancel START_TLS request timeout
        cb.startTLSComplete(rc, null);

        if (perChannelBookieClient.state != PerChannelBookieClient.ConnectionState.START_TLS) {
            LOG.error("Connection state changed before TLS response received");
            perChannelBookieClient.failTLS(BKException.Code.BookieHandleNotAvailableException);
        } else if (status != BookkeeperProtocol.StatusCode.EOK) {
            LOG.error("Client received error {} during TLS negotiation", status);
            perChannelBookieClient.failTLS(BKException.Code.SecurityException);
        } else {
            perChannelBookieClient.initTLSHandshake();
        }
    }
}
