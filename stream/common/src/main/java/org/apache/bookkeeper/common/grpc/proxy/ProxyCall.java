/*
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

package org.apache.bookkeeper.common.grpc.proxy;

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;
import lombok.Getter;

/**
 * Proxy grpc calls.
 */
@Getter
class ProxyCall<ReqT, RespT> {

    private final RequestProxy serverCallListener;
    private final ResponseProxy clientCallListener;

    ProxyCall(ServerCall<ReqT, RespT> serverCall,
              ClientCall<ReqT, RespT> clientCall) {
        this.serverCallListener = new RequestProxy(clientCall);
        this.clientCallListener = new ResponseProxy(serverCall);
    }

    /**
     * Request proxy to delegate client call.
     */
    private class RequestProxy extends ServerCall.Listener<ReqT> {

        private final ClientCall<ReqT, ?> clientCall;
        private boolean needToRequest;

        public RequestProxy(ClientCall<ReqT, ?> clientCall) {
            this.clientCall = clientCall;
        }

        @Override
        public void onMessage(ReqT message) {
            clientCall.sendMessage(message);
            synchronized (this) {
                if (clientCall.isReady()) {
                    clientCallListener.serverCall.request(1);
                } else {
                    needToRequest = true;
                }
            }
        }

        @Override
        public void onHalfClose() {
            clientCall.halfClose();
        }

        @Override
        public void onCancel() {
            clientCall.cancel("Server cancelled", null);
        }

        @Override
        public void onReady() {
            clientCallListener.onServerReady();
        }

        synchronized void onClientReady() {
            if (needToRequest) {
                clientCallListener.serverCall.request(1);
                needToRequest = false;
            }
        }

    }

    /**
     * Response proxy to delegate server call.
     */
    private class ResponseProxy extends ClientCall.Listener<RespT> {

        private final ServerCall<?, RespT> serverCall;
        private boolean needToRequest;

        public ResponseProxy(ServerCall<?, RespT> serverCall) {
            this.serverCall = serverCall;
        }

        @Override
        public void onHeaders(Metadata headers) {
            serverCall.sendHeaders(headers);
        }

        @Override
        public void onMessage(RespT message) {
            serverCall.sendMessage(message);
            synchronized (this) {
                if (serverCall.isReady()) {
                    serverCallListener.clientCall.request(1);
                } else {
                    needToRequest = true;
                }
            }
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
            serverCall.close(status, trailers);
        }

        @Override
        public void onReady() {
            serverCallListener.onClientReady();
        }

        synchronized void onServerReady() {
            if (needToRequest) {
                serverCallListener.clientCall.request(1);
                needToRequest = false;
            }
        }

    }

}
