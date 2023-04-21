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

package org.apache.bookkeeper.stream.storage.impl.sc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import javax.annotation.Nullable;

/**
 * A channel that always throws {@link Status#NOT_FOUND}.
 */
final class Channel404 extends Channel {

    static Channel404 of() {
        return INSTANCE;
    }

    private static final Channel404 INSTANCE = new Channel404();

    private static final ClientCall<Object, Object> CALL404 = new ClientCall<Object, Object>() {
        @Override
        public void start(Listener<Object> responseListener, Metadata headers) {
            responseListener.onClose(Status.NOT_FOUND, new Metadata());
        }

        @Override
        public void request(int numMessages) {
            // no-op
        }

        @Override
        public void cancel(@Nullable String message, @Nullable Throwable cause) {
            // no-op
        }

        @Override
        public void halfClose() {
            // no-op
        }

        @Override
        public void sendMessage(Object message) {
            // no-op
        }
    };

    private Channel404() {}

    @SuppressWarnings("unchecked")
    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
        MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        return (ClientCall<RequestT, ResponseT>) CALL404;
    }

    @Override
    public String authority() {
        return null;
    }
}
