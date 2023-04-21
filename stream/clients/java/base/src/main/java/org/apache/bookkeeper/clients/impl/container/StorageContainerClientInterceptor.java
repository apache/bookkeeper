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

package org.apache.bookkeeper.clients.impl.container;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.SC_ID_KEY;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors.CheckedForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.grpc.netty.LongBinaryMarshaller;

/**
 * A client interceptor that intercepting outgoing calls to storage containers.
 */
@Slf4j
public class StorageContainerClientInterceptor implements ClientInterceptor {

    private final long scId;
    private final Metadata.Key<Long> scIdKey;

    public StorageContainerClientInterceptor(long scId) {
        this.scId = scId;
        this.scIdKey = Metadata.Key.of(
            SC_ID_KEY,
            LongBinaryMarshaller.of());
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions,
                                                               Channel next) {
        if (log.isTraceEnabled()) {
            log.trace("Intercepting method {} : req marshaller = {}, resp marshaller = {}",
                method.getFullMethodName(),
                method.getRequestMarshaller(),
                method.getResponseMarshaller());
        }
        return new CheckedForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            @Override
            protected void checkedStart(Listener<RespT> responseListener,
                                        Metadata headers) throws Exception {
                if (log.isTraceEnabled()) {
                    log.trace("Attaching storage container {},", scId);
                }
                headers.put(scIdKey, scId);
                delegate().start(responseListener, headers);
            }

            @Override
            public void request(int numMessages) {
                if (log.isTraceEnabled()) {
                    log.trace("request {} messages", numMessages);
                }
                super.request(numMessages);
            }
        };
    }
}
