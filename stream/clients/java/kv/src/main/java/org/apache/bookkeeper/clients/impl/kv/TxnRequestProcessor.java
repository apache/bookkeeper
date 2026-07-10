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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.impl.kv;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.bookkeeper.clients.exceptions.InternalServerException;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.utils.ListenableFutureRpcProcessor;
import org.apache.bookkeeper.common.util.Backoff.Policy;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * Request Processor processing table request.
 */
class TxnRequestProcessor<RespT>
    extends ListenableFutureRpcProcessor<TxnRequest, TxnResponse, RespT> {

    public static <T> TxnRequestProcessor<T> of(
        Supplier<TxnRequest> requestSupplier,
        Function<TxnResponse, T> responseFunc,
        StorageContainerChannel channel,
        ScheduledExecutorService executor,
        Policy backoffPolicy) {
        return new TxnRequestProcessor<>(requestSupplier, responseFunc, channel, executor, backoffPolicy);
    }

    private final Supplier<TxnRequest> requestSupplier;
    private final Function<TxnResponse, RespT> responseFunc;

    private TxnRequestProcessor(Supplier<TxnRequest> requestSupplier,
                                Function<TxnResponse, RespT> respFunc,
                                StorageContainerChannel channel,
                                ScheduledExecutorService executor,
                                Policy backoffPolicy) {
        super(channel, executor, backoffPolicy);
        this.requestSupplier = requestSupplier;
        this.responseFunc = respFunc;
    }

    @Override
    protected TxnRequest createRequest() {
        // Serializing a request drains the ByteBuf slices stored in it, so a request instance
        // must not be reused across RPC attempts: build a fresh request for every attempt.
        return requestSupplier.get();
    }

    @Override
    protected ListenableFuture<TxnResponse> sendRPC(StorageServerChannel rsChannel,
                                                    TxnRequest request) {
        return rsChannel.getTableService().txn(request);
    }

    @Override
    protected RespT processResponse(TxnResponse response) throws Exception {
        if (StatusCode.SUCCESS == response.getHeader().getCode()) {
            return responseFunc.apply(response);
        }
        throw new InternalServerException("Encountered internal server exception : code = "
            + response.getHeader().getCode());
    }
}
