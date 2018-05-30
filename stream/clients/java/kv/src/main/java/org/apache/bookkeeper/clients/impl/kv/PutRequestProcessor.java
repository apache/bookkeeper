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
import org.apache.bookkeeper.clients.exceptions.InternalServerException;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.utils.ListenableFutureRpcProcessor;
import org.apache.bookkeeper.common.util.Backoff.Policy;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * Request Processor processing table request.
 */
class PutRequestProcessor<RespT>
    extends ListenableFutureRpcProcessor<PutRequest, PutResponse, RespT> {

    public static <T> PutRequestProcessor<T> of(
        PutRequest request,
        Function<PutResponse, T> responseFunc,
        StorageContainerChannel channel,
        ScheduledExecutorService executor,
        Policy backoffPolicy) {
        return new PutRequestProcessor<>(request, responseFunc, channel, executor, backoffPolicy);
    }

    private final PutRequest request;
    private final Function<PutResponse, RespT> responseFunc;

    private PutRequestProcessor(PutRequest request,
                                Function<PutResponse, RespT> respFunc,
                                StorageContainerChannel channel,
                                ScheduledExecutorService executor,
                                Policy backoffPolicy) {
        super(channel, executor, backoffPolicy);
        this.request = request;
        this.responseFunc = respFunc;
    }

    @Override
    protected PutRequest createRequest() {
        return request;
    }

    @Override
    protected ListenableFuture<PutResponse> sendRPC(StorageServerChannel rsChannel,
                                                    PutRequest request) {
        return rsChannel.getTableService().put(request);
    }

    @Override
    protected RespT processResponse(PutResponse response) throws Exception {
        if (StatusCode.SUCCESS == response.getHeader().getCode()) {
            return responseFunc.apply(response);
        }
        throw new InternalServerException("Encountered internal server exception : code = "
            + response.getHeader().getCode());
    }
}
