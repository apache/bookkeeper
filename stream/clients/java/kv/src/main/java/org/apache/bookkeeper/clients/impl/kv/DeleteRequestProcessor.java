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
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * Request Processor processing table request.
 */
class DeleteRequestProcessor<RespT>
    extends ListenableFutureRpcProcessor<DeleteRangeRequest, DeleteRangeResponse, RespT> {

    public static <T> DeleteRequestProcessor<T> of(
        DeleteRangeRequest request,
        Function<DeleteRangeResponse, T> responseFunc,
        StorageContainerChannel channel,
        ScheduledExecutorService executor,
        Policy backoffPolicy) {
        return new DeleteRequestProcessor<>(request, responseFunc, channel, executor, backoffPolicy);
    }

    private final DeleteRangeRequest request;
    private final Function<DeleteRangeResponse, RespT> responseFunc;

    private DeleteRequestProcessor(DeleteRangeRequest request,
                                   Function<DeleteRangeResponse, RespT> respFunc,
                                   StorageContainerChannel channel,
                                   ScheduledExecutorService executor,
                                   Policy backoffPolicy) {
        super(channel, executor, backoffPolicy);
        this.request = request;
        this.responseFunc = respFunc;
    }

    @Override
    protected DeleteRangeRequest createRequest() {
        return request;
    }

    @Override
    protected ListenableFuture<DeleteRangeResponse> sendRPC(StorageServerChannel rsChannel,
                                                            DeleteRangeRequest request) {
        return rsChannel.getTableService().delete(request);
    }

    @Override
    protected RespT processResponse(DeleteRangeResponse response) throws Exception {
        if (StatusCode.SUCCESS == response.getHeader().getCode()) {
            return responseFunc.apply(response);
        }
        throw new InternalServerException("Encountered internal server exception : code = "
            + response.getHeader().getCode());
    }
}
