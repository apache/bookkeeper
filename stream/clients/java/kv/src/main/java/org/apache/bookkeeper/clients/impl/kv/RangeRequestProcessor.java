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
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * Request Processor processing table request.
 */
class RangeRequestProcessor<RespT>
    extends ListenableFutureRpcProcessor<RangeRequest, RangeResponse, RespT> {

    public static <T> RangeRequestProcessor<T> of(
        RangeRequest request,
        Function<RangeResponse, T> responseFunc,
        StorageContainerChannel channel,
        ScheduledExecutorService executor,
        Policy backoffPolicy) {
        return new RangeRequestProcessor<>(request, responseFunc, channel, executor, backoffPolicy);
    }

    private final RangeRequest request;
    private final Function<RangeResponse, RespT> responseFunc;

    private RangeRequestProcessor(RangeRequest request,
                                  Function<RangeResponse, RespT> respFunc,
                                  StorageContainerChannel channel,
                                  ScheduledExecutorService executor,
                                  Policy backoffPolicy) {
        super(channel, executor, backoffPolicy);
        this.request = request;
        this.responseFunc = respFunc;
    }

    @Override
    protected RangeRequest createRequest() {
        return request;
    }

    @Override
    protected ListenableFuture<RangeResponse> sendRPC(StorageServerChannel rsChannel,
                                                      RangeRequest request) {
        return rsChannel.getTableService().range(request);
    }

    @Override
    protected RespT processResponse(RangeResponse response) throws Exception {
        if (StatusCode.SUCCESS == response.getHeader().getCode()) {
            return responseFunc.apply(response);
        }
        throw new InternalServerException("Encountered internal server exception : code = "
            + response.getHeader().getCode());
    }
}
