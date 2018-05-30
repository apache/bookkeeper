/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.impl.internal.mr;

import static org.apache.bookkeeper.clients.impl.internal.ProtocolInternalUtils.createMetaRangeException;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.utils.ListenableFutureRpcProcessor;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesRequest;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * Request Processor processing meta range request.
 */
public class MetaRangeRequestProcessor<RespT>
    extends ListenableFutureRpcProcessor<GetActiveRangesRequest, GetActiveRangesResponse, RespT> {

    public static <T> MetaRangeRequestProcessor<T> of(
        GetActiveRangesRequest request,
        Function<GetActiveRangesResponse, T> responseFunc,
        StorageContainerChannel channel,
        ScheduledExecutorService executor,
        Backoff.Policy backoffPolicy) {
        return new MetaRangeRequestProcessor<>(request, responseFunc, channel, executor, backoffPolicy);
    }

    private final GetActiveRangesRequest request;
    private final Function<GetActiveRangesResponse, RespT> responseFunc;

    private MetaRangeRequestProcessor(GetActiveRangesRequest request,
                                      Function<GetActiveRangesResponse, RespT> responseFunc,
                                      StorageContainerChannel channel,
                                      ScheduledExecutorService executor,
                                      Backoff.Policy backoffPolicy) {
        super(channel, executor, backoffPolicy);
        this.request = request;
        this.responseFunc = responseFunc;
    }

    @Override
    protected GetActiveRangesRequest createRequest() {
        return request;
    }

    @Override
    protected ListenableFuture<GetActiveRangesResponse> sendRPC(StorageServerChannel rsChannel,
                                                                GetActiveRangesRequest request) {
        return rsChannel.getMetaRangeService().getActiveRanges(request);
    }

    private String getIdentifier(GetActiveRangesRequest request) {

        return "" + request.getStreamId();
    }

    @Override
    protected RespT processResponse(GetActiveRangesResponse response) throws Exception {
        if (StatusCode.SUCCESS == response.getCode()) {
            return responseFunc.apply(response);
        }

        throw createMetaRangeException(getIdentifier(request), response.getCode());
    }
}
