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

package org.apache.distributedlog.stream.client.impl.internal.mr;

import static org.apache.distributedlog.stream.client.impl.internal.ProtocolInternalUtils.createMetaRangeException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.apache.distributedlog.stream.client.impl.StorageContainerChannel;
import org.apache.distributedlog.stream.client.impl.channel.RangeServerChannel;
import org.apache.distributedlog.stream.client.utils.ListenableFutureRpcProcessor;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerResponse;

/**
 * Request Processor processing meta range request.
 */
public class MetaRangeRequestProcessor<RespT>
    extends ListenableFutureRpcProcessor<StorageContainerRequest, StorageContainerResponse, RespT> {

  public static <T> MetaRangeRequestProcessor<T> of(
      StorageContainerRequest request,
      Function<StorageContainerResponse, T> responseFunc,
      StorageContainerChannel channel,
      ScheduledExecutorService executor) {
    return new MetaRangeRequestProcessor<>(request, responseFunc, channel, executor);
  }

  private final StorageContainerRequest request;
  private final Function<StorageContainerResponse, RespT> responseFunc;

  private MetaRangeRequestProcessor(StorageContainerRequest request,
                                    Function<StorageContainerResponse, RespT> responseFunc,
                                    StorageContainerChannel channel,
                                    ScheduledExecutorService executor) {
    super(channel, executor);
    this.request = request;
    this.responseFunc = responseFunc;
  }

  @Override
  protected StorageContainerRequest createRequest() {
    return request;
  }

  @Override
  protected ListenableFuture<StorageContainerResponse> sendRPC(RangeServerChannel rsChannel,
                                                               StorageContainerRequest request) {
    switch (request.getType()) {
      case GET_ACTIVE_RANGES:
        return rsChannel.getMetaRangeService().getActiveRanges(request);
      default:
        SettableFuture<StorageContainerResponse> respFuture = SettableFuture.create();
        respFuture.setException(new Exception("Unknown request " + request));
        return respFuture;
    }
  }

  private String getIdentifier(StorageContainerRequest request) {
    switch (request.getType()) {
      case GET_ACTIVE_RANGES:
        return "" + request.getGetActiveRangesReq().getStreamId();
      default:
        return "";
    }
  }

  @Override
  protected RespT processResponse(StorageContainerResponse response) throws Exception {
    if (StatusCode.SUCCESS == response.getCode()) {
      return responseFunc.apply(response);
    }

    throw createMetaRangeException(getIdentifier(request), response.getCode());
  }
}
