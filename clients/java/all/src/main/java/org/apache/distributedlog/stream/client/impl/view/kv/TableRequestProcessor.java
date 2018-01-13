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

package org.apache.distributedlog.stream.client.impl.view.kv;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.apache.distributedlog.stream.client.exceptions.InternalServerException;
import org.apache.distributedlog.stream.client.impl.StorageContainerChannel;
import org.apache.distributedlog.stream.client.impl.channel.RangeServerChannel;
import org.apache.distributedlog.stream.client.utils.ListenableFutureRpcProcessor;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerResponse;

/**
 * Request Processor processing table request.
 */
public class TableRequestProcessor<RespT>
    extends ListenableFutureRpcProcessor<StorageContainerRequest, StorageContainerResponse, RespT> {

  public static <T> TableRequestProcessor<T> of(
      StorageContainerRequest request,
      Function<StorageContainerResponse, T> responseFunc,
      StorageContainerChannel channel,
      ScheduledExecutorService executor) {
    return new TableRequestProcessor<>(request, responseFunc, channel, executor);
  }

  private final StorageContainerRequest request;
  private final Function<StorageContainerResponse, RespT> responseFunc;

  private TableRequestProcessor(StorageContainerRequest request,
                                Function<StorageContainerResponse, RespT> respFunc,
                                StorageContainerChannel channel,
                                ScheduledExecutorService executor) {
    super(channel, executor);
    this.request = request;
    this.responseFunc = respFunc;
  }

  @Override
  protected StorageContainerRequest createRequest() {
    return request;
  }

  @Override
  protected ListenableFuture<StorageContainerResponse> sendRPC(RangeServerChannel rsChannel,
                                                               StorageContainerRequest request) {
    switch (request.getType()) {
      case KV_RANGE:
        return rsChannel.getTableService().range(request);
      case KV_PUT:
        return rsChannel.getTableService().put(request);
      case KV_DELETE:
        return rsChannel.getTableService().delete(request);
      default:
        SettableFuture<StorageContainerResponse> respFuture = SettableFuture.create();
        respFuture.setException(new Exception("Unknown request " + request));
        return respFuture;
    }
  }

  @Override
  protected RespT processResponse(StorageContainerResponse response) throws Exception {
    if (StatusCode.SUCCESS == response.getCode()) {
      return responseFunc.apply(response);
    }
    throw new InternalServerException("Encountered internal server exception : code = "
      + response.getCode());
  }
}
