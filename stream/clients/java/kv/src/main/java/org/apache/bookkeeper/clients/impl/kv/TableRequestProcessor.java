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
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.bookkeeper.clients.exceptions.InternalServerException;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.utils.ListenableFutureRpcProcessor;
import org.apache.bookkeeper.common.util.Backoff.Policy;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerResponse;

/**
 * Request Processor processing table request.
 */
public class TableRequestProcessor<RespT>
    extends ListenableFutureRpcProcessor<StorageContainerRequest, StorageContainerResponse, RespT> {

    public static <T> TableRequestProcessor<T> of(
        StorageContainerRequest request,
        Function<StorageContainerResponse, T> responseFunc,
        StorageContainerChannel channel,
        ScheduledExecutorService executor,
        Policy backoffPolicy) {
        return new TableRequestProcessor<>(request, responseFunc, channel, executor, backoffPolicy);
    }

    private final StorageContainerRequest request;
    private final Function<StorageContainerResponse, RespT> responseFunc;

    private TableRequestProcessor(StorageContainerRequest request,
                                  Function<StorageContainerResponse, RespT> respFunc,
                                  StorageContainerChannel channel,
                                  ScheduledExecutorService executor,
                                  Policy backoffPolicy) {
        super(channel, executor, backoffPolicy);
        this.request = request;
        this.responseFunc = respFunc;
    }

    @Override
    protected StorageContainerRequest createRequest() {
        return request;
    }

    @Override
    protected ListenableFuture<StorageContainerResponse> sendRPC(StorageServerChannel rsChannel,
                                                                 StorageContainerRequest request) {
        switch (request.getRequestCase()) {
            case KV_RANGE_REQ:
                return rsChannel.getTableService().range(request);
            case KV_PUT_REQ:
                return rsChannel.getTableService().put(request);
            case KV_DELETE_REQ:
                return rsChannel.getTableService().delete(request);
            case KV_INCR_REQ:
                return rsChannel.getTableService().increment(request);
            case KV_TXN_REQ:
                return rsChannel.getTableService().txn(request);
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
