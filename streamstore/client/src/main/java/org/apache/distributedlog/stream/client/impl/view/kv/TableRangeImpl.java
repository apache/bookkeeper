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

import static org.apache.distributedlog.stream.client.utils.KvUtils.newDeleteRequest;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newDeleteResult;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newGetResult;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newKvDeleteRequest;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newKvPutRequest;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newKvRangeRequest;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newPutRequest;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newPutResult;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newRangeRequest;

import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.distributedlog.stream.api.view.kv.Table;
import org.apache.distributedlog.stream.api.view.kv.options.DeleteOption;
import org.apache.distributedlog.stream.api.view.kv.options.GetOption;
import org.apache.distributedlog.stream.api.view.kv.options.PutOption;
import org.apache.distributedlog.stream.api.view.kv.result.DeleteResult;
import org.apache.distributedlog.stream.api.view.kv.result.GetResult;
import org.apache.distributedlog.stream.api.view.kv.result.PutResult;
import org.apache.distributedlog.stream.client.impl.StorageContainerChannel;
import org.apache.distributedlog.stream.proto.RangeProperties;
import org.apache.distributedlog.stream.proto.kv.rpc.RoutingHeader;

/**
 * A range of a table.
 */
class TableRangeImpl implements Table {

  private final long streamId;
  private final RangeProperties rangeProps;
  private final StorageContainerChannel scChannel;
  private final ScheduledExecutorService executor;

  TableRangeImpl(long streamId,
                 RangeProperties rangeProps,
                 StorageContainerChannel scChannel,
                 ScheduledExecutorService executor) {
    this.streamId = streamId;
    this.rangeProps = rangeProps;
    this.scChannel = scChannel;
    this.executor = executor;
  }

  private RoutingHeader.Builder newRoutingHeader(ByteBuf pKey) {
    return RoutingHeader.newBuilder()
      .setStreamId(streamId)
      .setRangeId(rangeProps.getRangeId())
      .setRKey(UnsafeByteOperations.unsafeWrap(pKey.nioBuffer()));
  }

  @Override
  public CompletableFuture<GetResult> get(ByteBuf pKey,
                                          ByteBuf lKey,
                                          GetOption option) {
    pKey.retain();
    lKey.retain();
    if (option.endKey().isPresent()) {
      option.endKey().get().retain();
    }
    return TableRequestProcessor.of(
      newKvRangeRequest(
        scChannel.getStorageContainerId(),
        newRangeRequest(lKey, option)
          .setHeader(newRoutingHeader(pKey))),
      response -> newGetResult(response.getKvRangeResp()),
      scChannel,
      executor
    ).process().whenComplete((value, cause) -> {
      pKey.release();
      lKey.release();
      if (option.endKey().isPresent()) {
        option.endKey().get().release();
      }
    });
  }

  @Override
  public CompletableFuture<PutResult> put(ByteBuf pKey,
                                          ByteBuf lKey,
                                          ByteBuf value,
                                          PutOption option) {
    pKey.retain();
    lKey.retain();
    return TableRequestProcessor.of(
      newKvPutRequest(
        scChannel.getStorageContainerId(),
        newPutRequest(lKey, value, option)
          .setHeader(newRoutingHeader(pKey))),
      response -> newPutResult(response.getKvPutResp()),
      scChannel,
      executor
    ).process().whenComplete((ignored, cause) -> {
      pKey.release();
      lKey.release();
    });
  }

  @Override
  public CompletableFuture<DeleteResult> delete(ByteBuf pKey,
                                                ByteBuf lKey,
                                                DeleteOption option) {
    pKey.retain();
    lKey.retain();
    if (option.endKey().isPresent()) {
      option.endKey().get().retain();
    }
    return TableRequestProcessor.of(
      newKvDeleteRequest(
        scChannel.getStorageContainerId(),
        newDeleteRequest(lKey, option)
          .setHeader(newRoutingHeader(pKey))),
      response -> newDeleteResult(response.getKvDeleteResp()),
      scChannel,
      executor
    ).process().whenComplete((ignored, cause) -> {
      pKey.release();
      lKey.release();
      if (option.endKey().isPresent()) {
        option.endKey().get().release();
      }
    });
  }

  @Override
  public void close() {
    // no-op
  }
}
