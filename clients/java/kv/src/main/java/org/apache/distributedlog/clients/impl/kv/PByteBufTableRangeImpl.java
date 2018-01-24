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

package org.apache.distributedlog.clients.impl.kv;

import static org.apache.distributedlog.clients.impl.kv.KvUtils.toProtoCompare;
import static org.apache.distributedlog.clients.impl.kv.KvUtils.toProtoRequest;

import com.google.common.collect.Lists;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.api.kv.PTable;
import org.apache.distributedlog.api.kv.Txn;
import org.apache.distributedlog.api.kv.op.CompareOp;
import org.apache.distributedlog.api.kv.op.Op;
import org.apache.distributedlog.api.kv.op.OpFactory;
import org.apache.distributedlog.api.kv.options.DeleteOption;
import org.apache.distributedlog.api.kv.options.PutOption;
import org.apache.distributedlog.api.kv.options.RangeOption;
import org.apache.distributedlog.api.kv.result.DeleteResult;
import org.apache.distributedlog.api.kv.result.PutResult;
import org.apache.distributedlog.api.kv.result.RangeResult;
import org.apache.distributedlog.api.kv.result.TxnResult;
import org.apache.distributedlog.clients.impl.container.StorageContainerChannel;
import org.apache.distributedlog.clients.impl.kv.result.ResultFactory;
import org.apache.distributedlog.stream.proto.RangeProperties;
import org.apache.distributedlog.stream.proto.kv.rpc.RoutingHeader;
import org.apache.distributedlog.stream.proto.kv.rpc.TxnRequest;

/**
 * A range of a table.
 */
@Slf4j
class PByteBufTableRangeImpl implements PTable<ByteBuf, ByteBuf> {

  private final long streamId;
  private final RangeProperties rangeProps;
  private final StorageContainerChannel scChannel;
  private final ScheduledExecutorService executor;
  private final OpFactory<ByteBuf, ByteBuf> opFactory;
  private final ResultFactory<ByteBuf, ByteBuf> resultFactory;
  private final KeyValueFactory<ByteBuf, ByteBuf> kvFactory;

  PByteBufTableRangeImpl(long streamId,
                         RangeProperties rangeProps,
                         StorageContainerChannel scChannel,
                         ScheduledExecutorService executor,
                         OpFactory<ByteBuf, ByteBuf> opFactory,
                         ResultFactory<ByteBuf, ByteBuf> resultFactory,
                         KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
    this.streamId = streamId;
    this.rangeProps = rangeProps;
    this.scChannel = scChannel;
    this.executor = executor;
    this.opFactory = opFactory;
    this.resultFactory = resultFactory;
    this.kvFactory = kvFactory;
  }

  private RoutingHeader.Builder newRoutingHeader(ByteBuf pKey) {
    return RoutingHeader.newBuilder()
        .setStreamId(streamId)
        .setRangeId(rangeProps.getRangeId())
        .setRKey(UnsafeByteOperations.unsafeWrap(pKey.nioBuffer()));
  }

  @Override
  public CompletableFuture<RangeResult<ByteBuf, ByteBuf>> get(
      ByteBuf pKey, ByteBuf lKey, RangeOption<ByteBuf> option) {
    pKey.retain();
    lKey.retain();
    if (null != option.endKey()) {
      option.endKey().retain();
    }
    return TableRequestProcessor.of(
        KvUtils.newKvRangeRequest(
            scChannel.getStorageContainerId(),
            KvUtils.newRangeRequest(lKey, option)
                .setHeader(newRoutingHeader(pKey))),
        response -> KvUtils.newRangeResult(response.getKvRangeResp(), resultFactory, kvFactory),
        scChannel,
        executor
    ).process().whenComplete((value, cause) -> {
      pKey.release();
      lKey.release();
      if (null != option.endKey()) {
        option.endKey().release();
      }
    });
  }

  @Override
  public CompletableFuture<PutResult<ByteBuf, ByteBuf>> put(ByteBuf pKey,
                                                            ByteBuf lKey,
                                                            ByteBuf value,
                                                            PutOption option) {
    pKey.retain();
    lKey.retain();
    value.retain();
    return TableRequestProcessor.of(
        KvUtils.newKvPutRequest(
            scChannel.getStorageContainerId(),
            KvUtils.newPutRequest(lKey, value, option)
                .setHeader(newRoutingHeader(pKey))),
        response -> KvUtils.newPutResult(response.getKvPutResp(), resultFactory, kvFactory),
        scChannel,
        executor
    ).process().whenComplete((ignored, cause) -> {
      pKey.release();
      lKey.release();
      value.release();
    });
  }

  @Override
  public CompletableFuture<DeleteResult<ByteBuf, ByteBuf>> delete(ByteBuf pKey,
                                                                  ByteBuf lKey,
                                                                  DeleteOption<ByteBuf> option) {
    pKey.retain();
    lKey.retain();
    if (null != option.endKey()) {
      option.endKey().retain();
    }
    return TableRequestProcessor.of(
        KvUtils.newKvDeleteRequest(
            scChannel.getStorageContainerId(),
            KvUtils.newDeleteRequest(lKey, option)
                .setHeader(newRoutingHeader(pKey))),
        response -> KvUtils.newDeleteResult(response.getKvDeleteResp(), resultFactory, kvFactory),
        scChannel,
        executor
    ).process().whenComplete((ignored, cause) -> {
      pKey.release();
      lKey.release();
      if (null != option.endKey()) {
        option.endKey().release();
      }
    });
  }

  @Override
  public CompletableFuture<Void> increment(ByteBuf pKey, ByteBuf lKey, long amount) {
    pKey.retain();
    lKey.retain();
    return TableRequestProcessor.of(
        KvUtils.newKvIncrementRequest(
            scChannel.getStorageContainerId(),
            KvUtils.newIncrementRequest(lKey, amount)
              .setHeader(newRoutingHeader(pKey))),
        response -> KvUtils.newIncrementResult(response.getKvIncrResp(), resultFactory, kvFactory),
        scChannel,
        executor
    )
    .process()
    .thenApply(result -> {
      result.close();
      return (Void) null;
    })
    .whenComplete((ignored, cause) -> {
        pKey.release();
        lKey.release();
    });
  }

  @Override
  public Txn<ByteBuf, ByteBuf> txn(ByteBuf pKey) {
    return new TxnImpl(pKey);
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public OpFactory<ByteBuf, ByteBuf> opFactory() {
    return opFactory;
  }

  //
  // Txn Implementation
  //

  class TxnImpl implements Txn<ByteBuf, ByteBuf> {

    private final ByteBuf pKey;
    private final TxnRequest.Builder txnBuilder;
    private final List<AutoCloseable> resourcesToRelease;

    TxnImpl(ByteBuf pKey) {
      this.pKey = pKey.retain();
      this.txnBuilder = TxnRequest.newBuilder();
      this.resourcesToRelease = Lists.newArrayList();
    }

    @Override
    public Txn<ByteBuf, ByteBuf> If(CompareOp<ByteBuf, ByteBuf>... cmps) {
      for (CompareOp<ByteBuf, ByteBuf> cmp : cmps) {
        txnBuilder.addCompare(toProtoCompare(cmp));
        resourcesToRelease.add(cmp);
      }
      return this;
    }

    @Override
    public Txn<ByteBuf, ByteBuf> Then(Op<ByteBuf, ByteBuf>... ops) {
      for (Op<ByteBuf, ByteBuf> op : ops) {
        txnBuilder.addSuccess(toProtoRequest(op));
        resourcesToRelease.add(op);
      }
      return this;
    }

    @Override
    public Txn<ByteBuf, ByteBuf> Else(Op<ByteBuf, ByteBuf>... ops) {
      for (Op<ByteBuf, ByteBuf> op : ops) {
        txnBuilder.addFailure(toProtoRequest(op));
        resourcesToRelease.add(op);
      }
      return this;
    }

    @Override
    public CompletableFuture<TxnResult<ByteBuf, ByteBuf>> commit() {
      return TableRequestProcessor.of(
          KvUtils.newKvTxnRequest(
              scChannel.getStorageContainerId(),
              txnBuilder.setHeader(newRoutingHeader(pKey))),
          response -> KvUtils.newKvTxnResult(response.getKvTxnResp(), resultFactory, kvFactory),
          scChannel,
          executor
      ).process().whenComplete((ignored, cause) -> {
        pKey.release();
        for (AutoCloseable resource : resourcesToRelease) {
            closeResource(resource);
        }
      });
    }

    private void closeResource(AutoCloseable resource) {
      try {
        resource.close();
      } catch (Exception e) {
        log.warn("Fail to close resource {}", resource, e);
      }
    }
  }
}
