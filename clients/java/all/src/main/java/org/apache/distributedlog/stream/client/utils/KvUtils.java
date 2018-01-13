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

package org.apache.distributedlog.stream.client.utils;

import static org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest.Type.KV_DELETE;
import static org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest.Type.KV_PUT;
import static org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest.Type.KV_RANGE;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Optional;
import org.apache.bookkeeper.common.kv.KV;
import org.apache.distributedlog.api.kv.options.DeleteOption;
import org.apache.distributedlog.api.kv.options.GetOption;
import org.apache.distributedlog.api.kv.options.PutOption;
import org.apache.distributedlog.api.kv.result.DeleteResult;
import org.apache.distributedlog.api.kv.result.GetResult;
import org.apache.distributedlog.api.kv.result.Header;
import org.apache.distributedlog.api.kv.result.PutResult;
import org.apache.distributedlog.stream.client.impl.view.kv.KVImpl;
import org.apache.distributedlog.stream.proto.kv.KeyValue;
import org.apache.distributedlog.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.distributedlog.stream.proto.kv.rpc.PutRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.PutResponse;
import org.apache.distributedlog.stream.proto.kv.rpc.RangeRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.RangeResponse;
import org.apache.distributedlog.stream.proto.kv.rpc.RoutingHeader;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest;

/**
 * K/V related utils.
 */
public final class KvUtils {

  private KvUtils() {}

  public static ByteBuf fromProtoKey(ByteString key) {
    return Unpooled.wrappedBuffer(key.asReadOnlyByteBuffer());
  }

  public static ByteString toProtoKey(ByteBuf key) {
    return UnsafeByteOperations.unsafeWrap(key.nioBuffer());
  }

  public static KV<ByteBuf, ByteBuf> fromProtoKeyValue(KeyValue kv) {
    return new KVImpl<>(
        Unpooled.wrappedBuffer(kv.getKey().asReadOnlyByteBuffer()),
        Unpooled.wrappedBuffer(kv.getValue().asReadOnlyByteBuffer()));
  }

  public static List<KV<ByteBuf, ByteBuf>> fromProtoKeyValues(List<KeyValue> kvs) {
    return Lists.transform(kvs, kv -> fromProtoKeyValue(kv));
  }

  public static Header newHeader(RoutingHeader header, long revision) {
    return new Header(
      header.getStreamId(),
      header.getRangeId(),
      revision);
  }

  public static StorageContainerRequest newKvRangeRequest(
      long scId,
      RangeRequest.Builder rangeReq) {
    return StorageContainerRequest.newBuilder()
      .setScId(scId)
      .setType(KV_RANGE)
      .setKvRangeReq(rangeReq)
      .build();
  }

  public static RangeRequest.Builder newRangeRequest(ByteBuf key, GetOption option) {
    RangeRequest.Builder builder = RangeRequest.newBuilder()
      .setKey(toProtoKey(key))
      .setCountOnly(option.countOnly())
      .setKeysOnly(option.keysOnly())
      .setLimit(option.limit())
      .setRevision(option.revision());
    if (option.endKey().isPresent()) {
      builder = builder.setRangeEnd(toProtoKey(option.endKey().get()));
    }
    return builder;
  }

  public static GetResult newGetResult(RangeResponse response) {
    return new GetResult(
      newHeader(response.getHeader().getRoutingHeader(), response.getHeader().getRevision()),
      fromProtoKey(response.getHeader().getRoutingHeader().getRKey()),
      fromProtoKeyValues(response.getKvsList()),
      response.getMore(),
      response.getCount());
  }

  public static StorageContainerRequest newKvPutRequest(
      long scId,
      PutRequest.Builder putReq) {
    return StorageContainerRequest.newBuilder()
      .setScId(scId)
      .setType(KV_PUT)
      .setKvPutReq(putReq)
      .build();
  }

  public static PutRequest.Builder newPutRequest(ByteBuf key, ByteBuf value, PutOption option) {
    return PutRequest.newBuilder()
      .setKey(UnsafeByteOperations.unsafeWrap(key.nioBuffer()))
      .setValue(UnsafeByteOperations.unsafeWrap(value.nioBuffer()))
      .setExpectedVersion(option.expectedVersion())
      .setPrevKv(option.prevKv())
      .setLease(option.leaseId());
  }

  public static PutResult newPutResult(PutResponse response) {
    return new PutResult(
      newHeader(response.getHeader().getRoutingHeader(), response.getHeader().getRevision()),
      fromProtoKey(response.getHeader().getRoutingHeader().getRKey()),
      response.hasPrevKv() ? Optional.of(fromProtoKeyValue(response.getPrevKv())) : Optional.empty());
  }

  public static StorageContainerRequest newKvDeleteRequest(
      long scId,
      DeleteRangeRequest.Builder deleteReq) {
    return StorageContainerRequest.newBuilder()
      .setScId(scId)
      .setType(KV_DELETE)
      .setKvDeleteReq(deleteReq)
      .build();
  }

  public static DeleteRangeRequest.Builder newDeleteRequest(ByteBuf key, DeleteOption option) {
    DeleteRangeRequest.Builder builder = DeleteRangeRequest.newBuilder()
      .setKey(UnsafeByteOperations.unsafeWrap(key.nioBuffer()))
      .setPrevKv(option.prevKv());
    if (option.endKey().isPresent()) {
      builder = builder.setRangeEnd(UnsafeByteOperations.unsafeWrap(option.endKey().get().nioBuffer()));
    }
    return builder;
  }

  public static DeleteResult newDeleteResult(DeleteRangeResponse response) {
    return new DeleteResult(
      newHeader(response.getHeader().getRoutingHeader(), response.getHeader().getRevision()),
      response.getDeleted(),
      fromProtoKey(response.getHeader().getRoutingHeader().getRKey()),
      fromProtoKeyValues(response.getPrevKvsList()));
  }

}
