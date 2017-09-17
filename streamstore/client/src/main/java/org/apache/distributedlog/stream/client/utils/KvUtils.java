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

import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import java.util.Optional;
import org.apache.distributedlog.stream.api.view.kv.options.DeleteOption;
import org.apache.distributedlog.stream.api.view.kv.options.GetOption;
import org.apache.distributedlog.stream.api.view.kv.options.PutOption;
import org.apache.distributedlog.stream.api.view.kv.result.DeleteResult;
import org.apache.distributedlog.stream.api.view.kv.result.GetResult;
import org.apache.distributedlog.stream.api.view.kv.result.Header;
import org.apache.distributedlog.stream.api.view.kv.result.PutResult;
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
      .setKey(UnsafeByteOperations.unsafeWrap(key.nioBuffer()))
      .setCountOnly(option.countOnly())
      .setKeysOnly(option.keysOnly())
      .setLimit(option.limit())
      .setRevision(option.revision());
    if (option.endKey().isPresent()) {
      builder = builder.setRangeEnd(UnsafeByteOperations.unsafeWrap(option.endKey().get().nioBuffer()));
    }
    return builder;
  }

  public static GetResult newGetResult(RangeResponse response) {
    return new GetResult(
      newHeader(response.getHeader().getRoutingHeader(), response.getHeader().getRevision()),
      response.getHeader().getRoutingHeader().getRKey(),
      response.getKvsList(),
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
      response.getHeader().getRoutingHeader().getRKey(),
      response.hasPrevKv() ? Optional.of(response.getPrevKv()) : Optional.empty());
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
      response.getHeader().getRoutingHeader().getRKey(),
      response.getPrevKvsList());
  }

}
