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

import static org.apache.distributedlog.stream.proto.storage.StorageContainerRequest.Type.KV_DELETE;
import static org.apache.distributedlog.stream.proto.storage.StorageContainerRequest.Type.KV_PUT;
import static org.apache.distributedlog.stream.proto.storage.StorageContainerRequest.Type.KV_RANGE;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import org.apache.distributedlog.api.kv.options.DeleteOption;
import org.apache.distributedlog.api.kv.options.PutOption;
import org.apache.distributedlog.api.kv.options.RangeOption;
import org.apache.distributedlog.api.kv.result.DeleteResult;
import org.apache.distributedlog.api.kv.result.PutResult;
import org.apache.distributedlog.api.kv.result.RangeResult;
import org.apache.distributedlog.clients.impl.kv.result.PutResultImpl;
import org.apache.distributedlog.clients.impl.kv.result.ResultFactory;
import org.apache.distributedlog.stream.proto.kv.KeyValue;
import org.apache.distributedlog.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.distributedlog.stream.proto.kv.rpc.PutRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.PutResponse;
import org.apache.distributedlog.stream.proto.kv.rpc.RangeRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.RangeResponse;
import org.apache.distributedlog.stream.proto.storage.StorageContainerRequest;

/**
 * K/V related utils.
 */
public final class KvUtils {

  private KvUtils() {}

  public static ByteString toProtoKey(ByteBuf key) {
    return UnsafeByteOperations.unsafeWrap(key.nioBuffer());
  }

  public static org.apache.distributedlog.api.kv.result.KeyValue<ByteBuf, ByteBuf> fromProtoKeyValue(
      KeyValue kv,
      KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
    return kvFactory.newKv()
        .key(Unpooled.wrappedBuffer(kv.getKey().asReadOnlyByteBuffer()))
        .value(Unpooled.wrappedBuffer(kv.getValue().asReadOnlyByteBuffer()))
        .createRevision(kv.getCreateRevision())
        .modifiedRevision(kv.getModRevision())
        .version(kv.getVersion());
  }

  public static List<org.apache.distributedlog.api.kv.result.KeyValue<ByteBuf, ByteBuf>> fromProtoKeyValues(
      List<KeyValue> kvs, KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
    return Lists.transform(kvs, kv -> fromProtoKeyValue(kv, kvFactory));
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

  public static RangeRequest.Builder newRangeRequest(ByteBuf key, RangeOption<ByteBuf> option) {
    RangeRequest.Builder builder = RangeRequest.newBuilder()
      .setKey(toProtoKey(key))
      .setCountOnly(option.countOnly())
      .setKeysOnly(option.keysOnly())
      .setLimit(option.limit())
      .setRevision(option.revision());
    if (null != option.endKey()) {
      builder = builder.setRangeEnd(toProtoKey(option.endKey()));
    }
    return builder;
  }

  public static RangeResult<ByteBuf, ByteBuf> newRangeResult(
      RangeResponse response,
      ResultFactory<ByteBuf, ByteBuf> resultFactory,
      KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
    return resultFactory.newRangeResult()
        .count(response.getCount())
        .more(response.getMore())
        .kvs(fromProtoKeyValues(response.getKvsList(), kvFactory));
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

  public static PutRequest.Builder newPutRequest(ByteBuf key,
                                                 ByteBuf value,
                                                 PutOption<ByteBuf> option) {
    return PutRequest.newBuilder()
      .setKey(UnsafeByteOperations.unsafeWrap(key.nioBuffer()))
      .setValue(UnsafeByteOperations.unsafeWrap(value.nioBuffer()))
      .setPrevKv(option.prevKv());
  }

  public static PutResult<ByteBuf, ByteBuf> newPutResult(
          PutResponse response,
          ResultFactory<ByteBuf, ByteBuf> resultFactory,
          KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
      PutResultImpl<ByteBuf, ByteBuf> result = resultFactory.newPutResult();
      if (response.hasPrevKv()) {
          result.prevKv(fromProtoKeyValue(response.getPrevKv(), kvFactory));
      }
      return result;
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

  public static DeleteRangeRequest.Builder newDeleteRequest(ByteBuf key, DeleteOption<ByteBuf> option) {
    DeleteRangeRequest.Builder builder = DeleteRangeRequest.newBuilder()
      .setKey(UnsafeByteOperations.unsafeWrap(key.nioBuffer()))
      .setPrevKv(option.prevKv());
    if (null != option.endKey()) {
      builder = builder.setRangeEnd(UnsafeByteOperations.unsafeWrap(option.endKey().nioBuffer()));
    }
    return builder;
  }

  public static DeleteResult<ByteBuf, ByteBuf> newDeleteResult(
      DeleteRangeResponse response,
      ResultFactory<ByteBuf, ByteBuf> resultFactory,
      KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
    return resultFactory.newDeleteResult()
        .numDeleted(response.getDeleted())
        .prevKvs(fromProtoKeyValues(response.getPrevKvsList(), kvFactory));
  }

}
