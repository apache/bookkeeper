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

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newDeleteRequest;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newHeader;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newKvDeleteRequest;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newKvPutRequest;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newKvRangeRequest;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newPutRequest;
import static org.apache.distributedlog.stream.client.utils.KvUtils.newRangeRequest;
import static org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest.Type.KV_DELETE;
import static org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest.Type.KV_PUT;
import static org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest.Type.KV_RANGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.distributedlog.stream.api.view.kv.options.DeleteOption;
import org.apache.distributedlog.stream.api.view.kv.options.GetOption;
import org.apache.distributedlog.stream.api.view.kv.options.PutOption;
import org.apache.distributedlog.stream.api.view.kv.result.Header;
import org.apache.distributedlog.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.PutRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.RangeRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.RoutingHeader;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest;
import org.junit.Test;

/**
 * Unit test for {@link KvUtils}.
 */
public class TestKvUtils {

  private static final long scId = System.currentTimeMillis();
  private static final ByteString routingKey = ByteString.copyFrom("test-routing-key", UTF_8);
  private static final ByteBuf key = Unpooled.wrappedBuffer("test-key".getBytes(UTF_8));
  private static final ByteBuf value = Unpooled.wrappedBuffer("test-value".getBytes(UTF_8));
  private static final ByteString keyBs = ByteString.copyFrom("test-key".getBytes(UTF_8));
  private static final ByteString valueBs = ByteString.copyFrom("test-value".getBytes(UTF_8));

  @Test
  public void testNewHeader() {
    RoutingHeader rh = RoutingHeader.newBuilder()
      .setStreamId(1234L)
      .setRangeId(2345L)
      .setRKey(routingKey)
      .build();
    Header header = newHeader(rh, 5678L);
    assertEquals(1234L, header.getStreamId());
    assertEquals(2345L, header.getRangeId());
    assertEquals(5678L, header.getRevision());
  }

  @Test
  public void testNewRangeRequest() {
    GetOption getOption = GetOption.newBuilder()
      .endKey(key.duplicate())
      .countOnly(true)
      .keysOnly(true)
      .limit(10)
      .revision(1234L)
      .build();
    RangeRequest rr = newRangeRequest(key, getOption).build();
    assertEquals(keyBs, rr.getKey());
    assertEquals(keyBs, rr.getRangeEnd());
    assertTrue(rr.getCountOnly());
    assertTrue(rr.getKeysOnly());
    assertEquals(10, rr.getLimit());
    assertEquals(1234L, rr.getRevision());
    assertFalse(rr.hasHeader());
  }

  @Test
  public void testNewKvRangeRequest() {
    GetOption getOption = GetOption.newBuilder()
      .endKey(key.duplicate())
      .countOnly(true)
      .keysOnly(true)
      .limit(10)
      .revision(1234L)
      .build();
    RangeRequest.Builder rrBuilder = newRangeRequest(key, getOption);
    StorageContainerRequest request = newKvRangeRequest(scId, rrBuilder);
    assertEquals(scId, request.getScId());
    assertEquals(KV_RANGE, request.getType());
    assertEquals(rrBuilder.build(), request.getKvRangeReq());
  }

  @Test
  public void testNewPutRequest() {
    PutOption getOption = PutOption.newBuilder()
      .expectedVersion(1234L)
      .leaseId(5678L)
      .prevKv(true)
      .build();
    PutRequest rr = newPutRequest(key, value, getOption).build();
    assertEquals(keyBs, rr.getKey());
    assertEquals(valueBs, rr.getValue());
    assertEquals(1234L, rr.getExpectedVersion());
    assertEquals(5678L, rr.getLease());
    assertTrue(rr.getPrevKv());
    assertFalse(rr.hasHeader());
  }

  @Test
  public void testNewKvPutRequest() {
    PutOption getOption = PutOption.newBuilder()
      .expectedVersion(1234L)
      .leaseId(5678L)
      .prevKv(true)
      .build();
    PutRequest.Builder putBuilder = newPutRequest(key, value, getOption);
    StorageContainerRequest request = newKvPutRequest(scId, putBuilder);
    assertEquals(scId, request.getScId());
    assertEquals(KV_PUT, request.getType());
    assertEquals(putBuilder.build(), request.getKvPutReq());
  }

  @Test
  public void testNewDeleteRequest() {
    DeleteOption getOption = DeleteOption.newBuilder()
      .endKey(key.duplicate())
      .prevKv(true)
      .build();
    DeleteRangeRequest rr = newDeleteRequest(key, getOption).build();
    assertEquals(keyBs, rr.getKey());
    assertEquals(keyBs, rr.getRangeEnd());
    assertTrue(rr.getPrevKv());
    assertFalse(rr.hasHeader());
  }

  @Test
  public void testNewKvDeleteRequest() {
    DeleteOption getOption = DeleteOption.newBuilder()
      .endKey(key.duplicate())
      .prevKv(true)
      .build();
    DeleteRangeRequest.Builder delBuilder = newDeleteRequest(key, getOption);
    StorageContainerRequest request = newKvDeleteRequest(scId, delBuilder);
    assertEquals(scId, request.getScId());
    assertEquals(KV_DELETE, request.getType());
    assertEquals(delBuilder.build(), request.getKvDeleteReq());
  }

}
