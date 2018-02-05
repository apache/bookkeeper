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

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newDeleteRequest;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newIncrementRequest;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newKvDeleteRequest;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newKvIncrementRequest;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newKvPutRequest;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newKvRangeRequest;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newPutRequest;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newRangeRequest;
import static org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest.RequestCase.KV_DELETE_REQ;
import static org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest.RequestCase.KV_INCR_REQ;
import static org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest.RequestCase.KV_PUT_REQ;
import static org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest.RequestCase.KV_RANGE_REQ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.api.kv.impl.options.OptionFactoryImpl;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.OptionFactory;
import org.apache.bookkeeper.api.kv.options.Options;
import org.apache.bookkeeper.api.kv.options.PutOption;
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest;
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

    private final OptionFactory<ByteBuf> optionFactory = new OptionFactoryImpl<>();

    @Test
    public void testNewRangeRequest() {
        try (RangeOption<ByteBuf> rangeOption = optionFactory.newRangeOption()
            .endKey(key.retainedDuplicate())
            .countOnly(true)
            .keysOnly(true)
            .limit(10)
            .maxCreateRev(1234L)
            .minCreateRev(234L)
            .maxModRev(2345L)
            .minModRev(1235L)
            .build()) {
            RangeRequest rr = newRangeRequest(key, rangeOption).build();
            assertEquals(keyBs, rr.getKey());
            assertEquals(keyBs, rr.getRangeEnd());
            assertTrue(rr.getCountOnly());
            assertTrue(rr.getKeysOnly());
            assertEquals(10, rr.getLimit());
            assertEquals(1234L, rr.getMaxCreateRevision());
            assertEquals(234L, rr.getMinCreateRevision());
            assertEquals(2345L, rr.getMaxModRevision());
            assertEquals(1235L, rr.getMinModRevision());
            assertFalse(rr.hasHeader());
        }
    }

    @Test
    public void testNewKvRangeRequest() {
        try (RangeOption<ByteBuf> rangeOption = optionFactory.newRangeOption()
            .endKey(key.retainedDuplicate())
            .countOnly(true)
            .keysOnly(true)
            .limit(10)
            .maxCreateRev(1234L)
            .minCreateRev(234L)
            .maxModRev(2345L)
            .minModRev(1235L)
            .build()) {
            RangeRequest.Builder rrBuilder = newRangeRequest(key, rangeOption);
            StorageContainerRequest request = newKvRangeRequest(scId, rrBuilder);
            assertEquals(scId, request.getScId());
            assertEquals(KV_RANGE_REQ, request.getRequestCase());
            assertEquals(rrBuilder.build(), request.getKvRangeReq());
        }
    }

    @Test
    public void testNewPutRequest() {
        try (PutOption<ByteBuf> option = Options.putAndGet()) {
            PutRequest rr = newPutRequest(key, value, option).build();
            assertEquals(keyBs, rr.getKey());
            assertEquals(valueBs, rr.getValue());
            assertTrue(rr.getPrevKv());
            assertFalse(rr.hasHeader());
        }
    }

    @Test
    public void testNewKvPutRequest() {
        try (PutOption<ByteBuf> option = Options.putAndGet()) {
            PutRequest.Builder putBuilder = newPutRequest(key, value, option);
            StorageContainerRequest request = newKvPutRequest(scId, putBuilder);
            assertEquals(scId, request.getScId());
            assertEquals(KV_PUT_REQ, request.getRequestCase());
            assertEquals(putBuilder.build(), request.getKvPutReq());
        }
    }

    @Test
    public void testNewIncrementRequest() {
        IncrementRequest rr = newIncrementRequest(key, 100L).build();
        assertEquals(keyBs, rr.getKey());
        assertEquals(100L, rr.getAmount());
        assertFalse(rr.hasHeader());
    }

    @Test
    public void testNewKvIncrementRequest() {
        IncrementRequest.Builder incrBuilder = newIncrementRequest(key, 100L);
        StorageContainerRequest request = newKvIncrementRequest(scId, incrBuilder);
        assertEquals(scId, request.getScId());
        assertEquals(KV_INCR_REQ, request.getRequestCase());
        assertEquals(incrBuilder.build(), request.getKvIncrReq());
    }

    @Test
    public void testNewDeleteRequest() {
        try (DeleteOption<ByteBuf> option = optionFactory.newDeleteOption()
            .endKey(key.retainedDuplicate())
            .prevKv(true)
            .build()) {
            DeleteRangeRequest rr = newDeleteRequest(key, option).build();
            assertEquals(keyBs, rr.getKey());
            assertEquals(keyBs, rr.getRangeEnd());
            assertTrue(rr.getPrevKv());
            assertFalse(rr.hasHeader());
        }
    }

    @Test
    public void testNewKvDeleteRequest() {
        try (DeleteOption<ByteBuf> option = optionFactory.newDeleteOption()
            .endKey(key.retainedDuplicate())
            .prevKv(true)
            .build()) {
            DeleteRangeRequest.Builder delBuilder = newDeleteRequest(key, option);
            StorageContainerRequest request = newKvDeleteRequest(scId, delBuilder);
            assertEquals(scId, request.getScId());
            assertEquals(KV_DELETE_REQ, request.getRequestCase());
            assertEquals(delBuilder.build(), request.getKvDeleteReq());
        }
    }

}
