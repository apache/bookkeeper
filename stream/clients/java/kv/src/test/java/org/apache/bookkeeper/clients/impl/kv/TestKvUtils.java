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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newDeleteRequest;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newIncrementRequest;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newPutRequest;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.newRangeRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.api.kv.impl.options.OptionFactoryImpl;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.IncrementOption;
import org.apache.bookkeeper.api.kv.options.OptionFactory;
import org.apache.bookkeeper.api.kv.options.Options;
import org.apache.bookkeeper.api.kv.options.PutOption;
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
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
    public void testNewIncrementRequest() {
        try (IncrementOption<ByteBuf> option = Options.incrementAndGet()) {
            IncrementRequest rr = newIncrementRequest(key, 100L, option).build();
            assertEquals(keyBs, rr.getKey());
            assertEquals(100L, rr.getAmount());
            assertTrue(rr.getGetTotal());
            assertFalse(rr.hasHeader());
        }
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

}
