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

package org.apache.bookkeeper.common.router;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.hash.Murmur3;
import org.junit.Test;

/**
 * Unit test {@link HashRouter}s.
 */
@Slf4j
public class HashRouterTest {

    @Test
    public void testByteBufHashRouter() {
        byte[] keyBytes = "foo".getBytes(UTF_8);
        ByteBuf key = Unpooled.wrappedBuffer(keyBytes);

        // murmur3 - 32 bits
        int hash32 = Murmur3.hash32(
            key, key.readerIndex(), key.readableBytes(), (int) AbstractHashRouter.HASH_SEED);
        int bytesHash32 = Murmur3.hash32(keyBytes, 0, keyBytes.length, (int) AbstractHashRouter.HASH_SEED);
        int guavaHash32 = Hashing.murmur3_32((int) AbstractHashRouter.HASH_SEED)
            .newHasher()
            .putString("foo", UTF_8)
            .hash()
            .asInt();
        assertEquals(hash32, bytesHash32);
        assertEquals(hash32, guavaHash32);

        // murmur3 - 128 bits
        long[] hash128 = Murmur3.hash128(
            key, key.readerIndex(), key.readableBytes(), AbstractHashRouter.HASH_SEED);
        long[] bytesHash128 = Murmur3.hash128(keyBytes, 0, keyBytes.length, AbstractHashRouter.HASH_SEED);
        log.info("hash128: {}, bytes hash128: {}", hash128, bytesHash128);
        long guavaHash128 = Hashing.murmur3_128((int) AbstractHashRouter.HASH_SEED)
            .newHasher()
            .putString("foo", UTF_8)
            .hash()
            .asLong();
        assertArrayEquals(hash128, bytesHash128);
        assertEquals(hash128[0], guavaHash128);

        ByteBufHashRouter router = ByteBufHashRouter.of();
        long routingHash = router.getRoutingKey(key).longValue();
        log.info("Routing hash = {}", routingHash);
        assertEquals(hash128[0], routingHash);
        BytesHashRouter bytesRouter = BytesHashRouter.of();
        long bytesRoutingHash = bytesRouter.getRoutingKey(keyBytes).longValue();
        assertEquals(hash128[0], bytesRoutingHash);
    }

}
