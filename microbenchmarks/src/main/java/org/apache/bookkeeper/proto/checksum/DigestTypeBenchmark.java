/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.proto.checksum;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Microbenchmarks for different digest type
 * getting started:
 * 1. http://tutorials.jenkov.com/java-performance/jmh.html
 * 2. http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/
 * 3. google
 * To run:
 * build project from command line.
 * execute ./run.sh
 */
public class DigestTypeBenchmark {

    /**
     * BufferType.
     */
    public enum BufferType {
        ARRAY_BACKED,
        NOT_ARRAY_BACKED,
        BYTE_BUF_DEFAULT_ALLOC
    }

    /**
     * Digest.
     */
    public enum Digest {
        MAC,
        CRC32,
        CRC32_C,
    }

    static byte[] randomBytes(int sz) {
        byte[] b = new byte[sz];
        ThreadLocalRandom.current().nextBytes(b);
        return b;
    }

    /**
     * MyState.
     */
    @State(Scope.Thread)
    public static class MyState {

        @Param
        public BufferType bufferType;
        @Param
        public Digest digest;
        @Param({"1024", "4086", "8192", "16384", "65536"})
        public int entrySize;

        private DigestManager crc32;
        private DigestManager crc32c;
        private DigestManager mac;

        private ByteBuf arrayBackedBuffer;
        private CompositeByteBuf notArrayBackedBuffer;
        private ByteBuf byteBufDefaultAlloc;

        public ByteBuf digestBuf;

        @Setup(Level.Trial)
        public void doSetup() throws Exception {
            final byte[] password = "password".getBytes(StandardCharsets.UTF_8);
            crc32 = DigestManager.instantiate(ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE),
                    password, DigestType.CRC32, PooledByteBufAllocator.DEFAULT, true);

            crc32c = DigestManager.instantiate(ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE),
                    password, DigestType.CRC32C, PooledByteBufAllocator.DEFAULT, true);

            mac = DigestManager.instantiate(ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE),
                    password, DigestType.HMAC, PooledByteBufAllocator.DEFAULT, true);

            digestBuf = Unpooled.buffer(getDigestManager(digest).getMacCodeLength());

            arrayBackedBuffer = Unpooled.wrappedBuffer(randomBytes(entrySize));

            final int headerSize = 32 + getDigestManager(digest).getMacCodeLength();
            notArrayBackedBuffer = new CompositeByteBuf(ByteBufAllocator.DEFAULT, true, 2);
            notArrayBackedBuffer.addComponent(Unpooled.wrappedBuffer(randomBytes(headerSize)));
            notArrayBackedBuffer.addComponent(Unpooled.wrappedBuffer((randomBytes(entrySize - headerSize))));

            byteBufDefaultAlloc = ByteBufAllocator.DEFAULT.buffer(entrySize, entrySize);
            byteBufDefaultAlloc.writeBytes(randomBytes(entrySize));

            if (!arrayBackedBuffer.hasArray() || notArrayBackedBuffer.hasArray()) {
                throw new IllegalStateException("buffers in invalid state");
            }
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
        }

        public ByteBuf getByteBuff(BufferType bType) {
            switch (bType) {
            case ARRAY_BACKED:
                return arrayBackedBuffer;
            case NOT_ARRAY_BACKED:
                return notArrayBackedBuffer;
            case BYTE_BUF_DEFAULT_ALLOC:
                return byteBufDefaultAlloc;
            default:
                throw new IllegalArgumentException("unknown buffer type " + bType);
            }
        }

        public DigestManager getDigestManager(Digest digest) {
            switch (digest) {
            case CRC32:
                return crc32;
            case CRC32_C:
                return crc32c;
            case MAC:
                return mac;
            default:
                throw new IllegalArgumentException("unknown digest " + digest);
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 12, timeUnit = TimeUnit.SECONDS)
    @Threads(2)
    @Fork(value = 1, warmups = 1)
    public void digestManager(MyState state) {
        final ByteBuf buff = state.getByteBuff(state.bufferType);
        final DigestManager dm = state.getDigestManager(state.digest);
        dm.update(buff);
        state.digestBuf.clear();
        dm.populateValueAndReset(state.digestBuf);
    }

}
