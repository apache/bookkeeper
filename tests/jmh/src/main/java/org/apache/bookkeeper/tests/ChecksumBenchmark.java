/*
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
 */
package org.apache.bookkeeper.tests;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.common.checksum.Checksum;
import org.apache.bookkeeper.common.checksum.ChecksumFactory;
import org.apache.bookkeeper.common.checksum.Checksums;
import org.apache.bookkeeper.common.util.Java;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmarking digest managers.
 */
@BenchmarkMode({ Mode.AverageTime })
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ChecksumBenchmark {

    @State(Scope.Thread)
    public static class BenchStateProvider {

        private static final Random random = new Random(1736243454);

        @Param({ "8", "16", "32", "128", "1024", "65536", "1048576" })
        int bytesSize;

        public byte[] randomBytes() {
            byte[] bytes = new byte[bytesSize];
            random.nextBytes(bytes);
            return bytes;
        }

    }

    @Data
    @RequiredArgsConstructor
    public static class ByteBufBenchState {
        private final ByteBuffer buffer;
        private final Checksum checksum;

        long checksum() {
            checksum.reset();
            checksum.update(buffer);
            long val = checksum.getValue();
            buffer.clear();
            return val;
        }
    }

    protected static abstract class ByteBufBenchStateProvider extends BenchStateProvider {

        private final ChecksumFactory factory;
        ByteBufBenchState state;

        ByteBufBenchStateProvider(ChecksumFactory factory) {
            this.factory = factory;
        }

        protected abstract ByteBuffer randomByteBuffer();

        public void setup() {
            state = new ByteBufBenchState(
                randomByteBuffer(),
                factory.create());
        }

    }

    protected static class DirectByteBufBenchStateProvider extends ByteBufBenchStateProvider {

        DirectByteBufBenchStateProvider(ChecksumFactory factory) {
            super(factory);
        }

        @Override
        protected ByteBuffer randomByteBuffer() {
            byte[] data = randomBytes();
            ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);
            buffer.put(data);
            buffer.flip();
            return buffer;
        }
    }

    protected static class HeapByteBufBenchStateProvider extends ByteBufBenchStateProvider {

        HeapByteBufBenchStateProvider(ChecksumFactory factory) {
            super(factory);
        }

        @Override
        protected ByteBuffer randomByteBuffer() {
            return ByteBuffer.wrap(randomBytes());
        }
    }

    public static class DirectCRC32Provider extends DirectByteBufBenchStateProvider {

        public DirectCRC32Provider() {
            super(Checksums.crc32());
        }

        @Setup
        public void setup() {
            super.setup();
        }

    }

    public static class DirectPureJavaCRC32CProvider extends DirectByteBufBenchStateProvider {

        public DirectPureJavaCRC32CProvider() {
            super(Checksums.pureJavaCrc32c());
        }

        @Setup
        public void setup() {
            super.setup();
        }

    }

    public static class DirectJava9CRC32CProvider extends DirectByteBufBenchStateProvider {

        public DirectJava9CRC32CProvider() {
            super(Checksums.java9Crc32c());
        }

        @Setup
        public void setup() {
            super.setup();
        }

    }

    public static class HeapCRC32Provider extends HeapByteBufBenchStateProvider {

        public HeapCRC32Provider() {
            super(Checksums.crc32());
        }

        @Setup
        public void setup() {
            super.setup();
        }

    }

    public static class HeapPureJavaCRC32CProvider extends HeapByteBufBenchStateProvider {

        public HeapPureJavaCRC32CProvider() {
            super(Checksums.pureJavaCrc32c());
        }

        @Setup
        public void setup() {
            super.setup();
        }

    }

    public static class HeapJava9CRC32CProvider extends HeapByteBufBenchStateProvider {

        public HeapJava9CRC32CProvider() {
            super(Checksums.java9Crc32c());
        }

        @Setup
        public void setup() {
            super.setup();
        }

    }

    @Benchmark
    public long directCrc32(DirectCRC32Provider provider) {
        ByteBufBenchState state = provider.state;
        return state.checksum();
    }

    @Benchmark
    public long directPureJavaCrc32c(DirectPureJavaCRC32CProvider provider) {
        ByteBufBenchState state = provider.state;
        return state.checksum();
    }

    @Benchmark
    public long directJava9Crc32c(DirectJava9CRC32CProvider provider) {
        if (Java.IS_JAVA9_COMPATIBLE) {
            ByteBufBenchState state = provider.state;
            return state.checksum();
        }
        return 0L;
    }

    @Benchmark
    public long heapCrc32(HeapCRC32Provider provider) {
        ByteBufBenchState state = provider.state;
        return state.checksum();
    }

    @Benchmark
    public long heapPureJavaCrc32c(HeapPureJavaCRC32CProvider provider) {
        ByteBufBenchState state = provider.state;
        return state.checksum();
    }

    @Benchmark
    public long heapJava9Crc32c(HeapJava9CRC32CProvider provider) {
        if (Java.IS_JAVA9_COMPATIBLE) {
            ByteBufBenchState state = provider.state;
            return state.checksum();
        }
        return 0L;
    }

}
