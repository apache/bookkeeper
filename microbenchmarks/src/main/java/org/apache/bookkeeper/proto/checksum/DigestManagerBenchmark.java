/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.proto.checksum;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.util.ByteBufList;
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
public class DigestManagerBenchmark {

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

        @Param({"64", "1024", "4086", "8192"})
        public int entrySize;

        private DigestManager dm;

        public ByteBuf digestBuf;

        @Setup(Level.Trial)
        public void doSetup() throws Exception {
            final byte[] password = "password".getBytes(StandardCharsets.UTF_8);

            dm = DigestManager.instantiate(ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE),
                    password, DigestType.CRC32C, PooledByteBufAllocator.DEFAULT, true);

            ByteBuf data = ByteBufAllocator.DEFAULT.directBuffer(entrySize, entrySize);
            data.writeBytes(randomBytes(entrySize));

            digestBuf = ByteBufAllocator.DEFAULT.directBuffer();
            digestBuf.writeBytes((ByteBuf)
                    dm.computeDigestAndPackageForSending(1234, 1234, entrySize, data,
                            new byte[0], 0));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(2)
    @Fork(1)
    public void verifyDigest(MyState state) throws Exception {
        state.digestBuf.readerIndex(0);
        state.dm.verifyDigestAndReturnData(1234, state.digestBuf);
    }
}
