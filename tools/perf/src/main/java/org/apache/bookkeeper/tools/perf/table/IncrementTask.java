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

package org.apache.bookkeeper.tools.perf.table;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.tools.perf.table.PerfClient.Flags;
import org.apache.bookkeeper.tools.perf.table.PerfClient.OP;
import org.apache.bookkeeper.tools.perf.table.PerfClient.OpStats;

/**
 * Write task to inject key/value pairs to the table.
 */
@Slf4j
abstract class IncrementTask extends BenchmarkTask {

    protected final RateLimiter limiter;
    protected final Semaphore semaphore;
    protected final OpStats writeOpStats;

    IncrementTask(Table<ByteBuf, ByteBuf> table,
                  int tid,
                  long randSeed,
                  long numRecords,
                  long keyRange,
                  Flags flags,
                  KeyGenerator generator,
                  RateLimiter limiter,
                  Semaphore semaphore) {
        super(table, tid, randSeed, numRecords, keyRange, flags, generator);
        this.limiter = limiter;
        this.semaphore = semaphore;
        this.writeOpStats = new OpStats(OP.INC.name());
    }

    @Override
    protected void runTask() throws Exception {
        for (long i = 0L; i < numRecords; ++i) {
            if (null != semaphore) {
                semaphore.acquire();
            }
            if (null != limiter) {
                limiter.acquire();
            }
            incKey(i);
        }
    }

    protected abstract void getKey(ByteBuf key, long id, long range);

    void incKey(long i) {
        ByteBuf keyBuf = PooledByteBufAllocator.DEFAULT.heapBuffer(flags.keySize);
        getKey(keyBuf, i, keyRange);
        keyBuf.writerIndex(keyBuf.readerIndex() + keyBuf.writableBytes());

        final long startTime = System.nanoTime();
        table.increment(keyBuf, 100)
            .whenComplete((result, cause) -> {
                if (null != semaphore) {
                    semaphore.release();
                }
                if (null != cause) {
                    log.error("Error at increment key/amount", cause);
                } else {
                    long latencyMicros = TimeUnit.NANOSECONDS.toMicros(
                        System.nanoTime() - startTime
                    );
                    writeOpStats.recordOp(latencyMicros);
                }
                keyBuf.release();
            });
    }

    @Override
    protected void reportStats(long oldTime) {
        writeOpStats.reportStats(oldTime);
    }

    @Override
    protected void printAggregatedStats() {
        writeOpStats.printAggregatedStats();
    }
}
