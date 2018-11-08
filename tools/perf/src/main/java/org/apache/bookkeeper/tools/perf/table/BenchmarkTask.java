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

import io.netty.buffer.ByteBuf;
import java.util.Random;
import java.util.concurrent.Callable;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.tools.perf.table.PerfClient.Flags;

/**
 * Abstract benchmark task.
 */
abstract class BenchmarkTask implements Callable<Void> {

    protected final Table<ByteBuf, ByteBuf> table;
    protected final int tid;
    protected final Random random;
    protected final long numRecords;
    protected final long keyRange;
    protected final Flags flags;
    protected final KeyGenerator generator;

    BenchmarkTask(Table<ByteBuf, ByteBuf> table,
                  int tid,
                  long randSeed,
                  long numRecords,
                  long keyRange,
                  Flags flags,
                  KeyGenerator generator) {
        this.table = table;
        this.tid = tid;
        this.random = new Random(randSeed + tid * 1000);
        this.numRecords = numRecords;
        this.keyRange = keyRange;
        this.flags = flags;
        this.generator = generator;
    }

    @Override
    public Void call() throws Exception {
        runTask();
        return null;
    }

    protected abstract void runTask() throws Exception;

    protected void getFixedKey(ByteBuf key, long sn) {
        generator.generateKeyFromLong(key, sn);
    }

    protected void getRandomKey(ByteBuf key, long range) {
        generator.generateKeyFromLong(key, Math.abs(random.nextLong() % range));
    }

    protected abstract void reportStats(long oldTime);

    protected abstract void printAggregatedStats();

}
