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
import java.util.concurrent.Semaphore;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.tools.perf.table.PerfClient.Flags;

/**
 * Write key/values in sequence.
 */
class WriteSequentialTask extends WriteTask {

    WriteSequentialTask(Table<ByteBuf, ByteBuf> table,
                        int tid,
                        long randSeed,
                        long numRecords,
                        long keyRange,
                        Flags flags,
                        KeyGenerator generator,
                        RateLimiter limiter,
                        Semaphore semaphore) {
        super(table, tid, randSeed, numRecords, keyRange, flags, generator, limiter, semaphore);
    }

    @Override
    protected void getKey(ByteBuf key, long id, long range) {
        getFixedKey(key, id);
    }

}
