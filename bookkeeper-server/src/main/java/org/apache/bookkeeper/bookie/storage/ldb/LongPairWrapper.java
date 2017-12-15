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
package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

/**
 * Recyclable wrapper that holds a pair of longs.
 */
class LongPairWrapper {

    final byte[] array = new byte[16];

    public void set(long first, long second) {
        ArrayUtil.setLong(array, 0, first);
        ArrayUtil.setLong(array, 8, second);
    }

    public long getFirst() {
        return ArrayUtil.getLong(array, 0);
    }

    public long getSecond() {
        return ArrayUtil.getLong(array, 8);
    }

    public static LongPairWrapper get(long first, long second) {
        LongPairWrapper lp = RECYCLER.get();
        ArrayUtil.setLong(lp.array, 0, first);
        ArrayUtil.setLong(lp.array, 8, second);
        return lp;
    }

    public void recycle() {
        handle.recycle(this);
    }

    private static final Recycler<LongPairWrapper> RECYCLER = new Recycler<LongPairWrapper>() {
        @Override
        protected LongPairWrapper newObject(Handle<LongPairWrapper> handle) {
            return new LongPairWrapper(handle);
        }
    };

    private final Handle<LongPairWrapper> handle;

    private LongPairWrapper(Handle<LongPairWrapper> handle) {
        this.handle = handle;
    }
}