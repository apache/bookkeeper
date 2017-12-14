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
 * Wrapper for a long serialized into a byte array.
 */
class LongWrapper {

    final byte[] array = new byte[8];

    public void set(long value) {
        ArrayUtil.setLong(array, 0, value);
    }

    public long getValue() {
        return ArrayUtil.getLong(array, 0);
    }

    public static LongWrapper get() {
        return RECYCLER.get();
    }

    public static LongWrapper get(long value) {
        LongWrapper lp = RECYCLER.get();
        ArrayUtil.setLong(lp.array, 0, value);
        return lp;
    }

    public void recycle() {
        handle.recycle(this);
    }

    private static final Recycler<LongWrapper> RECYCLER = new Recycler<LongWrapper>() {
        @Override
        protected LongWrapper newObject(Handle<LongWrapper> handle) {
            return new LongWrapper(handle);
        }
    };

    private final Handle<LongWrapper> handle;

    private LongWrapper(Handle<LongWrapper> handle) {
        this.handle = handle;
    }
}