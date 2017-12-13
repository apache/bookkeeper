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

package org.apache.bookkeeper.common.collections;

import io.netty.util.Recycler.Handle;
import java.util.HashSet;
import org.apache.bookkeeper.common.util.Recyclable;

/**
 * A hash set which is recyclable.
 */
public final class RecyclableHashSet<T> extends HashSet<T> implements Recyclable {

    /**
     * An HashSet Recycler.
     */
    public static class Recycler<X>
        extends io.netty.util.Recycler<RecyclableHashSet<X>> {

        @Override
        protected RecyclableHashSet<X> newObject(Handle<RecyclableHashSet<X>> handle) {
            return new RecyclableHashSet<>(handle);
        }

        public RecyclableHashSet<X> newInstance() {
            return get();
        }
    }

    private final Handle<RecyclableHashSet<T>> handle;

    private RecyclableHashSet(Handle<RecyclableHashSet<T>> handle) {
        super();
        this.handle = handle;
    }

    @Override
    public void recycle() {
        clear();
        handle.recycle(this);
    }

}
