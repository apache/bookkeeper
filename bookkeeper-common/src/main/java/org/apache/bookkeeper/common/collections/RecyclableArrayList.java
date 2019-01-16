// Originally copied from netty project, version 4.1.17-Final, heavily modified
/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.common.collections;

import io.netty.util.Recycler.Handle;
import java.util.ArrayList;

/**
 * A simple list which is recyclable.
 */
public final class RecyclableArrayList<T> extends ArrayList<T> {

    private static final int DEFAULT_INITIAL_CAPACITY = 8;

    /**
     * An ArrayList recycler.
     */
    public static class Recycler<X>
        extends io.netty.util.Recycler<RecyclableArrayList<X>> {
        @Override
        protected RecyclableArrayList<X> newObject(
                Handle<RecyclableArrayList<X>> handle) {
            return new RecyclableArrayList<X>(handle, DEFAULT_INITIAL_CAPACITY);
        }

        public RecyclableArrayList<X> newInstance() {
            return get();
        }
    }

    private final Handle<RecyclableArrayList<T>> handle;

    /**
     * Default non-pooled instance.
     */
    public RecyclableArrayList() {
        super();
        this.handle = null;
    }

    private RecyclableArrayList(Handle<RecyclableArrayList<T>> handle, int initialCapacity) {
        super(initialCapacity);
        this.handle = handle;
    }

    public void recycle() {
        clear();
        if (handle != null) {
            handle.recycle(this);
        }
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

}
