/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.api.kv.impl.result;

import static io.netty.util.ReferenceCountUtil.release;
import static io.netty.util.ReferenceCountUtil.retain;

import io.netty.util.Recycler.Handle;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.api.kv.result.KeyValue;

/**
 * The implementation of {@link KeyValue}.
 */
@Accessors(fluent = true, chain = true)
@Setter
@Getter
@ToString(exclude = "handle")
@EqualsAndHashCode(exclude = "handle")
public class KeyValueImpl<K, V> implements KeyValue<K, V> {

    private final Handle<KeyValueImpl<K, V>> handle;

    private K key = null;
    private V value = null;
    private long createRevision = -1L;
    private long modifiedRevision = -1L;
    private long version = -1L;
    private boolean isNumber = false;
    private long numberValue = -1L;

    KeyValueImpl(Handle<KeyValueImpl<K, V>> handle) {
        this.handle = handle;
    }

    public KeyValueImpl<K, V> key(K key) {
        release(this.key);
        this.key = retain(key);
        return this;
    }

    public KeyValueImpl<K, V> value(V value) {
        release(this.value);
        this.value = retain(value);
        return this;
    }

    private void reset() {
        release(key);
        key = null;
        release(value);
        value = null;
        createRevision = -1L;
        modifiedRevision = -1L;
        version = -1L;
        isNumber = false;
        numberValue = -1L;
    }

    @Override
    public void close() {
        reset();

        handle.recycle(this);
    }
}
