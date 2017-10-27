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

package org.apache.distributedlog.statestore.impl.mvcc;

import io.netty.util.Recycler.Handle;
import lombok.Getter;
import lombok.Setter;
import org.apache.distributedlog.statestore.api.mvcc.KVRecord;

/**
 * The implementation of {@link KVRecord}.
 */
@Setter
@Getter
class KVRecordImpl<K, V> implements KVRecord<K, V> {

    private final Handle<KVRecordImpl<K, V>> handle;

    private K key = null;
    private V value = null;
    private long createRevision = -1L;
    private long modRevision = -1L;
    private long version = -1L;

    KVRecordImpl(Handle<KVRecordImpl<K, V>> handle) {
        this.handle = handle;
    }

    @Override
    public K key() {
        return key;
    }

    @Override
    public V value() {
        return value;
    }

    @Override
    public long createRevision() {
        return createRevision;
    }

    @Override
    public long modifiedRevision() {
        return modRevision;
    }

    @Override
    public long version() {
        return version;
    }

    private void reset() {
        key = null;
        value = null;
        createRevision = -1L;
        modRevision = -1L;
        version = -1L;
    }

    @Override
    public void recycle() {
        reset();

        handle.recycle(this);
    }
}
