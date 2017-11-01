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

package org.apache.distributedlog.statestore.impl.mvcc.result;

import io.netty.util.Recycler.Handle;
import java.util.Collections;
import java.util.List;
import lombok.Setter;
import org.apache.distributedlog.statestore.api.mvcc.KVRecord;
import org.apache.distributedlog.statestore.api.mvcc.op.OpType;
import org.apache.distributedlog.statestore.api.mvcc.result.RangeResult;

/**
 * A implementation of {@link RangeResult}.
 */
@Setter
public class RangeResultImpl<K, V>
        extends ResultImpl<K, V, RangeResultImpl<K, V>>
        implements RangeResult<K, V> {

    private List<KVRecord<K, V>> kvs = Collections.emptyList();
    private long count = 0L;
    private boolean hasMore = false;

    RangeResultImpl(Handle<RangeResultImpl<K, V>> handle) {
        super(OpType.RANGE, handle);
    }

    @Override
    public List<KVRecord<K, V>> kvs() {
        return kvs;
    }

    @Override
    public List<KVRecord<K, V>> getKvsAndClear() {
        List<KVRecord<K, V>> kvsToReturn = kvs;
        kvs = Collections.emptyList();
        return kvsToReturn;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public boolean hasMore() {
        return hasMore;
    }

    @Override
    protected void reset() {
        count = 0L;
        hasMore = false;
        kvs.forEach(KVRecord::recycle);
        kvs.clear();
        kvs = Collections.emptyList();

        super.reset();
    }

    @Override
    RangeResultImpl<K, V> asResult() {
        return this;
    }

}
