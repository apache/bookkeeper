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

import io.netty.util.Recycler.Handle;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.api.kv.op.OpType;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.api.kv.result.RangeResult;

/**
 * A implementation of {@link RangeResult}.
 */
@Accessors(fluent = true, chain = true)
@Getter
@Setter
@ToString
public class RangeResultImpl<K, V>
    extends ResultImpl<K, V, RangeResultImpl<K, V>>
    implements RangeResult<K, V> {

    private List<KeyValue<K, V>> kvs = Collections.emptyList();
    private long count = 0L;
    private boolean more = false;

    RangeResultImpl(Handle<RangeResultImpl<K, V>> handle) {
        super(OpType.RANGE, handle);
    }

    public RangeResultImpl<K, V> kvs(List<KeyValue<K, V>> kvs) {
        this.kvs.forEach(KeyValue::close);
        this.kvs.clear();
        this.kvs = kvs;
        return this;
    }

    @Override
    public List<KeyValue<K, V>> getKvsAndClear() {
        List<KeyValue<K, V>> kvsToReturn = kvs;
        kvs = Collections.emptyList();
        return kvsToReturn;
    }

    @Override
    protected void reset() {
        count = 0L;
        more = false;
        kvs.forEach(KeyValue::close);
        kvs = Collections.emptyList();

        super.reset();
    }

    @Override
    RangeResultImpl<K, V> asResult() {
        return this;
    }

}
