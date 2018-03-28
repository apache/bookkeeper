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
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.api.kv.op.OpType;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.api.kv.result.PutResult;

/**
 * An implementation of {@link PutResult}.
 */
@Accessors(fluent = true, chain = true)
@Getter
@Setter
@ToString
public class PutResultImpl<K, V> extends ResultImpl<K, V, PutResultImpl<K, V>> implements PutResult<K, V> {

    private KeyValue<K, V> prevKv;

    PutResultImpl(Handle<PutResultImpl<K, V>> handle) {
        super(OpType.PUT, handle);
    }

    public PutResultImpl<K, V> prevKv(KeyValue<K, V> kv) {
        if (null != prevKv) {
            prevKv.close();
        }
        prevKv = kv;
        return this;
    }

    @Override
    PutResultImpl<K, V> asResult() {
        return this;
    }

    @Override
    protected void reset() {
        if (null != prevKv) {
            prevKv.close();
            prevKv = null;
        }
        super.reset();
    }

}
