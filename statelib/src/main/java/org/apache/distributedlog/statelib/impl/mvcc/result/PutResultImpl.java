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

package org.apache.distributedlog.statelib.impl.mvcc.result;

import io.netty.util.Recycler.Handle;
import lombok.Setter;
import org.apache.distributedlog.statelib.api.mvcc.KVRecord;
import org.apache.distributedlog.statelib.api.mvcc.op.OpType;
import org.apache.distributedlog.statelib.api.mvcc.result.PutResult;

/**
 * An implementation of {@link PutResult}.
 */
@Setter
public class PutResultImpl<K, V> extends ResultImpl<K, V, PutResultImpl<K, V>> implements PutResult<K, V> {

    private KVRecord<K, V> prevKV;

    PutResultImpl(Handle<PutResultImpl<K, V>> handle) {
        super(OpType.PUT, handle);
    }

    @Override
    public KVRecord<K, V> prevKV() {
        return prevKV;
    }

    @Override
    PutResultImpl<K, V> asResult() {
        return this;
    }

    @Override
    protected void reset() {
        if (null != prevKV) {
            prevKV.recycle();
            prevKV = null;
        }
        super.reset();
    }

}
