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
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.api.kv.result.KeyValue;

/**
 * An implementation of {@link DeleteResult}.
 */
@Accessors(fluent = true, chain = true)
@Setter
@Getter
@ToString
public class DeleteResultImpl<K, V> extends ResultImpl<K, V, DeleteResultImpl<K, V>> implements DeleteResult<K, V> {

    private List<KeyValue<K, V>> prevKvs = Collections.emptyList();
    private long numDeleted = 0L;

    DeleteResultImpl(Handle<DeleteResultImpl<K, V>> handle) {
        super(OpType.DELETE, handle);
    }

    public DeleteResultImpl<K, V> prevKvs(List<KeyValue<K, V>> kvs) {
        prevKvs.forEach(KeyValue::close);
        prevKvs.clear();
        prevKvs = kvs;
        return this;
    }

    @Override
    public List<KeyValue<K, V>> getPrevKvsAndClear() {
        List<KeyValue<K, V>> prevKvsToReturn = prevKvs;
        prevKvs = Collections.emptyList();
        return prevKvsToReturn;
    }

    @Override
    protected void reset() {
        this.numDeleted = 0;
        this.prevKvs.forEach(KeyValue::close);
        this.prevKvs = Collections.emptyList();

        super.reset();
    }

    @Override
    DeleteResultImpl<K, V> asResult() {
        return this;
    }

}
