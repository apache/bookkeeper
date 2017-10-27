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
import org.apache.distributedlog.statestore.api.mvcc.result.DeleteResult;

/**
 * An implementation of {@link DeleteResult}.
 */
@Setter
public class DeleteResultImpl<K, V> extends ResultImpl<K, V, DeleteResultImpl<K, V>> implements DeleteResult<K, V> {

    private List<KVRecord<K, V>> prevKvs = Collections.emptyList();
    private long numDeleted = 0L;

    DeleteResultImpl(Handle<DeleteResultImpl<K, V>> handle) {
        super(OpType.DELETE, handle);
    }

    @Override
    public long numDeleted() {
        return numDeleted;
    }

    @Override
    public List<KVRecord<K, V>> prevKvs() {
        return prevKvs;
    }

    @Override
    protected void reset() {
        this.numDeleted = 0;
        this.prevKvs.forEach(KVRecord::recycle);
        this.prevKvs.clear();
        this.prevKvs = Collections.emptyList();

        super.reset();
    }

    @Override
    DeleteResultImpl<K, V> asResult() {
        return this;
    }

}
