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

import io.netty.util.Recycler;

/**
 * Factory to create results.
 */
public class ResultFactory<K, V> {

    private final Recycler<PutResultImpl<K, V>> putResultRecycler;
    private final Recycler<DeleteResultImpl<K, V>> deleteResultRecycler;
    private final Recycler<RangeResultImpl<K, V>> rangeResultRecycler;
    private final Recycler<TxnResultImpl<K, V>> txnResultRecycler;
    private final Recycler<IncrementResultImpl<K, V>> incrementResultRecycler;

    public ResultFactory() {
        this.putResultRecycler = new Recycler<PutResultImpl<K, V>>() {
            @Override
            protected PutResultImpl<K, V> newObject(Handle<PutResultImpl<K, V>> handle) {
                return new PutResultImpl<>(handle);
            }
        };
        this.deleteResultRecycler = new Recycler<DeleteResultImpl<K, V>>() {
            @Override
            protected DeleteResultImpl<K, V> newObject(Handle<DeleteResultImpl<K, V>> handle) {
                return new DeleteResultImpl<>(handle);
            }
        };
        this.rangeResultRecycler = new Recycler<RangeResultImpl<K, V>>() {
            @Override
            protected RangeResultImpl<K, V> newObject(Handle<RangeResultImpl<K, V>> handle) {
                return new RangeResultImpl<>(handle);
            }
        };
        this.txnResultRecycler = new Recycler<TxnResultImpl<K, V>>() {
            @Override
            protected TxnResultImpl<K, V> newObject(Handle<TxnResultImpl<K, V>> handle) {
                return new TxnResultImpl<>(handle);
            }
        };
        this.incrementResultRecycler = new Recycler<IncrementResultImpl<K, V>>() {
            @Override
            protected IncrementResultImpl<K, V> newObject(Handle<IncrementResultImpl<K, V>> handle) {
                return new IncrementResultImpl<>(handle);
            }
        };
    }

    public PutResultImpl<K, V> newPutResult(long revision) {
        PutResultImpl<K, V> result = this.putResultRecycler.get();
        result.revision(revision);
        return result;
    }

    public DeleteResultImpl<K, V> newDeleteResult(long revision) {
        DeleteResultImpl<K, V> result = this.deleteResultRecycler.get();
        result.revision(revision);
        return result;
    }

    public RangeResultImpl<K, V> newRangeResult(long revision) {
        RangeResultImpl<K, V> result = this.rangeResultRecycler.get();
        result.revision(revision);
        return result;
    }

    public TxnResultImpl<K, V> newTxnResult(long revision) {
        TxnResultImpl<K, V> result = this.txnResultRecycler.get();
        result.revision(revision);
        return result;
    }

    public IncrementResultImpl<K, V> newIncrementResult(long revision) {
        IncrementResultImpl<K, V> result = this.incrementResultRecycler.get();
        result.revision(revision);
        return result;
    }

}
