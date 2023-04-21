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

package org.apache.bookkeeper.api.kv.impl.op;

import io.netty.util.Recycler;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.api.kv.impl.options.OptionFactoryImpl;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.op.CompareTarget;
import org.apache.bookkeeper.api.kv.op.DeleteOp;
import org.apache.bookkeeper.api.kv.op.IncrementOp;
import org.apache.bookkeeper.api.kv.op.OpFactory;
import org.apache.bookkeeper.api.kv.op.PutOp;
import org.apache.bookkeeper.api.kv.op.RangeOp;
import org.apache.bookkeeper.api.kv.op.TxnOpBuilder;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.IncrementOption;
import org.apache.bookkeeper.api.kv.options.OptionFactory;
import org.apache.bookkeeper.api.kv.options.PutOption;
import org.apache.bookkeeper.api.kv.options.RangeOption;

/**
 * A default implementation of {@link OpFactory} to create operators.
 */
@Accessors(fluent = true)
public class OpFactoryImpl<K, V> implements OpFactory<K, V> {

    @Getter
    private final OptionFactory<K> optionFactory = new OptionFactoryImpl<>();

    private final Recycler<PutOpImpl<K, V>> putOpRecycler = new Recycler<PutOpImpl<K, V>>() {
        @Override
        protected PutOpImpl<K, V> newObject(Handle<PutOpImpl<K, V>> handle) {
            return new PutOpImpl<>(handle);
        }
    };

    private final Recycler<DeleteOpImpl<K, V>> deleteOpRecycler = new Recycler<DeleteOpImpl<K, V>>() {
        @Override
        protected DeleteOpImpl<K, V> newObject(Handle<DeleteOpImpl<K, V>> handle) {
            return new DeleteOpImpl<>(handle);
        }
    };

    private final Recycler<RangeOpImpl<K, V>> rangeOpRecycler = new Recycler<RangeOpImpl<K, V>>() {
        @Override
        protected RangeOpImpl<K, V> newObject(Handle<RangeOpImpl<K, V>> handle) {
            return new RangeOpImpl<>(handle);
        }
    };

    private final Recycler<CompareOpImpl<K, V>> compareOpRecycler = new Recycler<CompareOpImpl<K, V>>() {
        @Override
        protected CompareOpImpl<K, V> newObject(Handle<CompareOpImpl<K, V>> handle) {
            return new CompareOpImpl<>(handle);
        }
    };

    private final Recycler<IncrementOpImpl<K, V>> incrementOpRecycler = new Recycler<IncrementOpImpl<K, V>>() {
        @Override
        protected IncrementOpImpl<K, V> newObject(Handle<IncrementOpImpl<K, V>> handle) {
            return new IncrementOpImpl<>(handle);
        }
    };

    private final Recycler<TxnOpImpl<K, V>> txnOpRecycler = new Recycler<TxnOpImpl<K, V>>() {
        @Override
        protected TxnOpImpl<K, V> newObject(Handle<TxnOpImpl<K, V>> handle) {
            return new TxnOpImpl<>(handle);
        }
    };

    private final Recycler<TxnOpBuilderImpl<K, V>> txnOpBuilderRecycler = new Recycler<TxnOpBuilderImpl<K, V>>() {
        @Override
        protected TxnOpBuilderImpl<K, V> newObject(Handle<TxnOpBuilderImpl<K, V>> handle) {
            return new TxnOpBuilderImpl<>(
                handle,
                txnOpRecycler);
        }
    };

    @Override
    public PutOp<K, V> newPut(K key, V value, PutOption<K> option) {
        return putOpRecycler.get()
            .key(key)
            .value(value)
            .option(option);
    }

    @Override
    public IncrementOp<K, V> newIncrement(K key, long amount, IncrementOption<K> option) {
        return incrementOpRecycler.get()
            .key(key)
            .amount(amount)
            .option(option);
    }

    @Override
    public DeleteOp<K, V> newDelete(K key, DeleteOption<K> option) {
        return deleteOpRecycler.get()
            .key(key)
            .option(option);
    }

    @Override
    public RangeOp<K, V> newRange(K key, RangeOption<K> option) {
        return rangeOpRecycler.get()
            .key(key)
            .option(option);
    }

    @Override
    public TxnOpBuilder<K, V> newTxn() {
        return txnOpBuilderRecycler.get();
    }

    @Override
    public CompareOp<K, V> compareVersion(CompareResult result, K key, long version) {
        return compareOpRecycler.get()
            .target(CompareTarget.VERSION)
            .result(result)
            .key(key)
            .value(null)
            .revision(version);
    }

    @Override
    public CompareOp<K, V> compareModRevision(CompareResult result, K key, long revision) {
        return compareOpRecycler.get()
            .target(CompareTarget.MOD)
            .result(result)
            .key(key)
            .value(null)
            .revision(revision);
    }

    @Override
    public CompareOp<K, V> compareCreateRevision(CompareResult result, K key, long revision) {
        return compareOpRecycler.get()
            .target(CompareTarget.CREATE)
            .result(result)
            .key(key)
            .value(null)
            .revision(revision);
    }

    @Override
    public CompareOp<K, V> compareValue(CompareResult result, K key, V value) {
        return compareOpRecycler.get()
            .target(CompareTarget.VALUE)
            .result(result)
            .key(key)
            .value(value)
            .revision(-1L);
    }

}
