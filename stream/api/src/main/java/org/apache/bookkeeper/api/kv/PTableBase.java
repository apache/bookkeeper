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
package org.apache.bookkeeper.api.kv;

import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.op.DeleteOp;
import org.apache.bookkeeper.api.kv.op.IncrementOp;
import org.apache.bookkeeper.api.kv.op.OpFactory;
import org.apache.bookkeeper.api.kv.op.PutOp;
import org.apache.bookkeeper.api.kv.op.RangeOp;
import org.apache.bookkeeper.api.kv.options.Options;

/**
 * A base class for {@link PTable}.
 */
public interface PTableBase<K, V> extends AutoCloseable {

    OpFactory<K, V> opFactory();

    default CompareOp<K, V> compareCreateRevision(CompareResult result, K key, long revision) {
        return opFactory().compareCreateRevision(result, key, revision);
    }

    default CompareOp<K, V> compareModRevision(CompareResult result, K key, long revision) {
        return opFactory().compareModRevision(result, key, revision);
    }

    default CompareOp<K, V> compareVersion(CompareResult result, K key, long version) {
        return opFactory().compareVersion(result, key, version);
    }

    default CompareOp<K, V> compareValue(CompareResult result, K key, V value) {
        return opFactory().compareValue(result, key, value);
    }

    default PutOp<K, V> newPut(K key, V value) {
        return opFactory().newPut(key, value, Options.blindPut());
    }

    default DeleteOp<K, V> newDelete(K key) {
        return opFactory().newDelete(key, Options.delete());
    }

    default IncrementOp<K, V> newIncrement(K key, long amount) {
        return opFactory().newIncrement(key, amount, Options.blindIncrement());
    }

    default IncrementOp<K, V> newIncrementAndGet(K key, long amount) {
        return opFactory().newIncrement(key, amount, Options.incrementAndGet());
    }

    default RangeOp<K, V> newGet(K key) {
        return opFactory().newRange(
            key,
            Options.get());
    }

    @Override
    void close();

}
