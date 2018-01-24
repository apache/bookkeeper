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
package org.apache.distributedlog.api.kv;

import org.apache.distributedlog.api.kv.op.CompareOp;
import org.apache.distributedlog.api.kv.op.CompareResult;
import org.apache.distributedlog.api.kv.op.DeleteOp;
import org.apache.distributedlog.api.kv.op.OpFactory;
import org.apache.distributedlog.api.kv.op.PutOp;
import org.apache.distributedlog.api.kv.op.RangeOp;

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
        return opFactory().newPut(key, value, opFactory().optionFactory().newPutOption().prevKv(false).build());
    }

    default DeleteOp<K, V> newDelete(K key) {
        return opFactory().newDelete(key, opFactory().optionFactory().newDeleteOption().prevKv(false).build());
    }

    default RangeOp<K, V> newGet(K key) {
        return opFactory().newRange(
            key,
            opFactory().optionFactory().newRangeOption()
                .keysOnly(false)
                .countOnly(false)
                .limit(1)
                .endKey(null)
                .build());
    }

    @Override
    void close();

}
