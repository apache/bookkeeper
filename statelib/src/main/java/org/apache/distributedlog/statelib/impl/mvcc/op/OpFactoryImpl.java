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

package org.apache.distributedlog.statelib.impl.mvcc.op;

import java.util.Optional;
import org.apache.distributedlog.statelib.api.mvcc.op.CompareOp;
import org.apache.distributedlog.statelib.api.mvcc.op.CompareResult;
import org.apache.distributedlog.statelib.api.mvcc.op.CompareTarget;
import org.apache.distributedlog.statelib.api.mvcc.op.DeleteOpBuilder;
import org.apache.distributedlog.statelib.api.mvcc.op.OpFactory;
import org.apache.distributedlog.statelib.api.mvcc.op.PutOpBuilder;
import org.apache.distributedlog.statelib.api.mvcc.op.RangeOpBuilder;
import org.apache.distributedlog.statelib.api.mvcc.op.TxnOpBuilder;

/**
 * A default implementation of {@link OpFactory} to create operators.
 */
public class OpFactoryImpl<K, V> implements OpFactory<K, V> {

    @Override
    public PutOpBuilder<K, V> buildPutOp() {
        return PutOpImpl.newBuilder();
    }

    @Override
    public DeleteOpBuilder<K, V> buildDeleteOp() {
        return DeleteOpImpl.newBuilder();
    }

    @Override
    public RangeOpBuilder<K, V> buildRangeOp() {
        return RangeOpImpl.newBuilder();
    }

    @Override
    public TxnOpBuilder<K, V> buildTxnOp() {
        return TxnOpImpl.newBuilder();
    }

    @Override
    public CompareOp<K, V> compareVersion(CompareResult result, K key, long version) {
        return new CompareOpImpl<>(
            CompareTarget.VERSION,
            result,
            key,
            null,
            version);
    }

    @Override
    public CompareOp<K, V> compareModRevision(CompareResult result, K key, long revision) {
        return new CompareOpImpl<>(
            CompareTarget.MOD,
            result,
            key,
            null,
            revision);
    }

    @Override
    public CompareOp<K, V> compareCreateRevision(CompareResult result, K key, long revision) {
        return new CompareOpImpl<>(
            CompareTarget.CREATE,
            result,
            key,
            null,
            revision);
    }

    @Override
    public CompareOp<K, V> compareValue(CompareResult result, K key, V value) {
        return new CompareOpImpl<>(
            CompareTarget.VALUE,
            result,
            key,
            Optional.ofNullable(value),
            -1L);
    }

}
