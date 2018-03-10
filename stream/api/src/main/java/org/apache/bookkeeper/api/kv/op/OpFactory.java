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

package org.apache.bookkeeper.api.kv.op;

import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.IncrementOption;
import org.apache.bookkeeper.api.kv.options.OptionFactory;
import org.apache.bookkeeper.api.kv.options.PutOption;
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;

/**
 * A factory used for building operators to access the mvcc store.
 *
 * @param <K> key type.
 * @param <V> value type.
 */
@Public
@Evolving
public interface OpFactory<K, V> {

    OptionFactory<K> optionFactory();

    PutOp<K, V> newPut(K key, V value, PutOption<K> option);

    DeleteOp<K, V> newDelete(K key, DeleteOption<K> option);

    RangeOp<K, V> newRange(K key, RangeOption<K> option);

    IncrementOp<K, V> newIncrement(K key, long amount, IncrementOption<K> option);

    TxnOpBuilder<K, V> newTxn();

    CompareOp<K, V> compareVersion(CompareResult result, K key, long version);

    CompareOp<K, V> compareModRevision(CompareResult result, K key, long revision);

    CompareOp<K, V> compareCreateRevision(CompareResult result, K key, long revision);

    CompareOp<K, V> compareValue(CompareResult result, K key, V value);

}
