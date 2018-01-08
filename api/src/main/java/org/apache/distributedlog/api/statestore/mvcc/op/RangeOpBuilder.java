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

package org.apache.distributedlog.api.statestore.mvcc.op;

import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;

/**
 * RangeOpBuilder to build a range operator.
 */
@Public
@Evolving
public interface RangeOpBuilder<K, V> extends OpBuilder<K, V, RangeOp<K, V>, RangeOpBuilder<K, V>> {

    RangeOpBuilder<K, V> key(K key);

    RangeOpBuilder<K, V> nullableKey(K key);

    RangeOpBuilder<K, V> endKey(K endKey);

    RangeOpBuilder<K, V> nullableEndKey(K endKey);

    RangeOpBuilder<K, V> isRangeOp(boolean isRangeOp);

    RangeOpBuilder<K, V> limit(long limit);

    RangeOpBuilder<K, V> minModRev(long rev);

    RangeOpBuilder<K, V> maxModRev(long rev);

    RangeOpBuilder<K, V> minCreateRev(long rev);

    RangeOpBuilder<K, V> maxCreateRev(long rev);

}
