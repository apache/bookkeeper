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

package org.apache.distributedlog.statestore.api.mvcc.op;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Data
@Getter
@RequiredArgsConstructor
public class CompareOp<K, V> {

    public static <K, V> CompareOp<K, V> compareVersion(CompareResult result,
                                                        K key,
                                                        long version) {
        return new CompareOp<>(
            CompareTarget.VERSION,
            result,
            key,
            null,
            version);
    }

    public static <K, V> CompareOp<K, V> compareModRevision(CompareResult result,
                                                            K key,
                                                            long revision) {
        return new CompareOp<>(
            CompareTarget.MOD,
            result,
            key,
            null,
            revision);
    }

    public static <K, V> CompareOp<K, V> compareCreateRevision(CompareResult result,
                                                               K key,
                                                               long revision) {
        return new CompareOp<>(
            CompareTarget.CREATE,
            result,
            key,
            null,
            revision);
    }

    public static <K, V> CompareOp<K, V> compareValue(CompareResult result,
                                                      K key,
                                                      V value) {
        return new CompareOp<>(
            CompareTarget.CREATE,
            result,
            key,
            value,
            -1L);
    }


    private final CompareTarget target;
    private final CompareResult result;
    private final K key;
    private final V value;
    private final long revision;

}
