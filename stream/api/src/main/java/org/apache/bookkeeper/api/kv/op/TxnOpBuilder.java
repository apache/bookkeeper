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

/**
 * Builder to build {@link TxnOp}.
 */
public interface TxnOpBuilder<K, V> {

    /**
     * takes a list of comparison. If all comparisons passed in succeed,
     * the operations passed into Then() will be executed. Or the operations
     * passed into Else() will be executed.
     */
    // CHECKSTYLE.OFF: MethodName
    TxnOpBuilder<K, V> If(CompareOp... cmp);
    // CHECKSTYLE.ON: MethodName

    /**
     * takes a list of operations. The Ops list will be executed, if the
     * comparisons passed in If() succeed.
     */
    // CHECKSTYLE.OFF: MethodName
    TxnOpBuilder<K, V> Then(Op... ops);
    // CHECKSTYLE.ON: MethodName

    /**
     * takes a list of operations. The Ops list will be executed, if the
     * comparisons passed in If() fail.
     */
    // CHECKSTYLE.OFF: MethodName
    TxnOpBuilder<K, V> Else(Op... ops);
    // CHECKSTYLE.OFF: MethodName

    TxnOp<K, V> build();

}
