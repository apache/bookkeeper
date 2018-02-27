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


import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.Op;
import org.apache.bookkeeper.api.kv.result.TxnResult;

/**
 * Txn is the interface that wraps mini-transactions.
 *
 * <h3>Usage examples</h3>
 *
 * <pre>{@code
 * txn.If(
 *    new Cmp(KEY, Cmp.Op.GREATER, CmpTarget.value(VALUE)),
 *    new Cmp(KEY, cmp.Op.EQUAL, CmpTarget.version(2))
 * ).Then(
 *    Op.put(KEY2, VALUE2, PutOption.DEFAULT),
 *    Op.put(KEY3, VALUE3, PutOption.DEFAULT)
 * ).Else(
 *    Op.put(KEY4, VALUE4, PutOption.DEFAULT),
 *    Op.put(KEY4, VALUE4, PutOption.DEFAULT)
 * ).commit();
 * }</pre>
 *
 * <p>Txn also supports If, Then, and Else chaining. e.g.
 * <pre>{@code
 * txn.If(
 *    new Cmp(KEY, Cmp.Op.GREATER, CmpTarget.value(VALUE))
 * ).If(
 *    new Cmp(KEY, cmp.Op.EQUAL, CmpTarget.version(VERSION))
 * ).Then(
 *    Op.put(KEY2, VALUE2, PutOption.DEFAULT)
 * ).Then(
 *    Op.put(KEY3, VALUE3, PutOption.DEFAULT)
 * ).Else(
 *    Op.put(KEY4, VALUE4, PutOption.DEFAULT)
 * ).Else(
 *    Op.put(KEY4, VALUE4, PutOption.DEFAULT)
 * ).commit();
 * }</pre>
 */
public interface Txn<K, V> {

    /**
     * takes a list of comparison. If all comparisons passed in succeed,
     * the operations passed into Then() will be executed. Or the operations
     * passed into Else() will be executed.
     */
    // CHECKSTYLE.OFF: MethodName
    Txn<K, V> If(CompareOp... cmps);
    // CHECKSTYLE.ON: MethodName

    /**
     * takes a list of operations. The Ops list will be executed, if the
     * comparisons passed in If() succeed.
     */
    // CHECKSTYLE.OFF: MethodName
    Txn<K, V> Then(Op... ops);
    // CHECKSTYLE.ON: MethodName

    /**
     * takes a list of operations. The Ops list will be executed, if the
     * comparisons passed in If() fail.
     */
    // CHECKSTYLE.OFF: MethodName
    Txn<K, V> Else(Op... ops);
    // CHECKSTYLE.OFF: MethodName

    /**
     * tries to commit the transaction.
     *
     * @return a TxnResponse wrapped in CompletableFuture
     */
    CompletableFuture<TxnResult<K, V>> commit();

}
