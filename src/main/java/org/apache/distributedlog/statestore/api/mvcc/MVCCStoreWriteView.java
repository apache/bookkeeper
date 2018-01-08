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

package org.apache.distributedlog.statestore.api.mvcc;

import org.apache.distributedlog.statestore.api.mvcc.op.DeleteOp;
import org.apache.distributedlog.statestore.api.mvcc.op.PutOp;
import org.apache.distributedlog.statestore.api.mvcc.op.TxnOp;
import org.apache.distributedlog.statestore.api.mvcc.result.DeleteResult;
import org.apache.distributedlog.statestore.api.mvcc.result.PutResult;
import org.apache.distributedlog.statestore.api.mvcc.result.TxnResult;

/**
 * The write view of a mvcc key/value store that supports write operations, such as put and delete.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface MVCCStoreWriteView<K, V> {

    PutResult<K, V> put(PutOp<K, V> op);

    DeleteResult<K, V> delete(DeleteOp<K, V> op);

    TxnResult<K, V> txn(TxnOp<K, V> op);


}
