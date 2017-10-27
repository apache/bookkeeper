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

package org.apache.distributedlog.statestore.impl.mvcc.op;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.distributedlog.statestore.api.mvcc.op.OpType;
import org.apache.distributedlog.statestore.api.mvcc.op.TxnOp;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
interface TxnOpImpl<K, V> extends TxnOp<K, V> {

    class BuilderImpl<K, V> extends TxnOpImpl_Builder<K, V> implements Builder<K, V> {

        private BuilderImpl() {
            type(OpType.TXN);
        }

        @Override
        public TxnOpImpl<K, V> build() {
            checkArgument(type() == OpType.TXN, "Invalid type "  + type() + " is configured");
            checkArgument(!compareOps().isEmpty(), "No compare op is specified");
            return super.build();
        }
    }

    static <K, V> Builder<K, V> newBuilder() {
        return new BuilderImpl<>();
    }

}
