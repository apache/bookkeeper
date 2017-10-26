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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface TxnOp<K, V> extends Op<K, V> {

    List<CompareOp<K, V>> compareOps();

    List<Op<K, V>> successOps();

    List<Op<K, V>> failureOps();

    class Builder<K, V> extends TxnOp_Builder<K, V> {

        private Builder() {
            type(OpType.TXN);
        }

        @Override
        public TxnOp<K, V> build() {
            checkArgument(type() == OpType.TXN, "Invalid type "  + type() + " is configured");
            checkArgument(!compareOps().isEmpty(), "No compare op is specified");
            return super.build();
        }
    }

    static <K, V> Builder<K, V> newBuilder() {
        return new Builder<>();
    }

}
