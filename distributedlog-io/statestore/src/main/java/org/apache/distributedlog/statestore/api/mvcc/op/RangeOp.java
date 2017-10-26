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

import lombok.Data;
import lombok.Getter;
import org.inferred.freebuilder.FreeBuilder;

/**
 * A range operation.
 */
@FreeBuilder
public interface RangeOp<K, V> extends Op<K, V> {

    K endKey();

    int limit();

    long minModRev();

    long maxModRev();

    long minCreateRev();

    long maxCreateRev();

    class Builder<K, V> extends RangeOp_Builder<K, V> {

        private Builder() {
            type(OpType.RANGE);
            revision(-1L);
            limit(-1);
            minModRev(Long.MIN_VALUE);
            maxModRev(Long.MAX_VALUE);
            minCreateRev(Long.MIN_VALUE);
            maxCreateRev(Long.MAX_VALUE);
        }

        @Override
        public RangeOp<K, V> build() {
            checkArgument(type() == OpType.RANGE, "Invalid type "  + type() + " is configured");
            return super.build();
        }
    }

    static <K, V> Builder<K, V> newBuilder() {
        return new Builder<>();
    }

}
