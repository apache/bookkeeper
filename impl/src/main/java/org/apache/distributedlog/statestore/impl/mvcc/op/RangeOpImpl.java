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

import java.util.function.Predicate;
import org.apache.distributedlog.api.statestore.mvcc.op.OpType;
import org.apache.distributedlog.api.statestore.mvcc.op.RangeOp;
import org.apache.distributedlog.api.statestore.mvcc.op.RangeOpBuilder;
import org.apache.distributedlog.statestore.impl.mvcc.MVCCRecord;
import org.inferred.freebuilder.FreeBuilder;

/**
 * A range operation.
 */
@FreeBuilder
public interface RangeOpImpl<K, V> extends RangeOp<K, V>, Predicate<MVCCRecord> {

    @Override
    default boolean test(MVCCRecord record) {

        return record.compareModRev(maxModRev()) <= 0
            && record.compareModRev(minModRev()) >= 0
            && record.compareCreateRev(maxCreateRev()) <= 0
            && record.compareCreateRev(minCreateRev()) >= 0;

    }

    /**
     * Builder to build a range operator.
     */
    class Builder<K, V> extends RangeOpImpl_Builder<K, V> implements RangeOpBuilder<K, V> {

        private Builder() {
            type(OpType.RANGE);
            revision(-1L);
            limit(-1);
            isRangeOp(false);
            minModRev(Long.MIN_VALUE);
            maxModRev(Long.MAX_VALUE);
            minCreateRev(Long.MIN_VALUE);
            maxCreateRev(Long.MAX_VALUE);
        }

        @Override
        public RangeOpImpl<K, V> build() {
            checkArgument(type() == OpType.RANGE, "Invalid type "  + type() + " is configured");
            checkArgument(isRangeOp() || (!isRangeOp() && key().isPresent()),
                "No key is provided from a single get operation");
            return super.build();
        }
    }

    static <K, V> RangeOpBuilder<K, V> newBuilder() {
        return new Builder<>();
    }

}
