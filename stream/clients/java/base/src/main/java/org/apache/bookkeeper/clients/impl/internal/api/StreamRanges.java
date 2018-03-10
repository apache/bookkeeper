/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.impl.internal.api;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.NavigableMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.bookkeeper.stream.proto.RangeKeyType;
import org.apache.bookkeeper.stream.proto.RangeProperties;

/**
 * The active ranges for a given stream at a given time.
 */
@Data
@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public abstract class StreamRanges<T> {

    public static HashStreamRanges ofHash(RangeKeyType keyType,
                                          NavigableMap<Long, RangeProperties> ranges) {
        checkArgument(RangeKeyType.HASH == keyType,
            "Only hash routing is supported now. %s is not supported.", keyType);
        NavigableMap<Long, RangeProperties> readOnlyRanges = Collections.unmodifiableNavigableMap(ranges);
        long maxRangeId = 0L;
        for (RangeProperties props : ranges.values()) {
            maxRangeId = Math.max(maxRangeId, props.getRangeId());
        }
        return new HashStreamRanges(readOnlyRanges, maxRangeId);
    }

    private final RangeKeyType keyType;
    private final NavigableMap<T, RangeProperties> ranges;
    // the id (which is basically the max id among the ranges)
    // that tracks the changes to the range list of a stream.
    // the id here can be easily used for syncing the range list by
    // just comparing the id seen at client side and the id seen
    // at server side.
    private final long maxRangeId;

}
