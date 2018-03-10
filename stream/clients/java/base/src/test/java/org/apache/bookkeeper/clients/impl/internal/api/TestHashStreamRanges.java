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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Maps;
import java.util.NavigableMap;
import org.apache.bookkeeper.stream.proto.RangeKeyType;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.junit.Test;

/**
 * Unit test for {@link HashStreamRanges}.
 */
public class TestHashStreamRanges {

    @Test
    public void testConstructor() {
        NavigableMap<Long, RangeProperties> ranges = Maps.newTreeMap();
        for (long hashKey = 0L; hashKey < 10L; hashKey++) {
            RangeProperties props = RangeProperties.newBuilder()
                .setStorageContainerId(hashKey)
                .setRangeId(hashKey)
                .setStartHashKey(hashKey)
                .setEndHashKey(hashKey)
                .build();
            ranges.put(hashKey, props);
        }

        HashStreamRanges hsr = new HashStreamRanges(
            ranges,
            9L);

        assertEquals(RangeKeyType.HASH, hsr.getKeyType());
        assertEquals(ranges, hsr.getRanges());
        assertEquals(9L, hsr.getMaxRangeId());
    }

}
