/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.common.collections;

import static org.junit.Assert.assertEquals;

import org.apache.bookkeeper.common.collections.RecyclableHashSet.Recycler;
import org.junit.Test;

/**
 * Unit test of {@link RecyclableHashSet}.
 */
public class RecyclableHashSetTest {

    private final Recycler<Integer> recycler;

    public RecyclableHashSetTest() {
        this.recycler = new Recycler<>();
    }

    @Test
    public void testRecycle() {
        RecyclableHashSet<Integer> set = recycler.newInstance();
        for (int i = 0; i < 5; i++) {
            set.add(i);
        }
        assertEquals(5, set.size());
        set.recycle();
        assertEquals(0, set.size());
    }

}
