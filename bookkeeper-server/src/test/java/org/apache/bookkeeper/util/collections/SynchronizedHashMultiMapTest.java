/*
 *
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
 *
 */
package org.apache.bookkeeper.util.collections;

import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for SynchronizedHashMultiMap.
 */
public class SynchronizedHashMultiMapTest {
    @Test
    public void testGetAnyKey() {
        SynchronizedHashMultiMap<Integer, Integer> map = new SynchronizedHashMultiMap<>();
        Assert.assertFalse(map.getAnyKey().isPresent());

        map.put(1, 2);
        Assert.assertEquals(map.getAnyKey().get(), Integer.valueOf(1));

        map.put(1, 3);
        Assert.assertEquals(map.getAnyKey().get(), Integer.valueOf(1));

        map.put(2, 4);
        int res = map.getAnyKey().get();
        Assert.assertTrue(res == 1 || res == 2);

        map.removeIf((k, v) -> k == 1);
        Assert.assertEquals(map.getAnyKey().get(), Integer.valueOf(2));
    }

    @Test
    public void testRemoveAny() {
        SynchronizedHashMultiMap<Integer, Integer> map = new SynchronizedHashMultiMap<>();
        Assert.assertFalse(map.removeAny(1).isPresent());

        map.put(1, 2);
        map.put(1, 3);
        map.put(2, 4);
        map.put(2, 4);

        Optional<Integer> v = map.removeAny(1);
        int firstVal = v.get();
        Assert.assertTrue(firstVal == 2 || firstVal == 3);

        v = map.removeAny(1);
        int secondVal = v.get();
        Assert.assertTrue(secondVal == 2 || secondVal == 3);
        Assert.assertNotEquals(secondVal, firstVal);

        v = map.removeAny(2);
        Assert.assertTrue(v.isPresent());
        Assert.assertEquals(v.get(), Integer.valueOf(4));

        Assert.assertFalse(map.removeAny(1).isPresent());
        Assert.assertFalse(map.removeAny(2).isPresent());
        Assert.assertFalse(map.removeAny(3).isPresent());
    }

    @Test
    public void testRemoveIf() {
        SynchronizedHashMultiMap<Integer, Integer> map = new SynchronizedHashMultiMap<>();
        Assert.assertEquals(map.removeIf((k, v) -> true), 0);

        map.put(1, 2);
        map.put(1, 3);
        map.put(2, 4);
        map.put(2, 4);

        Assert.assertEquals(map.removeIf((k, v) -> v == 4), 1);
        Assert.assertEquals(map.removeIf((k, v) -> k == 1), 2);

        map.put(1, 2);
        map.put(1, 3);
        map.put(2, 4);

        Assert.assertEquals(map.removeIf((k, v) -> false), 0);
        Assert.assertEquals(map.removeIf((k, v) -> true), 3);
    }
}
