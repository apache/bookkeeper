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

package org.apache.bookkeeper.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.function.Function;
import org.apache.bookkeeper.common.collections.RecyclableArrayList.Recycler;
import org.junit.After;
import org.junit.Test;

/**
 * Unit test of {@link Watchable}.
 */
public class TestWatchable {

    private final Recycler<Watcher<Integer>> recycler;
    private final Watchable<Integer> watchable;

    public TestWatchable() {
        this.recycler = new Recycler<>();
        this.watchable = new Watchable<>(recycler);
    }

    @After
    public void teardown() {
        this.watchable.recycle();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAddWatcher() {
        Watcher<Integer> watcher = mock(Watcher.class);
        assertTrue(watchable.addWatcher(watcher));
        assertEquals(1, watchable.getNumWatchers());

        watchable.notifyWatchers(Function.identity(), 123);
        verify(watcher, times(1)).update(eq(123));

        // after the watcher is fired, watcher should be removed from watcher list.
        assertEquals(0, watchable.getNumWatchers());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteWatcher() {
        Watcher<Integer> watcher = mock(Watcher.class);
        assertTrue(watchable.addWatcher(watcher));
        assertEquals(1, watchable.getNumWatchers());
        assertTrue(watchable.deleteWatcher(watcher));
        assertEquals(0, watchable.getNumWatchers());

        watchable.notifyWatchers(Function.identity(), 123);
        verify(watcher, times(0)).update(anyInt());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMultipleWatchers() {
        Watcher<Integer> watcher1 = mock(Watcher.class);
        Watcher<Integer> watcher2 = mock(Watcher.class);

        assertTrue(watchable.addWatcher(watcher1));
        assertTrue(watchable.addWatcher(watcher2));
        assertEquals(2, watchable.getNumWatchers());

        watchable.notifyWatchers(Function.identity(), 123);
        verify(watcher1, times(1)).update(eq(123));
        verify(watcher2, times(1)).update(eq(123));
        assertEquals(0, watchable.getNumWatchers());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAddWatchMultipleTimes() {
        Watcher<Integer> watcher = mock(Watcher.class);

        int numTimes = 3;
        for (int i = 0; i < numTimes; i++) {
            assertTrue(watchable.addWatcher(watcher));
        }
        assertEquals(numTimes, watchable.getNumWatchers());

        watchable.notifyWatchers(Function.identity(), 123);
        verify(watcher, times(numTimes)).update(eq(123));
        assertEquals(0, watchable.getNumWatchers());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteWatchers() {
        Watcher<Integer> watcher1 = mock(Watcher.class);
        Watcher<Integer> watcher2 = mock(Watcher.class);

        assertTrue(watchable.addWatcher(watcher1));
        assertTrue(watchable.addWatcher(watcher2));
        assertEquals(2, watchable.getNumWatchers());
        watchable.deleteWatchers();
        assertEquals(0, watchable.getNumWatchers());

        watchable.notifyWatchers(Function.identity(), 123);
        verify(watcher1, times(0)).update(anyInt());
        verify(watcher2, times(0)).update(anyInt());
    }

}
