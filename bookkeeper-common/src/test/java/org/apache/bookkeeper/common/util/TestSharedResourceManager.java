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

package org.apache.bookkeeper.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

/**
 * Unit test for {@link SharedResourceManager}.
 */
public class TestSharedResourceManager {

    private final LinkedList<MockScheduledFuture<?>> scheduledDestroyTasks =
        new LinkedList<>();

    private SharedResourceManager manager;

    private static class ResourceInstance {
        volatile boolean closed;
    }

    private static class ResourceFactory implements Resource<ResourceInstance> {
        @Override
        public ResourceInstance create() {
            return new ResourceInstance();
        }

        @Override
        public void close(ResourceInstance instance) {
            instance.closed = true;
        }
    }

    // Defines two kinds of resources
    private static final Resource<ResourceInstance> SHARED_FOO = new ResourceFactory();
    private static final Resource<ResourceInstance> SHARED_BAR = new ResourceFactory();

    @Before
    public void setUp() {
        manager = SharedResourceManager.create(new MockExecutorFactory());
    }

    @Test
    public void destroyResourceWhenRefCountReachesZero() {
        ResourceInstance foo1 = manager.get(SHARED_FOO);
        ResourceInstance sharedFoo = foo1;
        ResourceInstance foo2 = manager.get(SHARED_FOO);
        assertSame(sharedFoo, foo2);

        ResourceInstance bar1 = manager.get(SHARED_BAR);
        ResourceInstance sharedBar = bar1;

        manager.release(SHARED_FOO, foo2);
        // foo refcount not reached 0, thus shared foo is not closed
        assertTrue(scheduledDestroyTasks.isEmpty());
        assertFalse(sharedFoo.closed);

        manager.release(SHARED_FOO, foo1);

        // foo refcount has reached 0, a destroying task is scheduled
        assertEquals(1, scheduledDestroyTasks.size());
        MockScheduledFuture<?> scheduledDestroyTask = scheduledDestroyTasks.poll();
        assertEquals(SharedResourceManager.DESTROY_DELAY_SECONDS,
            scheduledDestroyTask.getDelay(TimeUnit.SECONDS));

        // Simluate that the destroyer executes the foo destroying task
        scheduledDestroyTask.runTask();
        assertTrue(sharedFoo.closed);

        // After the destroying, obtaining a foo will get a different instance
        ResourceInstance foo3 = manager.get(SHARED_FOO);
        assertNotSame(sharedFoo, foo3);

        manager.release(SHARED_BAR, bar1);

        // bar refcount has reached 0, a destroying task is scheduled
        assertEquals(1, scheduledDestroyTasks.size());
        scheduledDestroyTask = scheduledDestroyTasks.poll();
        assertEquals(SharedResourceManager.DESTROY_DELAY_SECONDS,
            scheduledDestroyTask.getDelay(TimeUnit.SECONDS));

        // Simulate that the destroyer executes the bar destroying task
        scheduledDestroyTask.runTask();
        assertTrue(sharedBar.closed);
    }

    @Test
    public void cancelDestroyTask() {
        ResourceInstance foo1 = manager.get(SHARED_FOO);
        ResourceInstance sharedFoo = foo1;
        manager.release(SHARED_FOO, foo1);
        // A destroying task for foo is scheduled
        MockScheduledFuture<?> scheduledDestroyTask = scheduledDestroyTasks.poll();
        assertFalse(scheduledDestroyTask.cancelled);

        // obtaining a foo before the destroying task is executed will cancel the destroy
        ResourceInstance foo2 = manager.get(SHARED_FOO);
        assertTrue(scheduledDestroyTask.cancelled);
        assertTrue(scheduledDestroyTasks.isEmpty());
        assertFalse(sharedFoo.closed);

        // And it will be the same foo instance
        assertSame(sharedFoo, foo2);

        // Release it and the destroying task is scheduled again
        manager.release(SHARED_FOO, foo2);
        scheduledDestroyTask = scheduledDestroyTasks.poll();
        assertFalse(scheduledDestroyTask.cancelled);
        scheduledDestroyTask.runTask();
        assertTrue(sharedFoo.closed);
    }

    @Test
    public void releaseWrongInstance() {
        ResourceInstance uncached = new ResourceInstance();
        try {
            manager.release(SHARED_FOO, uncached);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        ResourceInstance cached = manager.get(SHARED_FOO);
        try {
            manager.release(SHARED_FOO, uncached);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        manager.release(SHARED_FOO, cached);
    }

    @Test
    public void overreleaseInstance() {
        ResourceInstance foo1 = manager.get(SHARED_FOO);
        manager.release(SHARED_FOO, foo1);
        try {
            manager.release(SHARED_FOO, foo1);
            fail("Should throw IllegalStateException");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    private class MockExecutorFactory implements Supplier<ScheduledExecutorService> {
        @Override
        public ScheduledExecutorService get() {
            ScheduledExecutorService mockExecutor = mock(ScheduledExecutorService.class);
            when(mockExecutor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(
                (Answer<MockScheduledFuture<Void>>) invocation -> {
                    Object[] args = invocation.getArguments();
                    Runnable command = (Runnable) args[0];
                    long delay = (Long) args[1];
                    TimeUnit unit = (TimeUnit) args[2];
                    MockScheduledFuture<Void> future = new MockScheduledFuture<Void>(
                        command, delay, unit);
                    scheduledDestroyTasks.add(future);
                    return future;
                });
            return mockExecutor;
        }
    }

    private static class MockScheduledFuture<V> implements ScheduledFuture<V> {
        private boolean cancelled;
        private boolean finished;
        final Runnable command;
        final long delay;
        final TimeUnit unit;

        MockScheduledFuture(Runnable command, long delay, TimeUnit unit) {
            this.command = command;
            this.delay = delay;
            this.unit = unit;
        }

        void runTask() {
            command.run();
            finished = true;
        }

        @Override
        public boolean cancel(boolean interrupt) {
            if (cancelled || finished) {
                return false;
            }
            cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public long getDelay(TimeUnit targetUnit) {
            return targetUnit.convert(this.delay, this.unit);
        }

        @Override
        public int compareTo(Delayed o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDone() {
            return cancelled || finished;
        }

        @Override
        public V get() {
            throw new UnsupportedOperationException();
        }

        @Override
        public V get(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
    }

}
