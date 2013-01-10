package org.apache.bookkeeper.util;

/**
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

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;

/**
 * This class provides 2 things over the java {@link ScheduledExecutorService}.
 *
 * 1. It takes {@link SafeRunnable objects} instead of plain Runnable objects.
 * This means that exceptions in scheduled tasks wont go unnoticed and will be
 * logged.
 *
 * 2. It supports submitting tasks with an ordering key, so that tasks submitted
 * with the same key will always be executed in order, but tasks across
 * different keys can be unordered. This retains parallelism while retaining the
 * basic amount of ordering we want (e.g. , per ledger handle). Ordering is
 * achieved by hashing the key objects to threads by their {@link #hashCode()}
 * method.
 *
 */
public class OrderedSafeExecutor {
    ExecutorService threads[];
    Random rand = new Random();

    public OrderedSafeExecutor(int numThreads) {
        if (numThreads <= 0) {
            throw new IllegalArgumentException();
        }

        threads = new ExecutorService[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = Executors.newSingleThreadExecutor();
        }
    }

    ExecutorService chooseThread() {
        // skip random # generation in this special case
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[rand.nextInt(threads.length)];

    }

    ExecutorService chooseThread(Object orderingKey) {
        // skip hashcode generation in this special case
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[MathUtils.signSafeMod(orderingKey.hashCode(), threads.length)];

    }

    /**
     * schedules a one time action to execute
     */
    public void submit(SafeRunnable r) {
        chooseThread().submit(r);
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key
     * @param orderingKey
     * @param r
     */
    public void submitOrdered(Object orderingKey, SafeRunnable r) {
        chooseThread(orderingKey).submit(r);
    }

    public void shutdown() {
        for (int i = 0; i < threads.length; i++) {
            threads[i].shutdown();
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean ret = true;
        for (int i = 0; i < threads.length; i++) {
            ret = ret && threads[i].awaitTermination(timeout, unit);
        }
        return ret;
    }

    /**
     * Generic callback implementation which will run the
     * callback in the thread which matches the ordering key
     */
    public static abstract class OrderedSafeGenericCallback<T>
            implements GenericCallback<T> {
        private final OrderedSafeExecutor executor;
        private final Object orderingKey;

        /**
         * @param executor The executor on which to run the callback
         * @param orderingKey Key used to decide which thread the callback
         *                    should run on.
         */
        public OrderedSafeGenericCallback(OrderedSafeExecutor executor, Object orderingKey) {
            this.executor = executor;
            this.orderingKey = orderingKey;
        }

        @Override
        public final void operationComplete(final int rc, final T result) {
            executor.submitOrdered(orderingKey, new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        safeOperationComplete(rc, result);
                    }
                });
        }

        public abstract void safeOperationComplete(int rc, T result);
    }
}
