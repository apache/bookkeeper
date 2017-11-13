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
package org.apache.bookkeeper.util;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * @Deprecated since 4.6.0, in favor of using {@link org.apache.bookkeeper.common.util.OrderedScheduler}.
 */
public class OrderedSafeExecutor extends org.apache.bookkeeper.common.util.OrderedScheduler {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends AbstractBuilder<OrderedSafeExecutor> {

        public OrderedSafeExecutor build() {
            if (null == threadFactory) {
                threadFactory = Executors.defaultThreadFactory();
            }
            return new OrderedSafeExecutor(name, numThreads, threadFactory, statsLogger,
                                           traceTaskExecution, warnTimeMicroSec, maxTasksInQueue);
        }

    }

    /**
     * Constructs Safe executor
     *
     * @param numThreads
     *            - number of threads
     * @param baseName
     *            - base name of executor threads
     * @param threadFactory
     *            - for constructing threads
     * @param statsLogger
     *            - for reporting executor stats
     * @param traceTaskExecution
     *            - should we stat task execution
     * @param warnTimeMicroSec
     *            - log long task exec warning after this interval
     * @param maxTasksInQueue
     *            - maximum items allowed in a thread queue. -1 for no limit
     */
    private OrderedSafeExecutor(String baseName, int numThreads, ThreadFactory threadFactory,
                                StatsLogger statsLogger, boolean traceTaskExecution,
                                long warnTimeMicroSec, int maxTasksInQueue) {
        super(baseName, numThreads, threadFactory, statsLogger, traceTaskExecution, warnTimeMicroSec, maxTasksInQueue);
    }

    /**
     * schedules a one time action to execute
     */
    public void submit(SafeRunnable r) {
        super.submit(r);
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key
     * @param orderingKey
     * @param r
     */
    public ListenableFuture<?> submitOrdered(Object orderingKey, SafeRunnable r) {
        return super.submitOrdered(orderingKey, r);
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key
     * @param orderingKey
     * @param r
     */
    public void submitOrdered(long orderingKey, SafeRunnable r) {
        super.submitOrdered(orderingKey, r);
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key
     * @param orderingKey
     * @param r
     */
    public void submitOrdered(int orderingKey, SafeRunnable r) {
        super.submitOrdered(orderingKey, r);
    }

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param command - the SafeRunnable to execute
     * @param delay - the time from now to delay execution
     * @param unit - the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose get() method will return null upon completion
     */
    public ScheduledFuture<?> schedule(SafeRunnable command, long delay, TimeUnit unit) {
        return super.schedule(command, delay, unit);
    }

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param orderingKey - the key used for ordering
     * @param command - the SafeRunnable to execute
     * @param delay - the time from now to delay execution
     * @param unit - the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose get() method will return null upon completion
     */
    public ScheduledFuture<?> scheduleOrdered(Object orderingKey, SafeRunnable command, long delay, TimeUnit unit) {
        return super.scheduleOrdered(orderingKey, command, delay, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period;
     *
     * For more details check scheduleAtFixedRate in interface ScheduledExecutorService
     *
     * @param command - the SafeRunnable to execute
     * @param initialDelay - the time to delay first execution
     * @param period - the period between successive executions
     * @param unit - the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose get()
     * method will throw an exception upon cancellation
     */
    public ScheduledFuture<?> scheduleAtFixedRate(SafeRunnable command, long initialDelay, long period, TimeUnit unit) {
        return super.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period;
     *
     * For more details check scheduleAtFixedRate in interface ScheduledExecutorService
     *
     * @param orderingKey - the key used for ordering
     * @param command - the SafeRunnable to execute
     * @param initialDelay - the time to delay first execution
     * @param period - the period between successive executions
     * @param unit - the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose get() method
     * will throw an exception upon cancellation
     */
    public ScheduledFuture<?> scheduleAtFixedRateOrdered(Object orderingKey, SafeRunnable command, long initialDelay,
            long period, TimeUnit unit) {
        return super.scheduleAtFixedRateOrdered(orderingKey, command, initialDelay, period, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and subsequently
     * with the given delay between the termination of one execution and the commencement of the next.
     *
     * For more details check scheduleWithFixedDelay in interface ScheduledExecutorService
     *
     * @param command - the SafeRunnable to execute
     * @param initialDelay - the time to delay first execution
     * @param delay - the delay between the termination of one execution and the commencement of the next
     * @param unit - the time unit of the initialDelay and delay parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose get() method
     * will throw an exception upon cancellation
     */
    public ScheduledFuture<?> scheduleWithFixedDelay(SafeRunnable command, long initialDelay, long delay,
            TimeUnit unit) {
        return super.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and subsequently
     * with the given delay between the termination of one execution and the commencement of the next.
     *
     * For more details check scheduleWithFixedDelay in interface ScheduledExecutorService
     *
     * @param orderingKey - the key used for ordering
     * @param command - the SafeRunnable to execute
     * @param initialDelay - the time to delay first execution
     * @param delay - the delay between the termination of one execution and the commencement of the next
     * @param unit - the time unit of the initialDelay and delay parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose get() method
     * will throw an exception upon cancellation
     */
    public ScheduledFuture<?> scheduleWithFixedDelayOrdered(Object orderingKey, SafeRunnable command, long initialDelay,
            long delay, TimeUnit unit) {
        return super.scheduleWithFixedDelayOrdered(orderingKey, command, initialDelay, delay, unit);
    }

    /**
     * Generic callback implementation which will run the
     * callback in the thread which matches the ordering key
     */
    public static abstract class OrderedSafeGenericCallback<T>
            implements GenericCallback<T> {
        private static final Logger LOG = LoggerFactory.getLogger(OrderedSafeGenericCallback.class);

        private final OrderedSafeExecutor executor;
        private final long orderingKey;

        /**
         * @param executor The executor on which to run the callback
         * @param orderingKey Key used to decide which thread the callback
         *                    should run on.
         */
        public OrderedSafeGenericCallback(OrderedSafeExecutor executor, long orderingKey) {
            this.executor = executor;
            this.orderingKey = orderingKey;
        }

        @Override
        public final void operationComplete(final int rc, final T result) {
            // during closing, callbacks that are error out might try to submit to
            // the scheduler again. if the submission will go to same thread, we
            // don't need to submit to executor again. this is also an optimization for
            // callback submission
            if (Thread.currentThread().getId() == executor.getThreadID(orderingKey)) {
                safeOperationComplete(rc, result);
            } else {
                try {
                    executor.submitOrdered(orderingKey, new SafeRunnable() {
                        @Override
                        public void safeRun() {
                            safeOperationComplete(rc, result);
                        }
                        @Override
                        public String toString() {
                            return String.format("Callback(key=%s, name=%s)",
                                                 orderingKey,
                                                 OrderedSafeGenericCallback.this);
                        }
                    });
                } catch (RejectedExecutionException re) {
                    LOG.warn("Failed to submit callback for {} : ", orderingKey, re);
                }
            }
        }

        public abstract void safeOperationComplete(int rc, T result);
    }
}
