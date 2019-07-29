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
package org.apache.bookkeeper.common.util;

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.stats.StatsLogger;

/**
 * This class provides 2 things over the java {@link ScheduledExecutorService}.
 *
 * <p>1. It takes {@link SafeRunnable objects} instead of plain Runnable objects.
 * This means that exceptions in scheduled tasks wont go unnoticed and will be
 * logged.
 *
 * <p>2. It supports submitting tasks with an ordering key, so that tasks submitted
 * with the same key will always be executed in order, but tasks across
 * different keys can be unordered. This retains parallelism while retaining the
 * basic amount of ordering we want (e.g. , per ledger handle). Ordering is
 * achieved by hashing the key objects to threads by their {@link #hashCode()}
 * method.
 */
public class OrderedScheduler extends OrderedExecutor implements ScheduledExecutorService {

    /**
     * Create a builder to build ordered scheduler.
     *
     * @return builder to build ordered scheduler.
     */
    public static SchedulerBuilder newSchedulerBuilder() {
        return new SchedulerBuilder();
    }

    /**
     * Builder to build ordered scheduler.
     */
    public static class SchedulerBuilder extends OrderedExecutor.AbstractBuilder<OrderedScheduler> {
        @Override
        public OrderedScheduler build() {
            if (null == threadFactory) {
                threadFactory = new DefaultThreadFactory(name);
            }
            return new OrderedScheduler(
                name,
                numThreads,
                threadFactory,
                statsLogger,
                traceTaskExecution,
                preserveMdcForTaskExecution,
                warnTimeMicroSec,
                maxTasksInQueue);
        }
    }

    /**
     * Constructs Safe executor.
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
     * @param preserveMdcForTaskExecution
     *            - should we preserve MDC for task execution
     * @param warnTimeMicroSec
     *            - log long task exec warning after this interval
     */
    private OrderedScheduler(String baseName,
                               int numThreads,
                               ThreadFactory threadFactory,
                               StatsLogger statsLogger,
                               boolean traceTaskExecution,
                               boolean preserveMdcForTaskExecution,
                               long warnTimeMicroSec,
                               int maxTasksInQueue) {
        super(baseName, numThreads, threadFactory, statsLogger, traceTaskExecution,
                preserveMdcForTaskExecution, warnTimeMicroSec, maxTasksInQueue, false /* enableBusyWait */);
    }

    @Override
    protected ScheduledThreadPoolExecutor createSingleThreadExecutor(ThreadFactory factory) {
        return new ScheduledThreadPoolExecutor(1, factory);
    }

    @Override
    protected ListeningScheduledExecutorService getBoundedExecutor(ThreadPoolExecutor executor) {
        return new BoundedScheduledExecutorService((ScheduledThreadPoolExecutor) executor, this.maxTasksInQueue);
    }

    @Override
    protected ListeningScheduledExecutorService addExecutorDecorators(ExecutorService executor) {
        return new OrderedSchedulerDecoratedThread((ListeningScheduledExecutorService) executor);
    }

    @Override
    public ListeningScheduledExecutorService chooseThread() {
        return (ListeningScheduledExecutorService) super.chooseThread();
    }

    @Override
    public ListeningScheduledExecutorService chooseThread(Object orderingKey) {
        return (ListeningScheduledExecutorService) super.chooseThread(orderingKey);
    }

    @Override
    public ListeningScheduledExecutorService chooseThread(long orderingKey) {
        return (ListeningScheduledExecutorService) super.chooseThread(orderingKey);
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key.
     *
     * @param orderingKey
     * @param callable
     */
    public <T> ListenableFuture<T> submitOrdered(Object orderingKey,
                                                 Callable<T> callable) {
        return chooseThread(orderingKey).submit(callable);
    }

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param command - the SafeRunnable to execute
     * @param delay - the time from now to delay execution
     * @param unit - the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose get() method
     *         will return null upon completion
     */
    public ScheduledFuture<?> schedule(SafeRunnable command, long delay, TimeUnit unit) {
        return chooseThread().schedule(timedRunnable(command), delay, unit);
    }

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param orderingKey - the key used for ordering
     * @param command - the SafeRunnable to execute
     * @param delay - the time from now to delay execution
     * @param unit - the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose get() method
     *         will return null upon completion
     */
    public ScheduledFuture<?> scheduleOrdered(Object orderingKey, SafeRunnable command, long delay, TimeUnit unit) {
        return chooseThread(orderingKey).schedule(command, delay, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period.
     *
     * <p>For more details check {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}.
     *
     * @param command - the SafeRunnable to execute
     * @param initialDelay - the time to delay first execution
     * @param period - the period between successive executions
     * @param unit - the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose get()
     * method will throw an exception upon cancellation
     */
    public ScheduledFuture<?> scheduleAtFixedRate(SafeRunnable command, long initialDelay, long period, TimeUnit unit) {
        return chooseThread().scheduleAtFixedRate(timedRunnable(command), initialDelay, period, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period.
     *
     * <p>For more details check {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}.
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
        return chooseThread(orderingKey).scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and subsequently
     * with the given delay between the termination of one execution and the commencement of the next.
     *
     * <p>For more details check {@link ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}
     * .
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
        return chooseThread().scheduleWithFixedDelay(timedRunnable(command), initialDelay, delay, unit);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and subsequently
     * with the given delay between the termination of one execution and the commencement of the next.
     *
     * <p>For more details check {@link ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}
     * .
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
        return chooseThread(orderingKey).scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }


    //
    // Methods for implementing {@link ScheduledExecutorService}
    //

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return chooseThread().schedule(timedRunnable(command), delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return chooseThread().schedule(timedCallable(callable), delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                  long initialDelay, long period, TimeUnit unit) {
        return chooseThread().scheduleAtFixedRate(timedRunnable(command), initialDelay, period, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                     long initialDelay, long delay, TimeUnit unit) {
        return chooseThread().scheduleWithFixedDelay(timedRunnable(command), initialDelay, delay, unit);
    }

    class OrderedSchedulerDecoratedThread extends ForwardingListeningExecutorService
        implements ListeningScheduledExecutorService {
        private final ListeningScheduledExecutorService delegate;

        private OrderedSchedulerDecoratedThread(ListeningScheduledExecutorService delegate) {
            this.delegate = delegate;
        }

        @Override
            protected ListeningExecutorService delegate() {
                return delegate;
            }

            @Override
            public ListenableScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
                return delegate.schedule(timedRunnable(command), delay, unit);
            }

            @Override
            public <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
                return delegate.schedule(timedCallable(callable), delay, unit);
            }

            @Override
            public ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                                    long initialDelay, long period, TimeUnit unit) {
                return delegate.scheduleAtFixedRate(timedRunnable(command), initialDelay, period, unit);
            }

            @Override
            public ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                                       long initialDelay, long delay, TimeUnit unit) {
                return delegate.scheduleAtFixedRate(timedRunnable(command), initialDelay, delay, unit);
            }

            @Override
            public <T> ListenableFuture<T> submit(Callable<T> task) {
                return super.submit(timedCallable(task));
            }

            @Override
            public ListenableFuture<?> submit(Runnable task) {
                return super.submit(timedRunnable(task));
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
                return super.invokeAll(timedCallables(tasks));
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                                 long timeout, TimeUnit unit) throws InterruptedException {
                return super.invokeAll(timedCallables(tasks), timeout, unit);
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                    throws InterruptedException, ExecutionException {
                return super.invokeAny(timedCallables(tasks));
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
                                   TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

                return super.invokeAny(timedCallables(tasks), timeout, unit);
            }

            @Override
            public <T> ListenableFuture<T> submit(Runnable task, T result) {
                return super.submit(timedRunnable(task), result);
            }

            @Override
            public void execute(Runnable command) {
                super.execute(timedRunnable(command));
            }
        }

}
