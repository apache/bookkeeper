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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang.StringUtils;
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
 */
public class OrderedSafeExecutor {
    final static long WARN_TIME_MICRO_SEC_DEFAULT = TimeUnit.SECONDS.toMicros(1);
    final String name;
    final ScheduledThreadPoolExecutor threads[];
    final long threadIds[];
    final Random rand = new Random();
    final OpStatsLogger taskExecutionStats;
    final OpStatsLogger taskPendingStats;
    final boolean traceTaskExecution;
    final long warnTimeMicroSec;

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String name = "OrderedSafeExecutor";
        private int numThreads = Runtime.getRuntime().availableProcessors();
        private ThreadFactory threadFactory = null;
        private StatsLogger statsLogger = NullStatsLogger.INSTANCE;
        private boolean traceTaskExecution = false;
        private long warnTimeMicroSec = WARN_TIME_MICRO_SEC_DEFAULT;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder numThreads(int num) {
            this.numThreads = num;
            return this;
        }

        public Builder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public Builder statsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        public Builder traceTaskExecution(boolean enabled) {
            this.traceTaskExecution = enabled;
            return this;
        }

        public Builder traceTaskWarnTimeMicroSec(long warnTimeMicroSec) {
            this.warnTimeMicroSec = warnTimeMicroSec;
            return this;
        }

        public OrderedSafeExecutor build() {
            if (null == threadFactory) {
                threadFactory = Executors.defaultThreadFactory();
            }
            return new OrderedSafeExecutor(name, numThreads, threadFactory, statsLogger,
                                           traceTaskExecution, warnTimeMicroSec);
        }

    }

    private class TimedRunnable extends SafeRunnable {
        final SafeRunnable runnable;
        final long initNanos;

        TimedRunnable(SafeRunnable runnable) {
            this.runnable = runnable;
            this.initNanos = MathUtils.nowInNano();
         }

        @Override
        public void safeRun() {
            taskPendingStats.registerSuccessfulEvent(initNanos, TimeUnit.NANOSECONDS);
            long startNanos = MathUtils.nowInNano();
            this.runnable.safeRun();
            long elapsedMicroSec = MathUtils.elapsedMicroSec(startNanos);
            taskExecutionStats.registerSuccessfulEvent(elapsedMicroSec, TimeUnit.MICROSECONDS);
            if (elapsedMicroSec >= warnTimeMicroSec) {
                logger.warn("Runnable {}:{} took too long {} micros to execute.",
                            new Object[] { runnable, runnable.getClass(), elapsedMicroSec });
            }
        }
     }

    @Deprecated
    public OrderedSafeExecutor(int numThreads, String threadName) {
        this(threadName, numThreads, Executors.defaultThreadFactory(), NullStatsLogger.INSTANCE,
             false, WARN_TIME_MICRO_SEC_DEFAULT);
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
     */
    @SuppressWarnings("unchecked")
    private OrderedSafeExecutor(String baseName, int numThreads, ThreadFactory threadFactory,
                                StatsLogger statsLogger, boolean traceTaskExecution,
                                long warnTimeMicroSec) {
        Preconditions.checkArgument(numThreads > 0);
        Preconditions.checkArgument(!StringUtils.isBlank(baseName));

        this.warnTimeMicroSec = warnTimeMicroSec;
        name = baseName;
        threads = new ScheduledThreadPoolExecutor[numThreads];
        threadIds = new long[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] =  new ScheduledThreadPoolExecutor(1,
                    new ThreadFactoryBuilder()
                        .setNameFormat(name + "-orderedsafeexecutor-" + i + "-%d")
                        .setThreadFactory(threadFactory)
                        .build());
            threads[i].setMaximumPoolSize(1);

            // Save thread ids
            final int idx = i;
            try {
                threads[idx].submit(new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        threadIds[idx] = Thread.currentThread().getId();
                    }
                }).get();
            } catch (InterruptedException e) {
                throw new RuntimeException("Couldn't start thread " + i, e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Couldn't start thread " + i, e);
            }

            // Register gauges
            statsLogger.registerGauge(String.format("%s-queue-%d", name, idx), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return threads[idx].getQueue().size();
                }
            });
            statsLogger.registerGauge(String.format("%s-completed-tasks-%d", name, idx), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return threads[idx].getCompletedTaskCount();
                }
            });
            statsLogger.registerGauge(String.format("%s-total-tasks-%d", name, idx), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return threads[idx].getTaskCount();
                }
            });
        }

        // Stats
        this.taskExecutionStats = statsLogger.scope(name).getOpStatsLogger("task_execution");
        this.taskPendingStats = statsLogger.scope(name).getOpStatsLogger("task_queued");
        this.traceTaskExecution = traceTaskExecution;
    }

    ScheduledExecutorService chooseThread() {
        // skip random # generation in this special case
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[rand.nextInt(threads.length)];

    }

    ScheduledExecutorService chooseThread(Object orderingKey) {
        // skip hashcode generation in this special case
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[MathUtils.signSafeMod(orderingKey.hashCode(), threads.length)];

    }

    private SafeRunnable timedRunnable(SafeRunnable r) {
        if (traceTaskExecution) {
            return new TimedRunnable(r);
        } else {
            return r;
        }
    }

    /**
     * schedules a one time action to execute
     */
    public void submit(SafeRunnable r) {
        chooseThread().submit(timedRunnable(r));
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key
     * @param orderingKey
     * @param r
     */
    public void submitOrdered(Object orderingKey, SafeRunnable r) {
        chooseThread(orderingKey).submit(timedRunnable(r));
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
        return chooseThread().schedule(command, delay, unit);
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
        return chooseThread(orderingKey).schedule(command, delay, unit);
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
        return chooseThread().scheduleAtFixedRate(command, initialDelay, period, unit);
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
        return chooseThread(orderingKey).scheduleAtFixedRate(command, initialDelay, period, unit);
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
        return chooseThread().scheduleWithFixedDelay(command, initialDelay, delay, unit);
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
        return chooseThread(orderingKey).scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    private long getThreadID(Object orderingKey) {
        // skip hashcode generation in this special case
        if (threadIds.length == 1) {
            return threadIds[0];
        }

        return threadIds[MathUtils.signSafeMod(orderingKey.hashCode(), threadIds.length)];
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
        private final Logger LOG = LoggerFactory.getLogger(OrderedSafeGenericCallback.class);

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
