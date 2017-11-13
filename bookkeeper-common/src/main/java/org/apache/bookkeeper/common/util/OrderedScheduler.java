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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang.StringUtils;

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
public class OrderedScheduler {
    public static final int NO_TASK_LIMIT = -1;
    protected static final long WARN_TIME_MICRO_SEC_DEFAULT = TimeUnit.SECONDS.toMicros(1);

    final String name;
    final ListeningScheduledExecutorService threads[];
    final long threadIds[];
    final Random rand = new Random();
    final OpStatsLogger taskExecutionStats;
    final OpStatsLogger taskPendingStats;
    final boolean traceTaskExecution;
    final long warnTimeMicroSec;
    final int maxTasksInQueue;

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
    public static class SchedulerBuilder extends AbstractBuilder<OrderedScheduler> {}

    /**
     * Abstract builder class to build {@link OrderedScheduler}.
     */
    public abstract static class AbstractBuilder<T extends OrderedScheduler> {
        protected String name = getClass().getSimpleName();
        protected int numThreads = Runtime.getRuntime().availableProcessors();
        protected ThreadFactory threadFactory = null;
        protected StatsLogger statsLogger = NullStatsLogger.INSTANCE;
        protected boolean traceTaskExecution = false;
        protected long warnTimeMicroSec = WARN_TIME_MICRO_SEC_DEFAULT;
        protected int maxTasksInQueue = NO_TASK_LIMIT;

        public AbstractBuilder<T> name(String name) {
            this.name = name;
            return this;
        }

        public AbstractBuilder<T> numThreads(int num) {
            this.numThreads = num;
            return this;
        }

        public AbstractBuilder<T> maxTasksInQueue(int num) {
            this.maxTasksInQueue = num;
            return this;
        }

        public AbstractBuilder<T> threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public AbstractBuilder<T> statsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        public AbstractBuilder<T> traceTaskExecution(boolean enabled) {
            this.traceTaskExecution = enabled;
            return this;
        }

        public AbstractBuilder<T> traceTaskWarnTimeMicroSec(long warnTimeMicroSec) {
            this.warnTimeMicroSec = warnTimeMicroSec;
            return this;
        }

        @SuppressWarnings("unchecked")
        public T build() {
            if (null == threadFactory) {
                threadFactory = Executors.defaultThreadFactory();
            }
            return (T) new OrderedScheduler(
                name,
                numThreads,
                threadFactory,
                statsLogger,
                traceTaskExecution,
                warnTimeMicroSec,
                maxTasksInQueue);
        }

    }

    private class TimedRunnable implements SafeRunnable {
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
                LOGGER.warn("Runnable {}:{} took too long {} micros to execute.",
                            new Object[] { runnable, runnable.getClass(), elapsedMicroSec });
            }
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
     * @param warnTimeMicroSec
     *            - log long task exec warning after this interval
     */
    protected OrderedScheduler(String baseName,
                               int numThreads,
                               ThreadFactory threadFactory,
                               StatsLogger statsLogger,
                               boolean traceTaskExecution,
                               long warnTimeMicroSec,
                               int maxTasksInQueue) {
        checkArgument(numThreads > 0);
        checkArgument(!StringUtils.isBlank(baseName));

        this.maxTasksInQueue = maxTasksInQueue;
        this.warnTimeMicroSec = warnTimeMicroSec;
        name = baseName;
        threads = new ListeningScheduledExecutorService[numThreads];
        threadIds = new long[numThreads];
        for (int i = 0; i < numThreads; i++) {
            final ScheduledThreadPoolExecutor thread = new ScheduledThreadPoolExecutor(1,
                    new ThreadFactoryBuilder()
                        .setNameFormat(name + "-" + getClass().getSimpleName() + "-" + i + "-%d")
                        .setThreadFactory(threadFactory)
                        .build());
            threads[i] = new BoundedScheduledExecutorService(thread, this.maxTasksInQueue);

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
                    return thread.getQueue().size();
                }
            });
            statsLogger.registerGauge(String.format("%s-completed-tasks-%d", name, idx), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return thread.getCompletedTaskCount();
                }
            });
            statsLogger.registerGauge(String.format("%s-total-tasks-%d", name, idx), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return thread.getTaskCount();
                }
            });
        }

        // Stats
        this.taskExecutionStats = statsLogger.scope(name).getOpStatsLogger("task_execution");
        this.taskPendingStats = statsLogger.scope(name).getOpStatsLogger("task_queued");
        this.traceTaskExecution = traceTaskExecution;
    }

    public ListeningScheduledExecutorService chooseThread() {
        // skip random # generation in this special case
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[rand.nextInt(threads.length)];
    }

    public ListeningScheduledExecutorService chooseThread(Object orderingKey) {
        // skip hashcode generation in this special case
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[MathUtils.signSafeMod(orderingKey.hashCode(), threads.length)];
    }

    /**
     * skip hashcode generation in this special case.
     *
     * @param orderingKey long ordering key
     * @return the thread for executing this order key
     */
    public ListeningScheduledExecutorService chooseThread(long orderingKey) {
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[MathUtils.signSafeMod(orderingKey, threads.length)];
    }

    private SafeRunnable timedRunnable(SafeRunnable r) {
        if (traceTaskExecution) {
            return new TimedRunnable(r);
        } else {
            return r;
        }
    }

    /**
     * schedules a one time action to execute.
     */
    public void submit(SafeRunnable r) {
        chooseThread().submit(timedRunnable(r));
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key.
     *
     * @param orderingKey
     * @param r
     */
    public ListenableFuture<?> submitOrdered(Object orderingKey, SafeRunnable r) {
        return chooseThread(orderingKey).submit(timedRunnable(r));
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key.
     *
     * @param orderingKey
     * @param r
     */
    public void submitOrdered(long orderingKey, SafeRunnable r) {
        chooseThread(orderingKey).execute(r);
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key.
     *
     * @param orderingKey
     * @param r
     */
    public void submitOrdered(int orderingKey, SafeRunnable r) {
        chooseThread(orderingKey).execute(r);
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key.
     *
     * @param orderingKey
     * @param callable
     */
    public <T> ListenableFuture<T> submitOrdered(Object orderingKey,
                                                 java.util.concurrent.Callable<T> callable) {
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
        return chooseThread().schedule(command, delay, unit);
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
        return chooseThread().scheduleAtFixedRate(command, initialDelay, period, unit);
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
        return chooseThread().scheduleWithFixedDelay(command, initialDelay, delay, unit);
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

    protected long getThreadID(long orderingKey) {
        // skip hashcode generation in this special case
        if (threadIds.length == 1) {
            return threadIds[0];
        }

        return threadIds[MathUtils.signSafeMod(orderingKey, threadIds.length)];
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
     * Force threads shutdown (cancel active requests) after specified delay,
     * to be used after shutdown() rejects new requests.
     */
    public void forceShutdown(long timeout, TimeUnit unit) {
        for (int i = 0; i < threads.length; i++) {
            try {
                if (!threads[i].awaitTermination(timeout, unit)) {
                    threads[i].shutdownNow();
                }
            } catch (InterruptedException exception) {
                threads[i].shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

}
