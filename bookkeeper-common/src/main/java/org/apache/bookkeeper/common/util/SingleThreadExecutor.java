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

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.collections.GrowableMpScArrayConsumerBlockingQueue;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Implements a single thread executor that drains the queue in batches to minimize contention between threads.
 *
 * <p>Tasks are executed in a safe manner: if there are exceptions they are logged and the executor will
 * proceed with the next tasks.
 */
@Slf4j
public class SingleThreadExecutor extends AbstractExecutorService implements ExecutorService, Runnable {
    private final BlockingQueue<Runnable> queue;
    private final Thread runner;

    private final boolean rejectExecution;

    private final LongAdder tasksCount = new LongAdder();
    private final LongAdder tasksCompleted = new LongAdder();
    private final LongAdder tasksRejected = new LongAdder();
    private final LongAdder tasksFailed = new LongAdder();

    private final int maxQueueCapacity;
    private static final AtomicIntegerFieldUpdater<SingleThreadExecutor> pendingTaskCountUpdater =
            AtomicIntegerFieldUpdater.newUpdater(SingleThreadExecutor.class, "pendingTaskCount");
    private volatile int pendingTaskCount = 0;

    enum State {
        Running,
        Shutdown,
        Terminated
    }

    private volatile State state;

    private final CountDownLatch startLatch;

    public SingleThreadExecutor(ThreadFactory tf) {
        this(tf, 0, false);
    }

    @SneakyThrows
    @SuppressFBWarnings(value = {"SC_START_IN_CTOR"})
    public SingleThreadExecutor(ThreadFactory tf, int maxQueueCapacity, boolean rejectExecution) {
        if (rejectExecution && maxQueueCapacity == 0) {
            throw new IllegalArgumentException("Executor cannot reject new items if the queue is unbound");
        }

        if (maxQueueCapacity > 0) {
            this.queue = new ArrayBlockingQueue<>(maxQueueCapacity);
        } else {
            this.queue = new GrowableMpScArrayConsumerBlockingQueue<>();
        }
        this.maxQueueCapacity = maxQueueCapacity;

        this.runner = tf.newThread(this);
        this.state = State.Running;
        this.rejectExecution = rejectExecution;
        this.startLatch = new CountDownLatch(1);
        this.runner.start();

        // Ensure the runner is already fully working by the time the constructor is done
        this.startLatch.await();
    }

    public void run() {
        try {
            boolean isInitialized = false;
            List<Runnable> localTasks = new ArrayList<>();

            while (state == State.Running) {
                if (!isInitialized) {
                    startLatch.countDown();
                    isInitialized = true;
                }

                int n = queue.drainTo(localTasks);
                if (n > 0) {
                    for (int i = 0; i < n; i++) {
                        if (!safeRunTask(localTasks.get(i))) {
                            return;
                        }
                    }
                    localTasks.clear();
                } else {
                    if (!safeRunTask(queue.take())) {
                        return;
                    }
                }
            }

            // Clear the queue in orderly shutdown
            int n = queue.drainTo(localTasks);
            for (int i = 0; i < n; i++) {
                safeRunTask(localTasks.get(i));
            }
        } catch (InterruptedException ie) {
            // Exit loop when interrupted
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            log.error("Exception in executor: {}", t.getMessage(), t);
            throw t;
        } finally {
            state = State.Terminated;
        }
    }

    private boolean safeRunTask(Runnable r) {
        try {
            r.run();
            tasksCompleted.increment();
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                return false;
            } else {
                tasksFailed.increment();
                log.error("Error while running task: {}", t.getMessage(), t);
            }
        } finally {
            decrementPendingTaskCount(1);
        }

        return true;
    }

    @Override
    public void shutdown() {
        state = State.Shutdown;
        if (queue.isEmpty()) {
            runner.interrupt();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        this.state = State.Shutdown;
        this.runner.interrupt();
        List<Runnable> remainingTasks = new ArrayList<>();
        int n = queue.drainTo(remainingTasks);
        decrementPendingTaskCount(n);
        return remainingTasks;
    }

    @Override
    public boolean isShutdown() {
        return state != State.Running;
    }

    @Override
    public boolean isTerminated() {
        return state == State.Terminated;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        runner.join(unit.toMillis(timeout));
        return runner.isAlive();
    }

    public long getQueuedTasksCount() {
        return Math.max(0, getSubmittedTasksCount() - getCompletedTasksCount());
    }

    public long getSubmittedTasksCount() {
        return tasksCount.sum();
    }

    public long getCompletedTasksCount() {
        return tasksCompleted.sum();
    }

    public long getRejectedTasksCount() {
        return tasksRejected.sum();
    }

    public long getFailedTasksCount() {
        return tasksFailed.sum();
    }

    @Override
    public void execute(Runnable r) {
        executeRunnableOrList(r, null);
    }

    @VisibleForTesting
    void executeRunnableOrList(Runnable runnable, List<Runnable> runnableList) {
        if (state != State.Running) {
            throw new RejectedExecutionException("Executor is shutting down");
        }

        boolean hasSingle = runnable != null;
        boolean hasList = runnableList != null && !runnableList.isEmpty();

        if (hasSingle == hasList) {
            // Both are provided or both are missing
            throw new IllegalArgumentException("Provide either 'runnable' or a non-empty 'runnableList', not both.");
        }

        try {
            if (!rejectExecution) {
                if (hasSingle) {
                    queue.put(runnable);
                    tasksCount.increment();
                } else {
                    for (Runnable task : runnableList) {
                        queue.put(task);
                        tasksCount.increment();
                    }
                }
            } else {
                int count = runnable != null ? 1 : runnableList.size();
                incrementPendingTaskCount(count);
                boolean success = hasSingle
                        ? queue.offer(runnable)
                        : queue.addAll(runnableList);
                if (success) {
                    tasksCount.add(count);
                } else {
                    decrementPendingTaskCount(count);
                    reject();
                }
            }
        } catch (InterruptedException e) {
            throw new RejectedExecutionException("Executor thread was interrupted", e);
        }
    }

    private void incrementPendingTaskCount(int count) {
        if (maxQueueCapacity <= 0) {
            return; // Unlimited capacity
        }

        if (count < 0) {
            throw new IllegalArgumentException("Count must be non-negative");
        }

        int oldPendingTaskCount = pendingTaskCountUpdater.getAndAccumulate(this, count,
                (curr, inc) -> (curr + inc > maxQueueCapacity) ? curr : curr + inc);

        if (oldPendingTaskCount + count > maxQueueCapacity) {
            reject();
        }
    }

    private void decrementPendingTaskCount(int count) {
        if (maxQueueCapacity <= 0) {
            return; // Unlimited capacity
        }

        if (count < 0) {
            throw new IllegalArgumentException("Count must be non-negative");
        }

        int currentPendingCount = pendingTaskCountUpdater.addAndGet(this, -count);
        if (log.isDebugEnabled()) {
            log.debug("Released {} task(s), current pending count: {}", count, currentPendingCount);
        }
    }

    private void reject() {
        tasksRejected.increment();
        throw new ExecutorRejectedException("Executor queue is full");
    }

    public void registerMetrics(StatsLogger statsLogger) {
        // Register gauges
        statsLogger.scopeLabel("thread", runner.getName())
                .registerGauge("thread_executor_queue", new Gauge<Number>() {
                    @Override
                    public Number getDefaultValue() {
                        return 0;
                    }

                    @Override
                    public Number getSample() {
                        return getQueuedTasksCount();
                    }
                });
        statsLogger.scopeLabel("thread", runner.getName())
                .registerGauge("thread_executor_completed", new Gauge<Number>() {
                    @Override
                    public Number getDefaultValue() {
                        return 0;
                    }

                    @Override
                    public Number getSample() {
                        return getCompletedTasksCount();
                    }
                });
        statsLogger.scopeLabel("thread", runner.getName())
                .registerGauge("thread_executor_tasks_completed", new Gauge<Number>() {
                    @Override
                    public Number getDefaultValue() {
                        return 0;
                    }

                    @Override
                    public Number getSample() {
                        return getCompletedTasksCount();
                    }
                });
        statsLogger.scopeLabel("thread", runner.getName())
                .registerGauge("thread_executor_tasks_rejected", new Gauge<Number>() {
                    @Override
                    public Number getDefaultValue() {
                        return 0;
                    }

                    @Override
                    public Number getSample() {
                        return getRejectedTasksCount();
                    }
                });
        statsLogger.scopeLabel("thread", runner.getName())
                .registerGauge("thread_executor_tasks_failed", new Gauge<Number>() {
                    @Override
                    public Number getDefaultValue() {
                        return 0;
                    }

                    @Override
                    public Number getSample() {
                        return getFailedTasksCount();
                    }
                });
    }

    @VisibleForTesting
    int getPendingTaskCount() {
        return pendingTaskCountUpdater.get(this);
    }

    private static class ExecutorRejectedException extends RejectedExecutionException {

        private ExecutorRejectedException(String msg) {
            super(msg);
        }
        @Override
        public Throwable fillInStackTrace() {
            // Avoid the stack traces to be generated for this exception. This is done
            // because when rejectExecution=true, there could be many such exceptions
            // getting thrown, and filling the stack traces is very expensive
            return this;
        }
    }
}
