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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
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

    enum State {
        Running,
        Shutdown,
        Terminated
    }

    private volatile State state;

    public SingleThreadExecutor(ThreadFactory tf) {
        this(tf, 64 * 1024, false);
    }

    public SingleThreadExecutor(ThreadFactory tf, int maxQueueCapacity, boolean rejectExecution) {
        this.queue = new ArrayBlockingQueue<>(maxQueueCapacity);
        this.runner = tf.newThread(this);
        this.state = State.Running;
        this.rejectExecution = rejectExecution;
        this.runner.start();
    }

    public void run() {
        try {
            List<Runnable> localTasks = new ArrayList<>();

            while (state == State.Running) {
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
        queue.drainTo(remainingTasks);
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
        if (state != State.Running) {
            throw new RejectedExecutionException("Executor is shutting down");
        }

        try {
            if (!rejectExecution) {
                queue.put(r);
                tasksCount.increment();
            } else {
                if (queue.offer(r)) {
                    tasksCount.increment();
                } else {
                    tasksRejected.increment();
                    throw new RejectedExecutionException("Executor queue is full");
                }
            }
        } catch (InterruptedException e) {
            throw new RejectedExecutionException("Executor thread was interrupted", e);
        }
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
}
