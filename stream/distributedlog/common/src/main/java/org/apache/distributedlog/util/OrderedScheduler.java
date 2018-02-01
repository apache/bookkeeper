/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.util;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.distributedlog.common.concurrent.FutureUtils;
import org.apache.distributedlog.common.util.MathUtil;

/**
 * Ordered Scheduler. It is thread pool based {@link ScheduledExecutorService}, additionally providing
 * the ability to execute/schedule tasks by <code>key</code>. Hence the tasks submitted by same <i>key</i>
 * will be executed in order.
 *
 * <p>The scheduler is comprised of multiple {@link ScheduledExecutorService}s. Each
 * {@link ScheduledExecutorService} is a single thread executor. Normal task submissions will
 * be submitted to executors in a random manner to guarantee load balancing. Keyed task submissions (e.g
 * {@link OrderedScheduler#submit(Object, Runnable)} will be submitted to a dedicated executor based on
 * the hash value of submit <i>key</i>.
 */
public class OrderedScheduler implements ScheduledExecutorService {

    /**
     * Create a builder to build scheduler.
     *
     * @return scheduler builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link OrderedScheduler}.
     */
    public static class Builder {

        private String name = "OrderedScheduler";
        private int corePoolSize = -1;
        private ThreadFactory threadFactory = null;

        /**
         * Set the name of this scheduler. It would be used as part of stats scope and thread name.
         *
         * @param name name of the scheduler.
         * @return scheduler builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Set the number of threads to be used in this scheduler.
         *
         * @param corePoolSize the number of threads to keep in the pool, even
         *                     if they are idle
         * @return scheduler builder
         */
        public Builder corePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
            return this;
        }

        /**
         * Set the thread factory that the scheduler uses to create a new thread.
         *
         * @param threadFactory the factory to use when the executor
         *                      creates a new thread
         * @return scheduler builder
         */
        public Builder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * Build the ordered scheduler.
         *
         * @return ordered scheduler
         */
        public OrderedScheduler build() {
            if (corePoolSize <= 0) {
                corePoolSize = Runtime.getRuntime().availableProcessors();
            }
            if (null == threadFactory) {
                threadFactory = Executors.defaultThreadFactory();
            }

            return new OrderedScheduler(
                name,
                corePoolSize,
                threadFactory);
        }

    }

    protected final String name;
    protected final int corePoolSize;
    protected final ScheduledExecutorService[] executors;
    protected final Random random;

    private OrderedScheduler(String name,
                             int corePoolSize,
                             ThreadFactory threadFactory) {
        this.name = name;
        this.corePoolSize = corePoolSize;
        this.executors = new ScheduledExecutorService[corePoolSize];
        for (int i = 0; i < corePoolSize; i++) {
            ThreadFactory tf = new ThreadFactoryBuilder()
                .setNameFormat(name + "-scheduler-" + i + "-%d")
                .setThreadFactory(threadFactory)
                .build();
            executors[i] = Executors.newSingleThreadScheduledExecutor(tf);
        }
        this.random = new Random(System.currentTimeMillis());
    }

    protected ScheduledExecutorService chooseExecutor() {
        return corePoolSize == 1 ? executors[0] : executors[random.nextInt(corePoolSize)];
    }

    public ScheduledExecutorService chooseExecutor(Object key) {
        if (null == key) {
            return chooseExecutor();
        }
        return corePoolSize == 1 ? executors[0] :
            executors[MathUtil.signSafeMod(Objects.hashCode(key), corePoolSize)];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return chooseExecutor().schedule(command, delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return chooseExecutor().schedule(callable, delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                  long initialDelay, long period, TimeUnit unit) {
        return chooseExecutor().scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                     long initialDelay, long delay, TimeUnit unit) {
        return chooseExecutor().scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        for (ScheduledExecutorService executor : executors) {
            executor.shutdown();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> runnables = new ArrayList<Runnable>();
        for (ScheduledExecutorService executor : executors) {
            runnables.addAll(executor.shutdownNow());
        }
        return runnables;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isShutdown() {
        for (ScheduledExecutorService executor : executors) {
            if (!executor.isShutdown()) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTerminated() {
        for (ScheduledExecutorService executor : executors) {
            if (!executor.isTerminated()) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
        throws InterruptedException {
        for (ScheduledExecutorService executor : executors) {
            if (!executor.awaitTermination(timeout, unit)) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return chooseExecutor().submit(task);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return chooseExecutor().submit(task, result);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<?> submit(Runnable task) {
        return chooseExecutor().submit(task);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
        return chooseExecutor().invokeAll(tasks);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException {
        return chooseExecutor().invokeAll(tasks, timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
        return chooseExecutor().invokeAny(tasks);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return chooseExecutor().invokeAny(tasks, timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Runnable command) {
        chooseExecutor().execute(command);
    }

    // Ordered Functions

    public ScheduledFuture<?> schedule(Object key, Runnable command, long delay, TimeUnit unit) {
        return chooseExecutor(key).schedule(command, delay, unit);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(Object key,
                                                  Runnable command,
                                                  long initialDelay,
                                                  long period,
                                                  TimeUnit unit) {
        return chooseExecutor(key).scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public Future<?> submit(Object key, Runnable command) {
        return chooseExecutor(key).submit(command);
    }

    public <T> CompletableFuture<T> submit(Object key, Callable<T> callable) {
        CompletableFuture<T> future = FutureUtils.createFuture();
        chooseExecutor(key).submit(() -> {
            try {
                future.complete(callable.call());
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

}
