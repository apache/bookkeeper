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
package org.apache.bookkeeper.common.util;

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implements {@link ListeningScheduledExecutorService} and allows limiting the number
 * of tasks to be scheduled in the thread's queue.
 *
 */
public class BoundedScheduledExecutorService extends ForwardingListeningExecutorService
        implements ListeningScheduledExecutorService {
    BlockingQueue<Runnable> queue;
    ListeningScheduledExecutorService thread;
    int maxTasksInQueue;

    public BoundedScheduledExecutorService(ScheduledThreadPoolExecutor thread, int maxTasksInQueue) {
        this.queue = thread.getQueue();
        this.thread = MoreExecutors.listeningDecorator(thread);
        this.maxTasksInQueue = maxTasksInQueue;
    }

    @Override
    protected ListeningExecutorService delegate() {
        return this.thread;
    }

    @Override
    public ListenableScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        this.checkQueue();
        return this.thread.schedule(command, delay, unit);
    }

    @Override
    public <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        this.checkQueue();
        return this.thread.schedule(callable, delay, unit);
    }

    @Override
    public ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                            long initialDelay, long period, TimeUnit unit) {
        this.checkQueue();
        return this.thread.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                               long initialDelay, long delay, TimeUnit unit) {
        this.checkQueue();
        return this.thread.scheduleAtFixedRate(command, initialDelay, delay, unit);
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task) {
        this.checkQueue();
        return super.submit(task);
    }

    @Override
    public ListenableFuture<?> submit(Runnable task) {
        this.checkQueue();
        return super.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        this.checkQueue();
        return super.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                         long timeout, TimeUnit unit) throws InterruptedException {
        this.checkQueue();
        return super.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        this.checkQueue();
        return super.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
                           TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        this.checkQueue();
        return super.invokeAny(tasks, timeout, unit);
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result) {
        this.checkQueue();
        return super.submit(task, result);
    }

    @Override
    public void execute(Runnable command) {
        this.checkQueue();
        super.execute(command);
    }

    private void checkQueue() {
        if (this.maxTasksInQueue > 0 && this.queue.size() >= this.maxTasksInQueue) {
            throw new RejectedExecutionException("Queue at limit of " + this.maxTasksInQueue + " items");
        }
    }

}

