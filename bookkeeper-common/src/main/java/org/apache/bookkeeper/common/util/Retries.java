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

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * A util class for supporting retries with customized backoff.
 */
public final class Retries {

    private Retries() {
    }

    public static final Predicate<Throwable> NonFatalPredicate =
        cause -> !(cause instanceof RuntimeException);

    /**
     * Retry a given {@code task} on failures.
     *
     * <p>It is a shortcut of {@link #run(Stream, Predicate, Supplier, OrderedScheduler, Object)}
     * that runs retries on any threads in the provided {@code scheduler}.
     *
     * @param backoffs       a stream of backoff delays, in milliseconds.
     * @param retryPredicate a predicate to test if failures are retryable.
     * @param task           a task to execute.
     * @param scheduler      scheduler to schedule the task and complete the futures.
     * @param <ReturnT>      the return type
     * @return future represents the result of the task with retries.
     */
    public static <ReturnT> CompletableFuture<ReturnT> run(
        Stream<Long> backoffs,
        Predicate<Throwable> retryPredicate,
        Supplier<CompletableFuture<ReturnT>> task,
        OrderedScheduler scheduler) {
        return run(backoffs, retryPredicate, task, scheduler, null);
    }

    /**
     * Retry a given {@code task} on failures.
     *
     * <p>It will only retry the tasks when the predicate {@code retryPredicate} tests
     * it as a retryable failure and it doesn't exhaust the retry budget. The retry delays
     * are defined in a stream of delay values (in milliseconds).
     *
     * <p>If a schedule {@code key} is provided, the {@code task} will be submitted to the
     * scheduler using the provided schedule {@code key} and also the returned future
     * will be completed in the same thread. Otherwise, the task and the returned future will
     * be executed and scheduled on any threads in the scheduler.
     *
     * @param backoffs       a stream of backoff delays, in milliseconds.
     * @param retryPredicate a predicate to test if failures are retryable.
     * @param task           a task to execute.
     * @param scheduler      scheduler to schedule the task and complete the futures.
     * @param key            the submit key for the scheduler.
     * @param <ReturnT>      the return tye.
     * @return future represents the result of the task with retries.
     */
    public static <ReturnT> CompletableFuture<ReturnT> run(
        Stream<Long> backoffs,
        Predicate<Throwable> retryPredicate,
        Supplier<CompletableFuture<ReturnT>> task,
        OrderedScheduler scheduler,
        Object key) {
        CompletableFuture<ReturnT> future = FutureUtils.createFuture();
        if (null == key) {
            execute(
                future,
                backoffs.iterator(),
                retryPredicate,
                task,
                scheduler,
                null);
        } else {
            scheduler.executeOrdered(key, () -> {
                execute(
                    future,
                    backoffs.iterator(),
                    retryPredicate,
                    task,
                    scheduler,
                    key);
            });
        }
        return future;
    }

    private static <ReturnT> void execute(
        CompletableFuture<ReturnT> finalResult,
        Iterator<Long> backoffIter,
        Predicate<Throwable> retryPredicate,
        Supplier<CompletableFuture<ReturnT>> task,
        OrderedScheduler scheduler,
        Object key) {

        FutureUtils.whenCompleteAsync(task.get(), (result, cause) -> {
            if (null == cause) {
                finalResult.complete(result);
                return;
            }
            if (retryPredicate.test(cause)) {
                if (!backoffIter.hasNext()) {
                    // exhausts all the retry budgets, fail the task now
                    finalResult.completeExceptionally(cause);
                    return;
                }
                long nextRetryDelayMs = backoffIter.next();
                scheduler.scheduleOrdered(key, () -> execute(
                    finalResult,
                    backoffIter,
                    retryPredicate,
                    task,
                    scheduler,
                    key), nextRetryDelayMs, TimeUnit.MILLISECONDS);
            } else {
                // the exception can not be retried
                finalResult.completeExceptionally(cause);
            }
        }, scheduler, key);
    }

}
