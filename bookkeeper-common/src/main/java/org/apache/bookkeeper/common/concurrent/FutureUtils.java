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

package org.apache.bookkeeper.common.concurrent;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.stats.OpStatsListener;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * Future related utils.
 */
@Slf4j
public final class FutureUtils {

    private FutureUtils() {}

    private static final Function<Throwable, Exception> DEFAULT_EXCEPTION_HANDLER = cause -> {
        if (cause instanceof Exception) {
            return (Exception) cause;
        } else {
            return new Exception(cause);
        }
    };

    public static CompletableFuture<Void> Void() {
        return value(null);
    }

    public static <T> T result(CompletableFuture<T> future) throws Exception {
        return FutureUtils.result(future, DEFAULT_EXCEPTION_HANDLER);
    }

    public static <T> T result(CompletableFuture<T> future, long timeout, TimeUnit timeUnit) throws Exception {
        return FutureUtils.result(future, DEFAULT_EXCEPTION_HANDLER, timeout, timeUnit);
    }

    @SneakyThrows(InterruptedException.class)
    public static <T, ExceptionT extends Throwable> T result(
        CompletableFuture<T> future, Function<Throwable, ExceptionT> exceptionHandler) throws ExceptionT {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            ExceptionT cause = exceptionHandler.apply(e.getCause());
            if (null == cause) {
                return null;
            } else {
                throw cause;
            }
        }
    }

    @SneakyThrows(InterruptedException.class)
    public static <T, ExceptionT extends Throwable> T result(
        CompletableFuture<T> future,
        Function<Throwable, ExceptionT> exceptionHandler,
        long timeout,
        TimeUnit timeUnit) throws ExceptionT, TimeoutException {
        try {
            return future.get(timeout, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            ExceptionT cause = exceptionHandler.apply(e.getCause());
            if (null == cause) {
                return null;
            } else {
                throw cause;
            }
        }
    }

    public static <T> CompletableFuture<T> createFuture() {
        return new CompletableFuture<T>();
    }

    public static <T> CompletableFuture<T> value(T value) {
        return CompletableFuture.completedFuture(value);
    }

    public static <T> CompletableFuture<T> exception(Throwable cause) {
        CompletableFuture<T> future = FutureUtils.createFuture();
        future.completeExceptionally(cause);
        return future;
    }

    public static <T> void complete(CompletableFuture<T> result,
                                    T value) {
        if (null == result) {
            return;
        }
        result.complete(value);
    }

    public static <T> void completeExceptionally(CompletableFuture<T> result,
                                                 Throwable cause) {
        if (null == result) {
            return;
        }
        result.completeExceptionally(cause);
    }

    /**
     * Completing the {@code future} in the thread in the scheduler identified by
     * the {@code scheduleKey}.
     *
     * @param future      future to complete
     * @param action      action to execute when complete
     * @param scheduler   scheduler to execute the action.
     * @param scheduleKey key to choose the thread to execute the action
     * @param <T>
     * @return
     */
    public static <T> CompletableFuture<T> whenCompleteAsync(
        CompletableFuture<T> future,
        BiConsumer<? super T, ? super Throwable> action,
        OrderedScheduler scheduler,
        Object scheduleKey) {
        return future.whenCompleteAsync(action, scheduler.chooseThread(scheduleKey));
    }

    public static <T> CompletableFuture<List<T>> collect(List<CompletableFuture<T>> futureList) {
        CompletableFuture<Void> finalFuture =
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        return finalFuture.thenApply(result ->
            futureList
                .stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList()));
    }

    public static <T> void proxyTo(CompletableFuture<T> src,
                                   CompletableFuture<T> target) {
        src.whenComplete((value, cause) -> {
            if (null == cause) {
                target.complete(value);
            } else {
                target.completeExceptionally(cause);
            }
        });
    }

    //
    // Process futures
    //

    private static class ListFutureProcessor<T, R>
        implements FutureEventListener<R>, Runnable {

        private volatile boolean done = false;
        private final Iterator<T> itemsIter;
        private final Function<T, CompletableFuture<R>> processFunc;
        private final CompletableFuture<List<R>> promise;
        private final List<R> results;
        private final ExecutorService callbackExecutor;

        ListFutureProcessor(List<T> items,
                            Function<T, CompletableFuture<R>> processFunc,
                            ExecutorService callbackExecutor) {
            this.itemsIter = items.iterator();
            this.processFunc = processFunc;
            this.promise = new CompletableFuture<>();
            this.results = Lists.newArrayListWithExpectedSize(items.size());
            this.callbackExecutor = callbackExecutor;
        }

        @Override
        public void onSuccess(R value) {
            results.add(value);
            if (null == callbackExecutor) {
                run();
            } else {
                callbackExecutor.submit(this);
            }
        }

        @Override
        public void onFailure(final Throwable cause) {
            done = true;

            if (null == callbackExecutor) {
                promise.completeExceptionally(cause);
            } else {
                callbackExecutor.submit((Runnable) () -> promise.completeExceptionally(cause));
            }
        }

        @Override
        public void run() {
            if (done) {
                log.debug("ListFutureProcessor is interrupted.");
                return;
            }
            if (!itemsIter.hasNext()) {
                promise.complete(results);
                done = true;
                return;
            }
            processFunc.apply(itemsIter.next()).whenComplete(this);
        }
    }

    /**
     * Process the list of items one by one using the process function <i>processFunc</i>.
     * The process will be stopped immediately if it fails on processing any one.
     *
     * @param collection       list of items
     * @param processFunc      process function
     * @param callbackExecutor executor to process the item
     * @return future presents the list of processed results
     */
    public static <T, R> CompletableFuture<List<R>> processList(List<T> collection,
                                                                Function<T, CompletableFuture<R>> processFunc,
                                                                @Nullable ExecutorService callbackExecutor) {
        ListFutureProcessor<T, R> processor =
            new ListFutureProcessor<T, R>(collection, processFunc, callbackExecutor);
        if (null != callbackExecutor) {
            callbackExecutor.submit(processor);
        } else {
            processor.run();
        }
        return processor.promise;
    }

    /**
     * Raise an exception to the <i>promise</i> within a given <i>timeout</i> period.
     * If the promise has been satisfied before raising, it won't change the state of the promise.
     *
     * @param promise   promise to raise exception
     * @param timeout   timeout period
     * @param unit      timeout period unit
     * @param cause     cause to raise
     * @param scheduler scheduler to execute raising exception
     * @param key       the submit key used by the scheduler
     * @return the promise applied with the raise logic
     */
    public static <T> CompletableFuture<T> within(final CompletableFuture<T> promise,
                                                  final long timeout,
                                                  final TimeUnit unit,
                                                  final Throwable cause,
                                                  final OrderedScheduler scheduler,
                                                  final Object key) {
        if (timeout < 0 || promise.isDone()) {
            return promise;
        }
        // schedule a timeout to raise timeout exception
        final java.util.concurrent.ScheduledFuture<?> task = scheduler.scheduleOrdered(key, () -> {
            if (!promise.isDone() && promise.completeExceptionally(cause)) {
                log.info("Raise exception", cause);
            }
        }, timeout, unit);
        // when the promise is satisfied, cancel the timeout task
        promise.whenComplete((value, throwable) -> {
                if (!task.cancel(true)) {
                    log.debug("Failed to cancel the timeout task");
                }
            }
        );
        return promise;
    }

    /**
     * Ignore exception from the <i>future</i>.
     *
     * @param future the original future
     * @return a transformed future ignores exceptions
     */
    public static <T> CompletableFuture<Void> ignore(CompletableFuture<T> future) {
        return ignore(future, null);
    }

    /**
     * Ignore exception from the <i>future</i> and log <i>errorMsg</i> on exceptions.
     *
     * @param future   the original future
     * @param errorMsg the error message to log on exceptions
     * @return a transformed future ignores exceptions
     */
    public static <T> CompletableFuture<Void> ignore(CompletableFuture<T> future,
                                                     final String errorMsg) {
        final CompletableFuture<Void> promise = new CompletableFuture<Void>();
        future.whenComplete(new FutureEventListener<T>() {
            @Override
            public void onSuccess(T value) {
                promise.complete(null);
            }

            @Override
            public void onFailure(Throwable cause) {
                if (null != errorMsg) {
                    log.error(errorMsg, cause);
                }
                promise.complete(null);
            }
        });
        return promise;
    }

    public static <T> CompletableFuture<T> ensure(CompletableFuture<T> future,
                                                  Runnable ensureBlock) {
        return future.whenComplete((value, cause) -> {
            ensureBlock.run();
        });
    }

    public static <T> CompletableFuture<T> rescue(CompletableFuture<T> future,
                                                  Function<Throwable, CompletableFuture<T>> rescueFuc) {
        CompletableFuture<T> result = FutureUtils.createFuture();
        future.whenComplete((value, cause) -> {
            if (null == cause) {
                result.complete(value);
                return;
            }
            proxyTo(rescueFuc.apply(cause), result);
        });
        return result;
    }

    /**
      * Add a event listener over <i>result</i> for collecting the operation stats.
      *
      * @param result result to listen on
      * @param opStatsLogger stats logger to record operations stats
      * @param stopwatch stop watch to time operation
      * @param <T>
      * @return result after registered the event listener
      */
    public static <T> CompletableFuture<T> stats(CompletableFuture<T> result,
                                                 OpStatsLogger opStatsLogger,
                                                 Stopwatch stopwatch) {
        return result.whenComplete(new OpStatsListener<T>(opStatsLogger, stopwatch));
    }

}
