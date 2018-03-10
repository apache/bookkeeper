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
package org.apache.distributedlog.common.concurrent;

import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.common.util.Permit;

/**
 * An AsyncSemaphore is a traditional semaphore but with asynchronous
 * execution.
 *
 * <p>Grabbing a permit returns a `Future[Permit]`.
 *
 * <p>Basic usage:
 * {{{
 *   val semaphore = new AsyncSemaphore(n)
 *   ...
 *   semaphore.acquireAndRun() {
 *     somethingThatReturnsFutureT()
 *   }
 * }}}
 *
 * <p>Calls to acquire() and acquireAndRun are serialized, and tickets are
 * given out fairly (in order of arrival).
 */
public class AsyncSemaphore {

    private final Optional<Integer> maxWaiters;

    private final Permit semaphorePermit = new Permit() {
        @Override
        public void release() {
            releasePermit(this);
        }
    };

    @GuardedBy("this")
    private Optional<Throwable> closed = Optional.empty();
    @GuardedBy("this")
    private final LinkedList<CompletableFuture<Permit>> waitq;
    @GuardedBy("this")
    private int availablePermits;

    public AsyncSemaphore(int initialPermits,
                          Optional<Integer> maxWaiters) {
        this.availablePermits = initialPermits;
        this.waitq = new LinkedList<>();
        this.maxWaiters = maxWaiters;
    }

    private synchronized void releasePermit(Permit permit) {
        CompletableFuture<Permit> next = waitq.pollFirst();
        if (null != next) {
            next.complete(permit);
        } else {
            availablePermits += 1;
        }
    }

    private CompletableFuture<Permit> newFuturePermit() {
        return FutureUtils.value(semaphorePermit);
    }

    /**
     * Acquire a [[Permit]], asynchronously.
     *
     * <p>Be sure to `permit.release()` in a
     * - `finally` block of your `onSuccess` callback
     * - `ensure` block of your future chain
     *
     * <p>Interrupting this future is only advisory, and will not release the permit
     * if the future has already been satisfied.
     *
     * @note This method always return the same instance of [[Permit]].
     * @return a `Future[Permit]` when the `Future` is satisfied, computation can proceed,
     *         or a Future.Exception[RejectedExecutionException]` if the configured maximum
     *         number of waiters would be exceeded.
     */
    public synchronized CompletableFuture<Permit> acquire() {
        if (closed.isPresent()) {
            return FutureUtils.exception(closed.get());
        }

        if (availablePermits > 0) {
            availablePermits -= 1;
            return newFuturePermit();
        } else {
            if (maxWaiters.isPresent() && waitq.size() >= maxWaiters.get()) {
                return FutureUtils.exception(new RejectedExecutionException("Max waiters exceeded"));
            } else {
                CompletableFuture<Permit> future = FutureUtils.createFuture();
                future.whenComplete((value, cause) -> {
                    synchronized (AsyncSemaphore.this) {
                        waitq.remove(future);
                    }
                });
                waitq.addLast(future);
                return future;
            }
        }
    }

    /**
     * Fail the semaphore and stop it from distributing further permits. Subsequent
     * attempts to acquire a permit fail with `exc`. This semaphore's queued waiters
     * are also failed with `exc`.
     */
    public synchronized void fail(Throwable exc) {
        closed = Optional.of(exc);
        for (CompletableFuture<Permit> future : waitq) {
            future.cancel(true);
        }
        waitq.clear();
    }

    /**
     * Execute the function asynchronously when a permit becomes available.
     *
     * <p>If the function throws a non-fatal exception, the exception is returned as part of the Future.
     * For all exceptions, the permit would be released before returning.
     *
     * @return a Future[T] equivalent to the return value of the input function. If the configured
     *         maximum value of waitq is reached, Future.Exception[RejectedExecutionException] is
     *         returned.
     */
    public <T> CompletableFuture<T> acquireAndRun(Supplier<CompletableFuture<T>> func) {
        return acquire().thenCompose(permit -> {
            CompletableFuture<T> future;
            try {
                future = func.get();
                future.whenComplete((value, cause) -> permit.release());
                return future;
            } catch (Throwable cause) {
                permit.release();
                throw cause;
            }
        });
    }

}
