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
package org.apache.distributedlog.lock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.common.concurrent.AsyncSemaphore;
import org.apache.distributedlog.exceptions.LockingException;
import org.apache.distributedlog.exceptions.OwnershipAcquireFailedException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed lock, using ZooKeeper.
 * <p/>
 * The lock is vulnerable to timing issues. For example, the process could
 * encounter a really long GC cycle between acquiring the lock, and writing to
 * a ledger. This could have timed out the lock, and another process could have
 * acquired the lock and started writing to bookkeeper. Therefore other
 * mechanisms are required to ensure correctness (i.e. Fencing).
 * <p/>
 * The lock is only allowed to acquire once. If the lock is acquired successfully,
 * the caller holds the ownership until it loses the ownership either because of
 * others already acquired the lock when session expired or explicitly close it.
 *
 *  <p>The caller could use {@link #checkOwnership()} or {@link #checkOwnershipAndReacquire()}
 * to check if it still holds the lock. If it doesn't hold the lock, the caller should
 * give up the ownership and close the lock.
 * <h3>Metrics</h3>
 * All the lock related stats are exposed under `lock`.
 * <ul>
 * <li>lock/acquire: opstats. latency spent on acquiring a lock.
 * <li>lock/reacquire: opstats. latency spent on re-acquiring a lock.
 * <li>lock/internalTryRetries: counter. the number of retries on re-creating internal locks.
 * </ul>
 * Other internal lock related stats are also exposed under `lock`. See {@link SessionLock}
 * for details.
 */
public class ZKDistributedLock implements LockListener, DistributedLock {

    static final Logger LOG = LoggerFactory.getLogger(ZKDistributedLock.class);

    private final SessionLockFactory lockFactory;
    private final OrderedScheduler lockStateExecutor;
    private final String lockPath;
    private final long lockTimeout;
    private final DistributedLockContext lockContext = new DistributedLockContext();

    private final AsyncSemaphore lockSemaphore = new AsyncSemaphore(1, Optional.empty());
    // We have two lock acquire futures:
    // 1. lock acquire future: for the initial acquire op
    // 2. lock reacquire future: for reacquire necessary when session expires, lock is closed
    private CompletableFuture<ZKDistributedLock> lockAcquireFuture = null;
    private CompletableFuture<ZKDistributedLock> lockReacquireFuture = null;
    // following variable tracking the status of acquire process
    //   => create (internalLock) => tryLock (tryLockFuture) => waitForAcquire (lockWaiter)
    private SessionLock internalLock = null;
    private CompletableFuture<LockWaiter> tryLockFuture = null;
    private LockWaiter lockWaiter = null;
    // exception indicating if the reacquire failed
    private LockingException lockReacquireException = null;
    // closeFuture
    private volatile boolean closed = false;
    private CompletableFuture<Void> closeFuture = null;

    // A counter to track how many re-acquires happened during a lock's life cycle.
    private static final AtomicIntegerFieldUpdater<ZKDistributedLock> reacquireCountUpdater =
        AtomicIntegerFieldUpdater.newUpdater(ZKDistributedLock.class, "reacquireCount");
    private volatile int reacquireCount = 0;
    private final StatsLogger lockStatsLogger;
    private final OpStatsLogger acquireStats;
    private final OpStatsLogger reacquireStats;
    private final Counter internalTryRetries;

    public ZKDistributedLock(
            OrderedScheduler lockStateExecutor,
            SessionLockFactory lockFactory,
            String lockPath,
            long lockTimeout,
            StatsLogger statsLogger) {
        this.lockStateExecutor = lockStateExecutor;
        this.lockPath = lockPath;
        this.lockTimeout = lockTimeout;
        this.lockFactory = lockFactory;

        lockStatsLogger = statsLogger.scope("lock");
        acquireStats = lockStatsLogger.getOpStatsLogger("acquire");
        reacquireStats = lockStatsLogger.getOpStatsLogger("reacquire");
        internalTryRetries = lockStatsLogger.getCounter("internalTryRetries");
    }

    private LockClosedException newLockClosedException() {
        return new LockClosedException(lockPath, "Lock is already closed");
    }

    private synchronized void checkLockState() throws LockingException {
        if (closed) {
            throw newLockClosedException();
        }
        if (null != lockReacquireException) {
            throw lockReacquireException;
        }
    }

    /**
     * Asynchronously acquire the lock. Technically the try phase of this operation--which adds us to the waiter
     * list--is executed synchronously, but the lock wait itself doesn't block.
     */
    @Override
    public synchronized CompletableFuture<ZKDistributedLock> asyncAcquire() {
        if (null != lockAcquireFuture) {
            return FutureUtils.exception(
                    new UnexpectedException("Someone is already acquiring/acquired lock " + lockPath));
        }
        final CompletableFuture<ZKDistributedLock> promise = FutureUtils.createFuture();
        promise.whenComplete((zkDistributedLock, throwable) -> {
            if (null == throwable || !(throwable instanceof CancellationException)) {
                return;
            }
            lockStateExecutor.executeOrdered(lockPath, () -> asyncClose());
        });
        final Stopwatch stopwatch = Stopwatch.createStarted();
        promise.whenComplete(new FutureEventListener<ZKDistributedLock>() {
            @Override
            public void onSuccess(ZKDistributedLock lock) {
                acquireStats.registerSuccessfulEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS),
                    TimeUnit.MICROSECONDS);
            }
            @Override
            public void onFailure(Throwable cause) {
                acquireStats.registerFailedEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS),
                    TimeUnit.MICROSECONDS);
                // release the lock if fail to acquire
                asyncClose();
            }
        });
        this.lockAcquireFuture = promise;
        lockStateExecutor.executeOrdered(
            lockPath, () -> doAsyncAcquireWithSemaphore(promise, lockTimeout));
        return promise;
    }

    void doAsyncAcquireWithSemaphore(final CompletableFuture<ZKDistributedLock> acquirePromise,
                                     final long lockTimeout) {
        lockSemaphore.acquireAndRun(() -> {
            doAsyncAcquire(acquirePromise, lockTimeout);
            return acquirePromise;
        });
    }

    void doAsyncAcquire(final CompletableFuture<ZKDistributedLock> acquirePromise,
                        final long lockTimeout) {
        LOG.trace("Async Lock Acquire {}", lockPath);
        try {
            checkLockState();
        } catch (IOException ioe) {
            FutureUtils.completeExceptionally(acquirePromise, ioe);
            return;
        }

        if (haveLock()) {
            // it already hold the lock
            FutureUtils.complete(acquirePromise, this);
            return;
        }

        lockFactory
            .createLock(lockPath, lockContext)
            .whenCompleteAsync(new FutureEventListener<SessionLock>() {
            @Override
            public void onSuccess(SessionLock lock) {
                synchronized (ZKDistributedLock.this) {
                    if (closed) {
                        LOG.info("Skipping tryLocking lock {} since it is already closed", lockPath);
                        FutureUtils.completeExceptionally(acquirePromise, newLockClosedException());
                        return;
                    }
                }
                synchronized (ZKDistributedLock.this) {
                    internalLock = lock;
                    internalLock.setLockListener(ZKDistributedLock.this);
                }
                asyncTryLock(lock, acquirePromise, lockTimeout);
            }

            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.completeExceptionally(acquirePromise, cause);
            }
        }, lockStateExecutor.chooseThread(lockPath));
    }

    void asyncTryLock(SessionLock lock,
                      final CompletableFuture<ZKDistributedLock> acquirePromise,
                      final long lockTimeout) {
        if (null != tryLockFuture) {
            tryLockFuture.cancel(true);
        }
        tryLockFuture = lock.asyncTryLock(lockTimeout, TimeUnit.MILLISECONDS);
        tryLockFuture.whenCompleteAsync(
            new FutureEventListener<LockWaiter>() {
                @Override
                public void onSuccess(LockWaiter waiter) {
                    synchronized (ZKDistributedLock.this) {
                        if (closed) {
                            LOG.info("Skipping acquiring lock {} since it is already closed", lockPath);
                            waiter
                                .getAcquireFuture()
                                .completeExceptionally(new LockingException(lockPath, "lock is already closed."));
                            FutureUtils.completeExceptionally(acquirePromise, newLockClosedException());
                            return;
                        }
                    }
                    tryLockFuture = null;
                    lockWaiter = waiter;
                    waitForAcquire(waiter, acquirePromise);
                }

                @Override
                public void onFailure(Throwable cause) {
                    FutureUtils.completeExceptionally(acquirePromise, cause);
                }
            }, lockStateExecutor.chooseThread(lockPath));
    }

    void waitForAcquire(final LockWaiter waiter,
                        final CompletableFuture<ZKDistributedLock> acquirePromise) {
        waiter.getAcquireFuture().whenCompleteAsync(
            new FutureEventListener<Boolean>() {
                @Override
                public void onSuccess(Boolean acquired) {
                    LOG.info("{} acquired lock {}", waiter, lockPath);
                    if (acquired) {
                        FutureUtils.complete(acquirePromise, ZKDistributedLock.this);
                    } else {
                        FutureUtils.completeExceptionally(acquirePromise,
                                new OwnershipAcquireFailedException(lockPath, waiter.getCurrentOwner()));
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    FutureUtils.completeExceptionally(acquirePromise, cause);
                }
            }, lockStateExecutor.chooseThread(lockPath));
    }

    /**
     * NOTE: The {@link LockListener#onExpired()} is already executed in lock executor.
     */
    @Override
    public void onExpired() {
        try {
            reacquireLock(false);
        } catch (LockingException le) {
            // should not happen
            LOG.error("Locking exception on re-acquiring lock {} : ", lockPath, le);
        }
    }

    /**
     * Check if hold lock, if it doesn't, then re-acquire the lock.
     *
     * @throws LockingException     if the lock attempt fails
     */
    @Override
    public synchronized void checkOwnershipAndReacquire() throws LockingException {
        if (null == lockAcquireFuture || !lockAcquireFuture.isDone()) {
            throw new LockingException(lockPath, "check ownership before acquiring");
        }

        if (haveLock()) {
            return;
        }

        // We may have just lost the lock because of a ZK session timeout
        // not necessarily because someone else acquired the lock.
        // In such cases just try to reacquire. If that fails, it will throw
        reacquireLock(true);
    }

    /**
     * Check if lock is held.
     * If not, error out and do not reacquire. Use this in cases where there are many waiters by default
     * and reacquire is unlikley to succeed.
     *
     * @throws LockingException     if the lock attempt fails
     */
    @Override
    public synchronized void checkOwnership() throws LockingException {
        if (null == lockAcquireFuture || !lockAcquireFuture.isDone()) {
            throw new LockingException(lockPath, "check ownership before acquiring");
        }
        if (!haveLock()) {
            throw new LockingException(lockPath, "Lost lock ownership");
        }
    }

    @VisibleForTesting
    int getReacquireCount() {
        return reacquireCountUpdater.get(this);
    }

    @VisibleForTesting
    synchronized CompletableFuture<ZKDistributedLock> getLockReacquireFuture() {
        return lockReacquireFuture;
    }

    @VisibleForTesting
    synchronized CompletableFuture<ZKDistributedLock> getLockAcquireFuture() {
        return lockAcquireFuture;
    }

    @VisibleForTesting
    synchronized SessionLock getInternalLock() {
        return internalLock;
    }

    @VisibleForTesting
    LockWaiter getLockWaiter() {
        return lockWaiter;
    }

    synchronized boolean haveLock() {
        return !closed && internalLock != null && internalLock.isLockHeld();
    }

    void closeWaiter(final LockWaiter waiter,
                     final CompletableFuture<Void> closePromise) {
        if (null == waiter) {
            interruptTryLock(tryLockFuture, closePromise);
        } else {
            waiter.getAcquireFuture().whenCompleteAsync(
                new FutureEventListener<Boolean>() {
                    @Override
                    public void onSuccess(Boolean value) {
                        unlockInternalLock(closePromise);
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        unlockInternalLock(closePromise);
                    }
                }, lockStateExecutor.chooseThread(lockPath));
            waiter.getAcquireFuture().cancel(true);
        }
    }

    void interruptTryLock(final CompletableFuture<LockWaiter> tryLockFuture,
                          final CompletableFuture<Void> closePromise) {
        if (null == tryLockFuture) {
            unlockInternalLock(closePromise);
        } else {
            tryLockFuture.whenCompleteAsync(
                new FutureEventListener<LockWaiter>() {
                    @Override
                    public void onSuccess(LockWaiter waiter) {
                        closeWaiter(waiter, closePromise);
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        unlockInternalLock(closePromise);
                    }
                }, lockStateExecutor.chooseThread(lockPath));
            tryLockFuture.cancel(true);
        }
    }

    synchronized void unlockInternalLock(final CompletableFuture<Void> closePromise) {
        if (internalLock == null) {
            FutureUtils.complete(closePromise, null);
        } else {
            internalLock.asyncUnlock().whenComplete((value, cause) -> closePromise.complete(null));
        }
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        final CompletableFuture<Void> closePromise;
        synchronized (this) {
            if (closed) {
                return closeFuture;
            }
            closed = true;
            closeFuture = closePromise = new CompletableFuture<Void>();
        }
        final CompletableFuture<Void> closeWaiterFuture = new CompletableFuture<Void>();
        closeWaiterFuture.whenCompleteAsync(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                complete();
            }
            @Override
            public void onFailure(Throwable cause) {
                complete();
            }

            private void complete() {
                FutureUtils.complete(closePromise, null);
            }
        }, lockStateExecutor.chooseThread(lockPath));
        lockStateExecutor.executeOrdered(
            lockPath, () -> closeWaiter(lockWaiter, closeWaiterFuture));
        return closePromise;
    }

    void internalReacquireLock(final AtomicInteger numRetries,
                               final long lockTimeout,
                               final CompletableFuture<ZKDistributedLock> reacquirePromise) {
        lockStateExecutor.executeOrdered(
            lockPath, () -> doInternalReacquireLock(numRetries, lockTimeout, reacquirePromise));
    }

    void doInternalReacquireLock(final AtomicInteger numRetries,
                                 final long lockTimeout,
                                 final CompletableFuture<ZKDistributedLock> reacquirePromise) {
        internalTryRetries.inc();
        CompletableFuture<ZKDistributedLock> tryPromise = new CompletableFuture<ZKDistributedLock>();
        tryPromise.whenComplete(new FutureEventListener<ZKDistributedLock>() {
            @Override
            public void onSuccess(ZKDistributedLock lock) {
                FutureUtils.complete(reacquirePromise, lock);
            }

            @Override
            public void onFailure(Throwable cause) {
                if (cause instanceof OwnershipAcquireFailedException) {
                    // the lock has been acquired by others
                    FutureUtils.completeExceptionally(reacquirePromise, cause);
                } else {
                    if (numRetries.getAndDecrement() > 0 && !closed) {
                        internalReacquireLock(numRetries, lockTimeout, reacquirePromise);
                    } else {
                        FutureUtils.completeExceptionally(reacquirePromise, cause);
                    }
                }
            }
        });
        doAsyncAcquireWithSemaphore(tryPromise, 0);
    }

    private CompletableFuture<ZKDistributedLock> reacquireLock(boolean throwLockAcquireException)
            throws LockingException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        CompletableFuture<ZKDistributedLock> lockPromise;
        synchronized (this) {
            if (closed) {
                throw newLockClosedException();
            }
            if (null != lockReacquireException) {
                if (throwLockAcquireException) {
                    throw lockReacquireException;
                } else {
                    return null;
                }
            }
            if (null != lockReacquireFuture) {
                return lockReacquireFuture;
            }
            LOG.info("reacquiring lock at {}", lockPath);
            lockReacquireFuture = lockPromise = new CompletableFuture<ZKDistributedLock>();
            lockReacquireFuture.whenComplete(new FutureEventListener<ZKDistributedLock>() {
                @Override
                public void onSuccess(ZKDistributedLock lock) {
                    // if re-acquire successfully, clear the state.
                    synchronized (ZKDistributedLock.this) {
                        lockReacquireFuture = null;
                    }
                    reacquireStats.registerSuccessfulEvent(
                        stopwatch.elapsed(TimeUnit.MICROSECONDS),
                        TimeUnit.MICROSECONDS);
                }

                @Override
                public void onFailure(Throwable cause) {
                    synchronized (ZKDistributedLock.this) {
                        if (cause instanceof LockingException) {
                            lockReacquireException = (LockingException) cause;
                        } else {
                            lockReacquireException = new LockingException(lockPath,
                                    "Exception on re-acquiring lock", cause);
                        }
                    }
                    reacquireStats.registerFailedEvent(
                        stopwatch.elapsed(TimeUnit.MICROSECONDS),
                        TimeUnit.MICROSECONDS);
                }
            });
        }
        reacquireCountUpdater.incrementAndGet(this);
        internalReacquireLock(new AtomicInteger(Integer.MAX_VALUE), 0, lockPromise);
        return lockPromise;
    }

}
