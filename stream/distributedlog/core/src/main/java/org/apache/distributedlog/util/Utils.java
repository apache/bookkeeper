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
package org.apache.distributedlog.util;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.DistributedLogConstants;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.common.functions.VoidFunctions;
import org.apache.distributedlog.exceptions.BKTransmitException;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.io.AsyncAbortable;
import org.apache.distributedlog.io.AsyncCloseable;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Basic Utilities.
 */
@Slf4j
public class Utils {

    /**
     * Current time from some arbitrary time base in the past, counting in
     * nanoseconds, and not affected by settimeofday or similar system clock
     * changes. This is appropriate to use when computing how much longer to
     * wait for an interval to expire.
     *
     * @return current time in nanoseconds.
     */
    public static long nowInNanos() {
        return System.nanoTime();
    }

    /**
     * Current time from some fixed base time - so useful for cross machine comparison.
     *
     * @return current time in milliseconds.
     */
    public static long nowInMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Milliseconds elapsed since the time specified, the input is nanoTime
     * the only conversion happens when computing the elapsed time.
     *
     * @param startMsecTime the start of the interval that we are measuring
     * @return elapsed time in milliseconds.
     */
    public static long elapsedMSec(long startMsecTime) {
        return (System.currentTimeMillis() - startMsecTime);
    }

    public static boolean randomPercent(double percent) {
        return (Math.random() * 100.0) <= percent;
    }

    /**
     * Synchronously create zookeeper path recursively and optimistically.
     *
     * @see #zkAsyncCreateFullPathOptimistic(ZooKeeperClient, String, byte[], List, CreateMode)
     * @param zkc Zookeeper client
     * @param path Zookeeper full path
     * @param data Zookeeper data
     * @param acl Acl of the zk path
     * @param createMode Create mode of zk path
     * @throws ZooKeeperClient.ZooKeeperConnectionException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void zkCreateFullPathOptimistic(
        ZooKeeperClient zkc,
        String path,
        byte[] data,
        final List<ACL> acl,
        final CreateMode createMode) throws IOException, KeeperException {
        try {
            FutureUtils.result(zkAsyncCreateFullPathOptimistic(zkc, path, data, acl, createMode));
        } catch (ZooKeeperClient.ZooKeeperConnectionException zkce) {
            throw zkce;
        } catch (KeeperException ke) {
            throw ke;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted on create zookeeper path " + path, ie);
        } catch (RuntimeException rte) {
            throw rte;
        } catch (Exception exc) {
            throw new RuntimeException("Unexpected Exception", exc);
        }
    }

    /**
     * Asynchronously create zookeeper path recursively and optimistically.
     *
     * @param zkc Zookeeper client
     * @param pathToCreate  Zookeeper full path
     * @param parentPathShouldNotCreate The recursive creation should stop if this path doesn't exist
     * @param data Zookeeper data
     * @param acl Acl of the zk path
     * @param createMode Create mode of zk path
     * @param callback Callback
     * @param ctx Context object
     */
    public static void zkAsyncCreateFullPathOptimisticRecursive(
        final ZooKeeperClient zkc,
        final String pathToCreate,
        final Optional<String> parentPathShouldNotCreate,
        final byte[] data,
        final List<ACL> acl,
        final CreateMode createMode,
        final AsyncCallback.StringCallback callback,
        final Object ctx) {
        try {
            zkc.get().create(pathToCreate, data, acl, createMode, new AsyncCallback.StringCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, String name) {

                    if (rc != KeeperException.Code.NONODE.intValue()) {
                        callback.processResult(rc, path, ctx, name);
                        return;
                    }

                    // Since we got a nonode, it means that my parents may not exist
                    // ephemeral nodes can't have children so Create mode is always
                    // persistent parents
                    int lastSlash = pathToCreate.lastIndexOf('/');
                    if (lastSlash <= 0) {
                        callback.processResult(rc, path, ctx, name);
                        return;
                    }
                    String parent = pathToCreate.substring(0, lastSlash);
                    if (parentPathShouldNotCreate.isPresent()
                            && Objects.equal(parentPathShouldNotCreate.get(), parent)) {
                        // we should stop here
                        callback.processResult(rc, path, ctx, name);
                        return;
                    }
                    zkAsyncCreateFullPathOptimisticRecursive(zkc, parent, parentPathShouldNotCreate, new byte[0], acl,
                            CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
                                @Override
                                public void processResult(int rc, String path, Object ctx, String name) {
                                    if (rc == KeeperException.Code.OK.intValue()
                                            || rc == KeeperException.Code.NODEEXISTS.intValue()) {
                                        // succeeded in creating the parent, now create the original path
                                        zkAsyncCreateFullPathOptimisticRecursive(zkc, pathToCreate,
                                                parentPathShouldNotCreate, data, acl, createMode, callback, ctx);
                                    } else {
                                        callback.processResult(rc, path, ctx, name);
                                    }
                                }
                            }, ctx);
                }
            }, ctx);
        } catch (ZooKeeperClient.ZooKeeperConnectionException zkce) {
            callback.processResult(DistributedLogConstants.ZK_CONNECTION_EXCEPTION_RESULT_CODE,
                    zkce.getMessage(), ctx, pathToCreate);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            callback.processResult(DistributedLogConstants.DL_INTERRUPTED_EXCEPTION_RESULT_CODE,
                    ie.getMessage(), ctx, pathToCreate);
        }
    }

    /**
     * Asynchronously create zookeeper path recursively and optimistically.
     *
     * @param zkc Zookeeper client
     * @param pathToCreate  Zookeeper full path
     * @param data Zookeeper data
     * @param acl Acl of the zk path
     * @param createMode Create mode of zk path
     */
    public static CompletableFuture<Void> zkAsyncCreateFullPathOptimistic(
        final ZooKeeperClient zkc,
        final String pathToCreate,
        final byte[] data,
        final List<ACL> acl,
        final CreateMode createMode) {
        Optional<String> parentPathShouldNotCreate = Optional.empty();
        return zkAsyncCreateFullPathOptimistic(
                zkc,
                pathToCreate,
                parentPathShouldNotCreate,
                data,
                acl,
                createMode);
    }

    /**
     * Asynchronously create zookeeper path recursively and optimistically.
     *
     * @param zkc Zookeeper client
     * @param pathToCreate  Zookeeper full path
     * @param parentPathShouldNotCreate zookeeper parent path should not be created
     * @param data Zookeeper data
     * @param acl Acl of the zk path
     * @param createMode Create mode of zk path
     */
    public static CompletableFuture<Void> zkAsyncCreateFullPathOptimistic(
        final ZooKeeperClient zkc,
        final String pathToCreate,
        final Optional<String> parentPathShouldNotCreate,
        final byte[] data,
        final List<ACL> acl,
        final CreateMode createMode) {
        final CompletableFuture<Void> result = new CompletableFuture<Void>();

        zkAsyncCreateFullPathOptimisticRecursive(zkc, pathToCreate, parentPathShouldNotCreate,
                data, acl, createMode, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                handleKeeperExceptionCode(rc, path, result);
            }
        }, result);

        return result;
    }

    /**
     * Asynchronously create zookeeper path recursively and optimistically.
     *
     * @param zkc Zookeeper client
     * @param pathToCreate  Zookeeper full path
     * @param data Zookeeper data
     * @param acl Acl of the zk path
     * @param createMode Create mode of zk path
     */
    public static CompletableFuture<Void> zkAsyncCreateFullPathOptimisticAndSetData(
        final ZooKeeperClient zkc,
        final String pathToCreate,
        final byte[] data,
        final List<ACL> acl,
        final CreateMode createMode) {
        final CompletableFuture<Void> result = new CompletableFuture<Void>();

        try {
            zkc.get().setData(pathToCreate, data, -1, new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    if (rc != KeeperException.Code.NONODE.intValue()) {
                        handleKeeperExceptionCode(rc, path, result);
                        return;
                    }

                    Optional<String> parentPathShouldNotCreate = Optional.empty();
                    zkAsyncCreateFullPathOptimisticRecursive(zkc, pathToCreate, parentPathShouldNotCreate,
                            data, acl, createMode, new AsyncCallback.StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, String name) {
                            handleKeeperExceptionCode(rc, path, result);
                        }
                    }, result);
                }
            }, result);
        } catch (Exception exc) {
            result.completeExceptionally(exc);
        }

        return result;
    }

    private static void handleKeeperExceptionCode(int rc, String pathOrMessage, CompletableFuture<Void> result) {
        if (KeeperException.Code.OK.intValue() == rc) {
            result.complete(null);
        } else if (DistributedLogConstants.ZK_CONNECTION_EXCEPTION_RESULT_CODE == rc) {
            result.completeExceptionally(new ZooKeeperClient.ZooKeeperConnectionException(pathOrMessage));
        } else if (DistributedLogConstants.DL_INTERRUPTED_EXCEPTION_RESULT_CODE == rc) {
            result.completeExceptionally(new DLInterruptedException(pathOrMessage));
        } else {
            result.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc), pathOrMessage));
        }
    }

    public static CompletableFuture<Versioned<byte[]>> zkGetData(ZooKeeperClient zkc, String path, boolean watch) {
        ZooKeeper zk;
        try {
            zk = zkc.get();
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            return FutureUtils.exception(zkException(e, path));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return FutureUtils.exception(zkException(e, path));
        }
        return zkGetData(zk, path, watch);
    }

    /**
     * Retrieve data from zookeeper <code>path</code>.
     *
     * @param path
     *          zookeeper path to retrieve data
     * @param watch
     *          whether to watch the path
     * @return future representing the versioned value. null version or null value means path doesn't exist.
     */
    public static CompletableFuture<Versioned<byte[]>> zkGetData(ZooKeeper zk, String path, boolean watch) {
        final CompletableFuture<Versioned<byte[]>> promise = new CompletableFuture<Versioned<byte[]>>();
        zk.getData(path, watch, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    if (null == stat) {
                        promise.complete(new Versioned<byte[]>(null, null));
                    } else {
                        promise.complete(new Versioned<byte[]>(data, new LongVersion(stat.getVersion())));
                    }
                } else if (KeeperException.Code.NONODE.intValue() == rc) {
                    promise.complete(new Versioned<byte[]>(null, null));
                } else {
                    promise.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc)));
                }
            }
        }, null);
        return promise;
    }

    public static CompletableFuture<LongVersion> zkSetData(ZooKeeperClient zkc,
                                                         String path, byte[] data, LongVersion version) {
        ZooKeeper zk;
        try {
            zk = zkc.get();
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            return FutureUtils.exception(zkException(e, path));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return FutureUtils.exception(zkException(e, path));
        }
        return zkSetData(zk, path, data, version);
    }

    /**
     * Set <code>data</code> to zookeeper <code>path</code>.
     *
     * @param zk
     *          zookeeper client
     * @param path
     *          path to set data
     * @param data
     *          data to set
     * @param version
     *          version used to set data
     * @return future representing the version after this operation.
     */
    public static CompletableFuture<LongVersion> zkSetData(
            ZooKeeper zk, String path, byte[] data, LongVersion version) {
        final CompletableFuture<LongVersion> promise = new CompletableFuture<LongVersion>();
        zk.setData(path, data, (int) version.getLongVersion(), new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    promise.complete(new LongVersion(stat.getVersion()));
                    return;
                }
                promise.completeExceptionally(
                        KeeperException.create(KeeperException.Code.get(rc)));
                return;
            }
        }, null);
        return promise;
    }

    public static CompletableFuture<Void> zkDelete(ZooKeeperClient zkc, String path, LongVersion version) {
        ZooKeeper zk;
        try {
            zk = zkc.get();
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            return FutureUtils.exception(zkException(e, path));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return FutureUtils.exception(zkException(e, path));
        }
        return zkDelete(zk, path, version);
    }

    /**
     * Delete the given <i>path</i> from zookeeper.
     *
     * @param zk
     *          zookeeper client
     * @param path
     *          path to delete
     * @param version
     *          version used to set data
     * @return future representing the version after this operation.
     */
    public static CompletableFuture<Void> zkDelete(ZooKeeper zk, String path, LongVersion version) {
        final CompletableFuture<Void> promise = new CompletableFuture<Void>();
        zk.delete(path, (int) version.getLongVersion(), new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    promise.complete(null);
                    return;
                }
                promise.completeExceptionally(
                        KeeperException.create(KeeperException.Code.get(rc)));
                return;
            }
        }, null);
        return promise;
    }

    /**
     * Delete the given <i>path</i> from zookeeper.
     *
     * @param zkc
     *          zookeeper client
     * @param path
     *          path to delete
     * @param version
     *          version used to set data
     * @return future representing if the delete is successful. Return true if the node is deleted,
     * false if the node doesn't exist, otherwise future will throw exception
     *
     */
    public static CompletableFuture<Boolean> zkDeleteIfNotExist(ZooKeeperClient zkc, String path, LongVersion version) {
        ZooKeeper zk;
        try {
            zk = zkc.get();
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            return FutureUtils.exception(zkException(e, path));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return FutureUtils.exception(zkException(e, path));
        }
        final CompletableFuture<Boolean> promise = new CompletableFuture<Boolean>();
        zk.delete(path, (int) version.getLongVersion(), new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    promise.complete(true);
                } else if (KeeperException.Code.NONODE.intValue() == rc) {
                    promise.complete(false);
                } else {
                    promise.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc)));
                }
            }
        }, null);
        return promise;
    }

    public static CompletableFuture<Void> asyncClose(@Nullable AsyncCloseable closeable,
                                          boolean swallowIOException) {
        if (null == closeable) {
            return FutureUtils.Void();
        } else if (swallowIOException) {
            return FutureUtils.ignore(closeable.asyncClose());
        } else {
            return closeable.asyncClose();
        }
    }

    /**
     * Sync zookeeper client on given <i>path</i>.
     *
     * @param zkc
     *          zookeeper client
     * @param path
     *          path to sync
     * @return zookeeper client after sync
     * @throws IOException
     */
    public static ZooKeeper sync(ZooKeeperClient zkc, String path) throws IOException {
        ZooKeeper zk;
        try {
            zk = zkc.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted on checking if log " + path + " exists", e);
        }
        final CountDownLatch syncLatch = new CountDownLatch(1);
        final AtomicInteger syncResult = new AtomicInteger(0);
        zk.sync(path, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                syncResult.set(rc);
                syncLatch.countDown();
            }
        }, null);
        try {
            syncLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted on syncing zookeeper connection", e);
        }
        if (KeeperException.Code.OK.intValue() != syncResult.get()) {
            throw new ZKException("Error syncing zookeeper connection ",
                    KeeperException.Code.get(syncResult.get()));
        }
        return zk;
    }

    /**
     * Close a closeable.
     *
     * @param closeable
     *          closeable to close
     */
    public static void close(@Nullable Closeable closeable) {
        if (null == closeable) {
            return;
        }
        try {
            Closeables.close(closeable, true);
        } catch (IOException e) {
            // no-op. the exception is swallowed.
        }
    }

    /**
     * Close an async closeable.
     *
     * @param closeable
     *          closeable to close
     */
    public static void close(@Nullable AsyncCloseable closeable)
            throws IOException {
        if (null == closeable) {
            return;
        }
        Utils.ioResult(closeable.asyncClose());
    }

    /**
     * Close an async closeable.
     *
     * @param closeable
     *          closeable to close
     */
    public static void closeQuietly(@Nullable AsyncCloseable closeable) {
        if (null == closeable) {
            return;
        }
        try {
            Utils.ioResult(closeable.asyncClose());
        } catch (IOException e) {
            // no-op. the exception is swallowed.
        }
    }

    /**
     * Close the closeables in sequence.
     *
     * @param closeables
     *          closeables to close
     * @return future represents the close future
     */
    public static CompletableFuture<Void> closeSequence(ExecutorService executorService,
                                             AsyncCloseable... closeables) {
        return closeSequence(executorService, false, closeables);
    }

    /**
     * Close the closeables in sequence and ignore errors during closing.
     *
     * @param executorService executor to execute closeable
     * @param ignoreCloseError whether to ignore errors during closing
     * @param closeables list of closeables
     * @return future represents the close future.
     */
    public static CompletableFuture<Void> closeSequence(ExecutorService executorService,
                                             boolean ignoreCloseError,
                                             AsyncCloseable... closeables) {
        List<AsyncCloseable> closeableList = Lists.newArrayListWithExpectedSize(closeables.length);
        for (AsyncCloseable closeable : closeables) {
            if (null == closeable) {
                closeableList.add(AsyncCloseable.NULL);
            } else {
                closeableList.add(closeable);
            }
        }
        return FutureUtils.processList(
                closeableList,
                ignoreCloseError ? AsyncCloseable.CLOSE_FUNC_IGNORE_ERRORS : AsyncCloseable.CLOSE_FUNC,
                executorService
        ).thenApply(VoidFunctions.LIST_TO_VOID_FUNC);
    }

    /**
     * Gets the parent of a path.
     *
     * @param path
     *            path to get the parent of
     * @return parent of the path or null if no parent exists.
     */
    public static String getParent(final String path) {
        if (path == null) {
            return null;
        }
        if (path.length() < 2) {
            return null;
        }
        int firstIndex = path.indexOf("/");
        if (firstIndex == -1) {
            return null;
        }
        int lastIndex = path.lastIndexOf("/");
        if (lastIndex == path.length() - 1) {
            lastIndex = path.substring(0, path.length() - 1).lastIndexOf("/");
        }
        if (lastIndex == -1) {
            return null;
        }
        if (lastIndex == 0) {
            return "/";
        }
        return path.substring(0, lastIndex);
    }

    /**
     * Convert the <i>throwable</i> to zookeeper related exceptions.
     *
     * @param throwable cause
     * @param path zookeeper path
     * @return zookeeper related exceptions
     */
    public static Throwable zkException(Throwable throwable, String path) {
        if (throwable instanceof KeeperException) {
            return new ZKException("Encountered zookeeper exception on " + path, (KeeperException) throwable);
        } else if (throwable instanceof ZooKeeperClient.ZooKeeperConnectionException) {
            return new ZKException("Encountered zookeeper connection loss on " + path,
                    KeeperException.Code.CONNECTIONLOSS);
        } else if (throwable instanceof InterruptedException) {
            return new DLInterruptedException("Interrupted on operating " + path, throwable);
        } else {
            return new UnexpectedException("Encountered unexpected exception on operatiing " + path, throwable);
        }
    }

    /**
     * Create transmit exception from transmit result.
     *
     * @param transmitResult
     *          transmit result (basically bk exception code)
     * @return transmit exception
     */
    public static BKTransmitException transmitException(int transmitResult) {
        return new BKTransmitException("Failed to write to bookkeeper; Error is ("
            + transmitResult + ") "
            + BKException.getMessage(transmitResult), transmitResult);
    }

    /**
     * A specific version of {@link FutureUtils#result(CompletableFuture)} to handle known exception issues.
     */
    public static <T> T ioResult(CompletableFuture<T> result) throws IOException {
        return FutureUtils.result(
            result,
            (cause) -> {
                if (cause instanceof IOException) {
                    return (IOException) cause;
                } else if (cause instanceof KeeperException) {
                    return new ZKException("Encountered zookeeper exception on waiting result",
                        (KeeperException) cause);
                } else if (cause instanceof BKException) {
                    return new BKTransmitException("Encountered bookkeeper exception on waiting result",
                        ((BKException) cause).getCode());
                } else if (cause instanceof InterruptedException) {
                    return new DLInterruptedException("Interrupted on waiting result", cause);
                } else {
                    return new IOException("Encountered exception on waiting result", cause);
                }
            });
    }

    /**
     * A specific version of {@link FutureUtils#result(CompletableFuture, long, TimeUnit)}
     * to handle known exception issues.
     */
    public static <T> T ioResult(CompletableFuture<T> result, long timeout, TimeUnit timeUnit)
            throws IOException, TimeoutException {
        return FutureUtils.result(
            result,
            (cause) -> {
                if (cause instanceof IOException) {
                    return (IOException) cause;
                } else if (cause instanceof KeeperException) {
                    return new ZKException("Encountered zookeeper exception on waiting result",
                        (KeeperException) cause);
                } else if (cause instanceof BKException) {
                    return new BKTransmitException("Encountered bookkeeper exception on waiting result",
                        ((BKException) cause).getCode());
                } else if (cause instanceof InterruptedException) {
                    return new DLInterruptedException("Interrupted on waiting result", cause);
                } else {
                    return new IOException("Encountered exception on waiting result", cause);
                }
            },
            timeout,
            timeUnit);
    }

    /**
     * Abort async <i>abortable</i>.
     *
     * @param abortable the {@code AsyncAbortable} object to be aborted, or null, in which case this method
     *                  does nothing.
     * @param swallowIOException if true, don't propagate IO exceptions thrown by the {@code abort} methods
     * @throws IOException if {@code swallowIOException} is false and {@code abort} throws an {@code IOException}
     */
    public static void abort(@Nullable AsyncAbortable abortable,
                             boolean swallowIOException)
            throws IOException {
        if (null == abortable) {
            return;
        }
        try {
            ioResult(abortable.asyncAbort());
        } catch (Exception ioe) {
            if (swallowIOException) {
                log.warn("IOException thrown while aborting Abortable {} : ", abortable, ioe);
            } else {
                throw ioe;
            }
        }
    }

}
