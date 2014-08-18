/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.zookeeper.ZooWorker.ZooCallable;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide a zookeeper client to handle session expire
 */
public class ZooKeeperClient extends ZooKeeper implements Watcher {

    final static Logger logger = LoggerFactory.getLogger(ZooKeeperClient.class);

    // ZooKeeper client connection variables
    private final String connectString;
    private final int sessionTimeoutMs;

    // state for the zookeeper client
    private final AtomicReference<ZooKeeper> zk = new AtomicReference<ZooKeeper>();
    private final ZooKeeperWatcherBase watcherManager;

    private final ScheduledExecutorService retryExecutor;
    private final ExecutorService connectExecutor;

    // retry polices
    private final RetryPolicy connectRetryPolicy;
    private final RetryPolicy operationRetryPolicy;

    private final Callable<ZooKeeper> clientCreator = new Callable<ZooKeeper>() {

        @Override
        public ZooKeeper call() throws Exception {
            try {
                return ZooWorker.syncCallWithRetries(null, new ZooCallable<ZooKeeper>() {

                    @Override
                    public ZooKeeper call() throws KeeperException, InterruptedException {
                        logger.info("Reconnecting zookeeper {}.", connectString);
                        ZooKeeper newZk;
                        try {
                            newZk = createZooKeeper();
                        } catch (IOException ie) {
                            logger.error("Failed to create zookeeper instance to " + connectString, ie);
                            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
                        }
                        // close the previous one
                        closeZkHandle();
                        zk.set(newZk);
                        if (logger.isDebugEnabled()) {
                            logger.debug("ZooKeeper session {} is created to {}.",
                                    Long.toHexString(newZk.getSessionId()), connectString);
                        }
                        return newZk;
                    }

                    @Override
                    public String toString() {
                        return String.format("ZooKeeper Client Creator (%s)", connectString);
                    }

                }, connectRetryPolicy);
            } catch (Exception e) {
                logger.error("Gave up reconnecting to ZooKeeper : ", e);
                Runtime.getRuntime().exit(-1);
                return null;
            }
        }

    };

    public static ZooKeeper createConnectedZooKeeper(String connectString, int sessionTimeoutMs)
                    throws KeeperException, InterruptedException, IOException {
        ZooKeeperWatcherBase watcher = new ZooKeeperWatcherBase(sessionTimeoutMs);
        ZooKeeper zk = new ZooKeeper(connectString, sessionTimeoutMs, watcher);
        try {
            watcher.waitForConnection();
        } catch (KeeperException ke) {
            zk.close();
            throw ke;
        } catch (InterruptedException ie) {
            zk.close();
            throw ie;
        }
        return zk;
    }

    public static ZooKeeperClient createConnectedZooKeeperClient(String connectString, int sessionTimeoutMs)
                    throws KeeperException, InterruptedException, IOException {
        ZooKeeperWatcherBase watcherManager = new ZooKeeperWatcherBase(sessionTimeoutMs);
        ZooKeeperClient client = new ZooKeeperClient(connectString, sessionTimeoutMs, watcherManager,
                new BoundExponentialBackoffRetryPolicy(sessionTimeoutMs, sessionTimeoutMs, 0));
        try {
            watcherManager.waitForConnection();
        } catch (KeeperException ke) {
            client.close();
            throw ke;
        } catch (InterruptedException ie) {
            client.close();
            throw ie;
        }
        return client;
    }

    public static ZooKeeperClient createConnectedZooKeeperClient(
            String connectString, int sessionTimeoutMs, RetryPolicy operationRetryPolicy)
                    throws KeeperException, InterruptedException, IOException {
        ZooKeeperWatcherBase watcherManager = new ZooKeeperWatcherBase(sessionTimeoutMs); 
        ZooKeeperClient client = new ZooKeeperClient(connectString, sessionTimeoutMs, watcherManager,
                operationRetryPolicy);
        try {
            watcherManager.waitForConnection();
        } catch (KeeperException ke) {
            client.close();
            throw ke;
        } catch (InterruptedException ie) {
            client.close();
            throw ie;
        }
        return client;
    }

    public static ZooKeeperClient createConnectedZooKeeperClient(
            String connectString, int sessionTimeoutMs, Set<Watcher> childWatchers,
            RetryPolicy operationRetryPolicy)
                    throws KeeperException, InterruptedException, IOException {
        ZooKeeperWatcherBase watcherManager =
                new ZooKeeperWatcherBase(sessionTimeoutMs, childWatchers);
        ZooKeeperClient client = new ZooKeeperClient(connectString, sessionTimeoutMs, watcherManager,
                operationRetryPolicy);
        try {
            watcherManager.waitForConnection();
        } catch (KeeperException ke) {
            client.close();
            throw ke;
        } catch (InterruptedException ie) {
            client.close();
            throw ie;
        }
        return client;
    }

    ZooKeeperClient(String connectString, int sessionTimeoutMs, ZooKeeperWatcherBase watcherManager,
            RetryPolicy operationRetryPolicy) throws IOException {
        this(connectString, sessionTimeoutMs, watcherManager,
                new BoundExponentialBackoffRetryPolicy(6000, 60000, Integer.MAX_VALUE),
                operationRetryPolicy);
    }

    private ZooKeeperClient(String connectString, int sessionTimeoutMs,
            ZooKeeperWatcherBase watcherManager,
            RetryPolicy connectRetryPolicy, RetryPolicy operationRetryPolicy) throws IOException {
        super(connectString, sessionTimeoutMs, watcherManager);
        this.connectString = connectString;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.watcherManager = watcherManager;
        this.connectRetryPolicy = connectRetryPolicy;
        this.operationRetryPolicy = operationRetryPolicy;
        this.retryExecutor =
                Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
        this.connectExecutor =
                Executors.newSingleThreadExecutor();
        // added itself to the watcher
        watcherManager.addChildWatcher(this);
    }

    @Override
    public void close() throws InterruptedException {
        connectExecutor.shutdown();
        retryExecutor.shutdown();
        closeZkHandle();
    }
    
    private void closeZkHandle() throws InterruptedException {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            super.close();
        } else {
            zkHandle.close();
        }
    }

    protected void waitForConnection() throws KeeperException, InterruptedException {
        watcherManager.waitForConnection();
    }

    protected ZooKeeper createZooKeeper() throws IOException {
        return new ZooKeeper(connectString, sessionTimeoutMs, watcherManager);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == EventType.None &&
                event.getState() == KeeperState.Expired) {
            onExpired();
        }
    }

    private void onExpired() {
        if (logger.isDebugEnabled()) {
            logger.debug("ZooKeeper session {} is expired from {}.",
                    Long.toHexString(getSessionId()), connectString);
        }
        try {
            connectExecutor.submit(clientCreator);
        } catch (RejectedExecutionException ree) {
            logger.error("ZooKeeper reconnect task is rejected : ", ree);
        }
    }


    static abstract class RetryRunnable implements Runnable {

        final ZooWorker worker;
        final Runnable that;

        RetryRunnable(RetryPolicy retryPolicy) {
            worker = new ZooWorker(retryPolicy);
            that = this;
        }

    }

    // inherits from ZooKeeper client for all operations

    @Override
    public long getSessionId() {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return super.getSessionId();
        }
        return zkHandle.getSessionId();
    }

    @Override
    public byte[] getSessionPasswd() {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return super.getSessionPasswd();
        }
        return zkHandle.getSessionPasswd();
    }

    @Override
    public int getSessionTimeout() {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return super.getSessionTimeout();
        }
        return zkHandle.getSessionTimeout();
    }

    @Override
    public void addAuthInfo(String scheme, byte[] auth) {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            super.addAuthInfo(scheme, auth);
            return;
        }
        zkHandle.addAuthInfo(scheme, auth);
    }

    @Override
    public synchronized void register(Watcher watcher) {
        watcherManager.addChildWatcher(watcher);
    }

    @Override
    public List<OpResult> multi(final Iterable<Op> ops) throws InterruptedException, KeeperException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<List<OpResult>>() {

            @Override
            public List<OpResult> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.multi(ops);
                }
                return zkHandle.multi(ops);
            }

        }, operationRetryPolicy);
    }

    @Override
    @Deprecated
    public Transaction transaction() {
        // since there is no reference about which client that the transaction could use
        // so just use ZooKeeper instance directly.
        // you'd better to use {@link #multi}.
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return super.transaction();
        }
        return zkHandle.transaction();
    }

    @Override
    public List<ACL> getACL(final String path, final Stat stat) throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<List<ACL>>() {

            @Override
            public List<ACL> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.getACL(path, stat);
                }
                return zkHandle.getACL(path, stat);
            }

        }, operationRetryPolicy);
    }

    @Override
    public void getACL(final String path, final Stat stat, final ACLCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final ACLCallback aclCb = new ACLCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, List<ACL> acl, Stat stat) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, acl, stat);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.getACL(path, stat, aclCb, worker);
                } else {
                    zkHandle.getACL(path, stat, aclCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public Stat setACL(final String path, final List<ACL> acl, final int version)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<Stat>() {

            @Override
            public Stat call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.setACL(path, acl, version);
                }
                return zkHandle.setACL(path, acl, version);
            }

        }, operationRetryPolicy);
    }

    @Override
    public void setACL(final String path, final List<ACL> acl, final int version,
            final StatCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final StatCallback stCb = new StatCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, stat);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.setACL(path, acl, version, stCb, worker);
                } else {
                    zkHandle.setACL(path, acl, version, stCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void sync(final String path, final VoidCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final VoidCallback vCb = new VoidCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.sync(path, vCb, worker);
                } else {
                    zkHandle.sync(path, vCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public States getState() {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return ZooKeeperClient.super.getState();
        } else {
            return zkHandle.getState();
        }
    }

    @Override
    public String toString() {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return ZooKeeperClient.super.toString();
        } else {
            return zkHandle.toString();
        }
    }

    @Override
    public String create(final String path, final byte[] data,
            final List<ACL> acl, final CreateMode createMode)
                    throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<String>() {

            @Override
            public String call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.create(path, data, acl, createMode);
                }
                return zkHandle.create(path, data, acl, createMode);
            }

        }, operationRetryPolicy);
    }

    @Override
    public void create(final String path, final byte[] data, final List<ACL> acl,
            final CreateMode createMode, final StringCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final StringCallback createCb = new StringCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, String name) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, name);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.create(path, data, acl, createMode, createCb, worker);
                } else {
                    zkHandle.create(path, data, acl, createMode, createCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void delete(final String path, final int version) throws KeeperException, InterruptedException {
        ZooWorker.syncCallWithRetries(this, new ZooCallable<Void>() {

            @Override
            public Void call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.delete(path, version);
                } else {
                    zkHandle.delete(path, version);
                }
                return null;
            }

        }, operationRetryPolicy);
    }

    @Override
    public void delete(final String path, final int version, final VoidCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final VoidCallback deleteCb = new VoidCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.delete(path, version, deleteCb, worker);
                } else {
                    zkHandle.delete(path, version, deleteCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public Stat exists(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<Stat>() {

            @Override
            public Stat call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.exists(path, watcher);
                }
                return zkHandle.exists(path, watcher);
            }

        }, operationRetryPolicy);
    }

    @Override
    public Stat exists(final String path, final boolean watch) throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<Stat>() {

            @Override
            public Stat call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.exists(path, watch);
                }
                return zkHandle.exists(path, watch);
            }

        }, operationRetryPolicy);
    }

    @Override
    public void exists(final String path, final Watcher watcher, final StatCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final StatCallback stCb = new StatCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, stat);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.exists(path, watcher, stCb, worker);
                } else {
                    zkHandle.exists(path, watcher, stCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void exists(final String path, final boolean watch, final StatCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final StatCallback stCb = new StatCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, stat);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.exists(path, watch, stCb, worker);
                } else {
                    zkHandle.exists(path, watch, stCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public byte[] getData(final String path, final Watcher watcher, final Stat stat)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<byte[]>() {

            @Override
            public byte[] call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.getData(path, watcher, stat);
                }
                return zkHandle.getData(path, watcher, stat);
            }

        }, operationRetryPolicy);
    }

    @Override
    public byte[] getData(final String path, final boolean watch, final Stat stat)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<byte[]>() {

            @Override
            public byte[] call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.getData(path, watch, stat);
                }
                return zkHandle.getData(path, watch, stat);
            }

        }, operationRetryPolicy);
    }

    @Override
    public void getData(final String path, final Watcher watcher, final DataCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final DataCallback dataCb = new DataCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, data, stat);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.getData(path, watcher, dataCb, worker);
                } else {
                    zkHandle.getData(path, watcher, dataCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void getData(final String path, final boolean watch, final DataCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final DataCallback dataCb = new DataCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, data, stat);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.getData(path, watch, dataCb, worker);
                } else {
                    zkHandle.getData(path, watch, dataCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public Stat setData(final String path, final byte[] data, final int version)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<Stat>() {

            @Override
            public Stat call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.setData(path, data, version);
                }
                return zkHandle.setData(path, data, version);
            }

        }, operationRetryPolicy);
    }

    @Override
    public void setData(final String path, final byte[] data, final int version,
            final StatCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final StatCallback stCb = new StatCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, stat);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.setData(path, data, version, stCb, worker);
                } else {
                    zkHandle.setData(path, data, version, stCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public List<String> getChildren(final String path, final Watcher watcher, final Stat stat)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<List<String>>() {

            @Override
            public List<String> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.getChildren(path, watcher, stat);
                }
                return zkHandle.getChildren(path, watcher, stat);
            }

        }, operationRetryPolicy);
    }

    @Override
    public List<String> getChildren(final String path, final boolean watch, final Stat stat)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<List<String>>() {

            @Override
            public List<String> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.getChildren(path, watch, stat);
                }
                return zkHandle.getChildren(path, watch, stat);
            }

        }, operationRetryPolicy);
    }

    @Override
    public void getChildren(final String path, final Watcher watcher,
            final Children2Callback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final Children2Callback childCb = new Children2Callback() {

                @Override
                public void processResult(int rc, String path, Object ctx,
                        List<String> children, Stat stat) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, children, stat);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.getChildren(path, watcher, childCb, worker);
                } else {
                    zkHandle.getChildren(path, watcher, childCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void getChildren(final String path, final boolean watch, final Children2Callback cb,
            final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final Children2Callback childCb = new Children2Callback() {

                @Override
                public void processResult(int rc, String path, Object ctx,
                        List<String> children, Stat stat) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, children, stat);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.getChildren(path, watch, childCb, worker);
                } else {
                    zkHandle.getChildren(path, watch, childCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }


    @Override
    public List<String> getChildren(final String path, final Watcher watcher)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<List<String>>() {

            @Override
            public List<String> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.getChildren(path, watcher);
                }
                return zkHandle.getChildren(path, watcher);
            }

        }, operationRetryPolicy);
    }

    @Override
    public List<String> getChildren(final String path, final boolean watch)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooCallable<List<String>>() {

            @Override
            public List<String> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return ZooKeeperClient.super.getChildren(path, watch);
                }
                return zkHandle.getChildren(path, watch);
            }

        }, operationRetryPolicy);
    }

    @Override
    public void getChildren(final String path, final Watcher watcher,
            final ChildrenCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final ChildrenCallback childCb = new ChildrenCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx,
                        List<String> children) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, children);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.getChildren(path, watcher, childCb, worker);
                } else {
                    zkHandle.getChildren(path, watcher, childCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void getChildren(final String path, final boolean watch,
            final ChildrenCallback cb, final Object context) {
        final Runnable proc = new RetryRunnable(operationRetryPolicy) {

            final ChildrenCallback childCb = new ChildrenCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx,
                        List<String> children) {
                    ZooWorker worker = (ZooWorker)ctx;
                    if (worker.allowRetry(rc)) {
                        retryExecutor.schedule(that, worker.nextRetryWaitTime(), TimeUnit.MILLISECONDS);
                    } else {
                        cb.processResult(rc, path, context, children);
                    }
                }

            };

            @Override
            public void run() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    ZooKeeperClient.super.getChildren(path, watch, childCb, worker);
                } else {
                    zkHandle.getChildren(path, watch, childCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

}
