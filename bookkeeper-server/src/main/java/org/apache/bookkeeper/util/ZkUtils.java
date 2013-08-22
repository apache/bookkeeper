/*
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

package org.apache.bookkeeper.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provided utilites for zookeeper access, etc.
 */
public class ZkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ZkUtils.class);

    /**
     * Asynchronously create zookeeper path recursively and optimistically.
     *
     * @see #createFullPathOptimistic(ZooKeeper,String,byte[],List<ACL>,CreateMode)
     *
     * @param zk
     *          Zookeeper client
     * @param originalPath
     *          Zookeeper full path
     * @param data
     *          Zookeeper data
     * @param acl
     *          Acl of the zk path
     * @param createMode
     *          Create mode of zk path
     * @param callback
     *          Callback
     * @param ctx
     *          Context object
     */
    public static void asyncCreateFullPathOptimistic(
        final ZooKeeper zk, final String originalPath, final byte[] data,
        final List<ACL> acl, final CreateMode createMode,
        final AsyncCallback.StringCallback callback, final Object ctx) {

        zk.create(originalPath, data, acl, createMode, new StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {

                if (rc != Code.NONODE.intValue()) {
                    callback.processResult(rc, path, ctx, name);
                    return;
                }

                // Since I got a nonode, it means that my parents don't exist
                // create mode is persistent since ephemeral nodes can't be
                // parents
                String parent = new File(originalPath).getParent().replace("\\", "/");
                asyncCreateFullPathOptimistic(zk, parent, new byte[0], acl,
                        CreateMode.PERSISTENT, new StringCallback() {

                            @Override
                            public void processResult(int rc, String path, Object ctx, String name) {
                                if (rc == Code.OK.intValue() || rc == Code.NODEEXISTS.intValue()) {
                                    // succeeded in creating the parent, now
                                    // create the original path
                                    asyncCreateFullPathOptimistic(zk, originalPath, data,
                                            acl, createMode, callback, ctx);
                                } else {
                                    callback.processResult(rc, path, ctx, name);
                                }
                            }
                        }, ctx);
            }
        }, ctx);
    }

    /**
     * Create zookeeper path recursively and optimistically. This method can throw
     * any of the KeeperExceptions which can be thrown by ZooKeeper#create.
     * KeeperException.NodeExistsException will only be thrown if the full path specified
     * by _path_ already exists. The existence of any parent znodes is not an error
     * condition.
     *
     * @param zkc
     *            - ZK instance
     * @param path
     *            - znode path
     * @param data
     *            - znode data
     * @param acl
     *            - Acl of the zk path
     * @param createMode
     *            - Create mode of zk path
     * @throws KeeperException
     *             if the server returns a non-zero error code, or invalid ACL
     * @throws InterruptedException
     *             if the transaction is interrupted
     */
    public static void createFullPathOptimistic(ZooKeeper zkc, String path,
            byte[] data, final List<ACL> acl, final CreateMode createMode)
            throws KeeperException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger rc = new AtomicInteger(Code.OK.intValue());
        asyncCreateFullPathOptimistic(zkc, path, data, acl, createMode,
                                      new StringCallback() {
                                          @Override
                                          public void processResult(int rc2, String path,
                                                                    Object ctx, String name) {
                                              rc.set(rc2);
                                              latch.countDown();
                                          }
                                      }, null);
        latch.await();
        if (rc.get() != Code.OK.intValue()) {
            throw KeeperException.create(Code.get(rc.get()));
        }
    }

    private static class GetChildrenCtx {
        int rc;
        boolean done = false;
        List<String> children = null;
    }

    /**
     * Sync get all children under single zk node
     *
     * @param zk
     *          zookeeper client
     * @param node
     *          node path
     * @return direct children
     * @throws InterruptedException
     * @throws IOException
     */
    public static List<String> getChildrenInSingleNode(final ZooKeeper zk, final String node)
            throws InterruptedException, IOException {
        final GetChildrenCtx ctx = new GetChildrenCtx();
        getChildrenInSingleNode(zk, node, new GenericCallback<List<String>>() {
            @Override
            public void operationComplete(int rc, List<String> ledgers) {
                synchronized (ctx) {
                    if (Code.OK.intValue() == rc) {
                        ctx.children = ledgers;
                    }
                    ctx.rc = rc;
                    ctx.done = true;
                    ctx.notifyAll();
                }
            }
        });

        synchronized (ctx) {
            while (ctx.done == false) {
                ctx.wait();
            }
        }
        if (Code.OK.intValue() != ctx.rc) {
            throw new IOException("Error on getting children from node " + node);
        }
        return ctx.children;

    }

    /**
     * Async get direct children under single node
     *
     * @param zk
     *          zookeeper client
     * @param node
     *          node path
     * @param cb
     *          callback function
     */
    public static void getChildrenInSingleNode(final ZooKeeper zk, final String node,
            final GenericCallback<List<String>> cb) {
        zk.sync(node, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("ZK error syncing nodes when getting children: ", KeeperException
                            .create(KeeperException.Code.get(rc), path));
                    cb.operationComplete(rc, null);
                    return;
                }
                zk.getChildren(node, false, new AsyncCallback.ChildrenCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, List<String> nodes) {
                        if (rc != Code.OK.intValue()) {
                            LOG.error("Error polling ZK for the available nodes: ", KeeperException
                                    .create(KeeperException.Code.get(rc), path));
                            cb.operationComplete(rc, null);
                            return;
                        }

                        cb.operationComplete(rc, nodes);

                    }
                }, null);
            }
        }, null);
    }

    /**
     * Get new ZooKeeper client. Waits till the connection is complete. If
     * connection is not successful within timeout, then throws back exception.
     *
     * @param servers
     *            ZK servers connection string.
     * @param timeout
     *            Session timeout.
     */
    public static ZooKeeper createConnectedZookeeperClient(String servers,
            ZooKeeperWatcherBase w) throws IOException, InterruptedException,
            KeeperException {
        if (servers == null || servers.isEmpty()) {
            throw new IllegalArgumentException("servers cannot be empty");
        }
        final ZooKeeper newZk = new ZooKeeper(servers, w.getZkSessionTimeOut(),
                w);
        w.waitForConnection();
        // Re-checking zookeeper connection status
        if (!newZk.getState().isConnected()) {
            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
        }
        return newZk;
    }
}
