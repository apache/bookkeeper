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

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
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
     * @see #createFullPathOptimistic(ZooKeeper, String, byte[], List, CreateMode)
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
     * Asynchronously deletes zookeeper path recursively and optimistically.
     * This method is used for deleting the leaf nodes and its corresponding
     * parents if they don't have anymore children after deleting the child
     * node. For deleting the parent nodes it uses -1 as znodeversion. If
     * it fails to delete the leafnode then it will callback with the received
     * error code, but it fails to delete the parent node for whatsoever reason
     * it stops proceeding further and it will callback with ok error code.
     *
     * @param zk
     *            Zookeeper client
     * @param originalPath
     *            Zookeeper full path
     * @param znodeVersion
     *            the expected node version of the leafnode
     * @param callback
     *            callback
     * @param leafNodePath
     *            for actual caller this leafNodePath should be same as the
     *            originalPath. But when it is called recursively leafNodePath
     *            remains the same, but the originalPath will be internal nodes.
     */
    public static void asyncDeleteFullPathOptimistic(final ZooKeeper zk, final String originalPath,
            int znodeVersion, final AsyncCallback.VoidCallback callback, final String leafNodePath) {
        zk.delete(originalPath, znodeVersion, new VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (rc == Code.OK.intValue()) {
                    String parent = new File(originalPath).getParent().replace("\\", "/");
                    zk.getData(parent, false, (dRc, dPath, dCtx, data, stat) -> {
                        if (Code.OK.intValue() == dRc && (stat != null && stat.getNumChildren() == 0)) {
                            asyncDeleteFullPathOptimistic(zk, parent, -1, callback, leafNodePath);
                        } else {
                            // parent node is not empty so, complete the
                            // callback
                            callback.processResult(Code.OK.intValue(), path, leafNodePath);
                        }
                    }, null);
                } else {
                    // parent node deletion fails.. so, complete the callback
                    if (path.equals(leafNodePath)) {
                        callback.processResult(rc, path, leafNodePath);
                    } else {
                        callback.processResult(Code.OK.intValue(), path, leafNodePath);
                    }
                }
            }
        }, leafNodePath);
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

    public static void deleteFullPathOptimistic(ZooKeeper zkc, String path, int znodeVersion)
            throws KeeperException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger rc = new AtomicInteger(Code.OK.intValue());
        asyncDeleteFullPathOptimistic(zkc, path, znodeVersion, new VoidCallback() {
            @Override
            public void processResult(int rc2, String path, Object ctx) {
                rc.set(rc2);
                latch.countDown();
            }
        }, path);
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
     * Sync get all children under single zk node.
     *
     * @param zk
     *          zookeeper client
     * @param node
     *          node path
     * @return direct children
     * @throws InterruptedException
     * @throws IOException
     */
    public static List<String> getChildrenInSingleNode(final ZooKeeper zk, final String node, long zkOpTimeoutMs)
            throws InterruptedException, IOException, KeeperException.NoNodeException {
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
            long startTime = System.currentTimeMillis();
            while (!ctx.done) {
                try {
                    ctx.wait(zkOpTimeoutMs > 0 ? zkOpTimeoutMs : 0);
                } catch (InterruptedException e) {
                    ctx.rc = Code.OPERATIONTIMEOUT.intValue();
                    ctx.done = true;
                }
                // timeout the process if get-children response not received
                // zkOpTimeoutMs.
                if (zkOpTimeoutMs > 0 && (System.currentTimeMillis() - startTime) >= zkOpTimeoutMs) {
                    ctx.rc = Code.OPERATIONTIMEOUT.intValue();
                    ctx.done = true;
                }
            }
        }
        if (Code.NONODE.intValue() == ctx.rc) {
            throw new KeeperException.NoNodeException("Got NoNode on call to getChildren on path " + node);
        } else if (Code.OK.intValue() != ctx.rc) {
            throw new IOException("Error on getting children from node " + node);
        }
        return ctx.children;
    }

    /**
     * Async get direct children under single node.
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
     * Compute ZooKeeper ACLs using actual configuration.
     *
     * @param conf Bookie or BookKeeper configuration
     */
    public static List<ACL> getACLs(AbstractConfiguration conf) {
        return conf.isZkEnableSecurity() ? ZooDefs.Ids.CREATOR_ALL_ACL : ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }

}
