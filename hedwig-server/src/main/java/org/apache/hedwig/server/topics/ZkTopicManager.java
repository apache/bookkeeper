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
package org.apache.hedwig.server.topics;

import java.net.UnknownHostException;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import static com.google.common.base.Charsets.UTF_8;
import com.google.protobuf.ByteString;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.apache.hedwig.util.Either;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.hedwig.zookeeper.SafeAsyncZKCallback;
import org.apache.hedwig.zookeeper.ZkUtils;
import org.apache.hedwig.zookeeper.SafeAsyncZKCallback.DataCallback;
import org.apache.hedwig.zookeeper.SafeAsyncZKCallback.StatCallback;

/**
 * Topics are operated on in parallel as they are independent.
 *
 */
public class ZkTopicManager extends AbstractTopicManager implements TopicManager {

    static Logger logger = LoggerFactory.getLogger(ZkTopicManager.class);

    /**
     * Persistent storage for topic metadata.
     */
    private ZooKeeper zk;

    // hub server manager
    private final HubServerManager hubManager;

    private final HubInfo myHubInfo;
    private final HubLoad myHubLoad;

    // Boolean flag indicating if we should suspend activity. If this is true,
    // all of the Ops put into the queuer will fail automatically.
    protected volatile boolean isSuspended = false;

    /**
     * Create a new topic manager. Pass in an active ZooKeeper client object.
     *
     * @param zk
     */
    public ZkTopicManager(final ZooKeeper zk, final ServerConfiguration cfg, ScheduledExecutorService scheduler)
            throws UnknownHostException, PubSubException {

        super(cfg, scheduler);
        this.zk = zk;
        this.hubManager = new ZkHubServerManager(cfg, zk, addr);

        myHubLoad = new HubLoad(topics.size());
        this.hubManager.registerListener(new HubServerManager.ManagerListener() {
            @Override
            public void onSuspend() {
                isSuspended = true;
            }
            @Override
            public void onResume() {
                isSuspended = false;
            }
            @Override
            public void onShutdown() {
                // if hub server manager can't work, we had to quit
                Runtime.getRuntime().exit(1);
            }
        });

        final SynchronousQueue<Either<HubInfo, PubSubException>> queue =
            new SynchronousQueue<Either<HubInfo, PubSubException>>();
        this.hubManager.registerSelf(myHubLoad, new Callback<HubInfo>() {
            @Override
            public void operationFinished(final Object ctx, final HubInfo resultOfOperation) {
                logger.info("Successfully registered hub {} with zookeeper", resultOfOperation);
                ConcurrencyUtils.put(queue, Either.of(resultOfOperation, (PubSubException) null));
            }
            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                logger.error("Failed to register hub with zookeeper", exception);
                ConcurrencyUtils.put(queue, Either.of((HubInfo)null, exception));
            }
        }, null);

        Either<HubInfo, PubSubException> result = ConcurrencyUtils.take(queue);
        PubSubException pse = result.right();
        if (pse != null) {
            throw pse;
        }
        myHubInfo = result.left();
    }

    String hubPath(ByteString topic) {
        return cfg.getZkTopicPath(new StringBuilder(), topic).append("/hub").toString();
    }

    @Override
    protected void realGetOwner(final ByteString topic, final boolean shouldClaim,
                                final Callback<HedwigSocketAddress> cb, final Object ctx) {
        // If operations are suspended due to a ZK client disconnect, just error
        // out this call and return.
        if (isSuspended) {
            cb.operationFailed(ctx, new PubSubException.ServiceDownException(
                                   "ZKTopicManager service is temporarily suspended!"));
            return;
        }

        TopicStats stats = topics.getIfPresent(topic);
        if (null != stats) {
            cb.operationFinished(ctx, addr);
            return;
        }

        new ZkGetOwnerOp(topic, shouldClaim, cb, ctx).read();
    }

    // Recursively call each other.
    class ZkGetOwnerOp {
        ByteString topic;
        boolean shouldClaim;
        Callback<HedwigSocketAddress> cb;
        Object ctx;
        String hubPath;

        public ZkGetOwnerOp(ByteString topic, boolean shouldClaim, Callback<HedwigSocketAddress> cb, Object ctx) {
            this.topic = topic;
            this.shouldClaim = shouldClaim;
            this.cb = cb;
            this.ctx = ctx;
            hubPath = hubPath(topic);

        }

        public void choose() {
            hubManager.chooseLeastLoadedHub(new Callback<HubInfo>() {
                @Override
                public void operationFinished(Object ctx, HubInfo owner) {
                    logger.info("{} : Least loaded owner {} is chosen for topic {}",
                                new Object[] { addr, owner, topic.toStringUtf8() });
                    if (owner.getAddress().equals(addr)) {
                        claim();
                    } else {
                        cb.operationFinished(ZkGetOwnerOp.this.ctx, owner.getAddress());
                    }
                }
                @Override
                public void operationFailed(Object ctx, PubSubException pse) {
                    logger.error("Failed to choose least loaded hub server for topic "
                               + topic.toStringUtf8() + " : ", pse);
                    cb.operationFailed(ctx, pse);
                }
            }, null);
        }

        public void claimOrChoose() {
            if (shouldClaim)
                claim();
            else
                choose();
        }

        public void read() {
            zk.getData(hubPath, false, new SafeAsyncZKCallback.DataCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx, byte[] data, Stat stat) {

                    if (rc == Code.NONODE.intValue()) {
                        claimOrChoose();
                        return;
                    }

                    if (rc != Code.OK.intValue()) {
                        KeeperException e = ZkUtils.logErrorAndCreateZKException("Could not read ownership for topic: "
                                            + topic.toStringUtf8(), path, rc);
                        cb.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                        return;
                    }

                    // successfully did a read
                    try {
                        HubInfo ownerHubInfo = HubInfo.parse(new String(data, UTF_8));
                        HedwigSocketAddress owner = ownerHubInfo.getAddress();
                        if (!owner.equals(addr)) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("topic: " + topic.toStringUtf8() + " belongs to someone else: " + owner);
                            }
                            cb.operationFinished(ctx, owner);
                            return;
                        }
                        logger.info("Discovered stale self-node for topic: " + topic.toStringUtf8() + ", will delete it");
                    } catch (HubInfo.InvalidHubInfoException ihie) {
                        logger.info("Discovered invalid hub info for topic: " + topic.toStringUtf8() + ", will delete it : ", ihie);
                    }

                    // we must have previously failed and left a
                    // residual ephemeral node here, so we must
                    // delete it (clean it up) and then
                    // re-create/re-acquire the topic.
                    zk.delete(hubPath, stat.getVersion(), new VoidCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx) {
                            if (Code.OK.intValue() == rc || Code.NONODE.intValue() == rc) {
                                claimOrChoose();
                            } else {
                                KeeperException e = ZkUtils.logErrorAndCreateZKException(
                                                        "Could not delete self node for topic: " + topic.toStringUtf8(), path, rc);
                                cb.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                            }
                        }
                    }, ctx);
                }
            }, ctx);
        }

        public void claim() {
            if (logger.isDebugEnabled()) {
                logger.debug("claiming topic: " + topic.toStringUtf8());
            }

            ZkUtils.createFullPathOptimistic(zk, hubPath,
                    myHubInfo.toString().getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL, new SafeAsyncZKCallback.StringCallback() {

                @Override
                public void safeProcessResult(int rc, String path, Object ctx, String name) {
                    if (rc == Code.OK.intValue()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("claimed topic: " + topic.toStringUtf8());
                        }
                        notifyListenersAndAddToOwnedTopics(topic, cb, ctx);
                        hubManager.uploadSelfLoadData(myHubLoad.setNumTopics(topics.size()));
                    } else if (rc == Code.NODEEXISTS.intValue()) {
                        read();
                    } else {
                        KeeperException e = ZkUtils.logErrorAndCreateZKException(
                                                "Failed to create ephemeral node to claim ownership of topic: "
                                                + topic.toStringUtf8(), path, rc);
                        cb.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                    }
                }
            }, ctx);
        }

    }

    @Override
    protected void postReleaseCleanup(final ByteString topic, final Callback<Void> cb, Object ctx) {

        zk.getData(hubPath(topic), false, new SafeAsyncZKCallback.DataCallback() {
            @Override
            public void safeProcessResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (rc == Code.NONODE.intValue()) {
                    // Node has somehow disappeared from under us, live with it
                    // since its a transient node
                    logger.warn("While deleting self-node for topic: " + topic.toStringUtf8() + ", node not found");
                    cb.operationFinished(ctx, null);
                    return;
                }

                if (rc != Code.OK.intValue()) {
                    KeeperException e = ZkUtils.logErrorAndCreateZKException(
                                            "Failed to delete self-ownership node for topic: " + topic.toStringUtf8(), path, rc);
                    cb.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                    return;
                }

                String hubInfoStr = new String(data, UTF_8);
                try {
                    HubInfo ownerHubInfo = HubInfo.parse(hubInfoStr);
                    HedwigSocketAddress owner = ownerHubInfo.getAddress();
                    if (!owner.equals(addr)) {
                        logger.warn("Wanted to delete self-node for topic: " + topic.toStringUtf8() + " but node for "
                                    + owner + " found, leaving untouched");
                        // Not our node, someone else's, leave it alone
                        cb.operationFinished(ctx, null);
                        return;
                    }
                } catch (HubInfo.InvalidHubInfoException ihie) {
                    logger.info("Invalid hub info " + hubInfoStr + " found when release topic "
                              + topic.toStringUtf8() + ". Leaving untouched until next acquire action.");
                    cb.operationFinished(ctx, null);
                    return;
                }

                zk.delete(path, stat.getVersion(), new SafeAsyncZKCallback.VoidCallback() {
                    @Override
                    public void safeProcessResult(int rc, String path, Object ctx) {
                        if (rc != Code.OK.intValue() && rc != Code.NONODE.intValue()) {
                            KeeperException e = ZkUtils
                                                .logErrorAndCreateZKException("Failed to delete self-ownership node for topic: "
                                                        + topic.toStringUtf8(), path, rc);
                            cb.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                            return;
                        }

                        cb.operationFinished(ctx, null);
                    }
                }, ctx);
            }
        }, ctx);
    }

    @Override
    public void stop() {
        // we just unregister it with zookeeper to make it unavailable from hub servers list
        try {
            hubManager.unregisterSelf();
        } catch (IOException e) {
            logger.error("Error unregistering hub server :", e);
        }
        super.stop();
    }

}
