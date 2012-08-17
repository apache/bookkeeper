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
package org.apache.hedwig.server.topics;

import java.net.UnknownHostException;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;

import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.server.meta.TopicOwnershipManager;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.apache.hedwig.util.Either;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
/**
 * TopicOwnershipManager based topic manager
 */
public class MMTopicManager extends AbstractTopicManager implements TopicManager {

    static Logger logger = LoggerFactory.getLogger(MMTopicManager.class);

    // topic ownership manager
    private final TopicOwnershipManager mm;
    // hub server manager
    private final HubServerManager hubManager;

    private final HubInfo myHubInfo;
    private final HubLoad myHubLoad;

    // Boolean flag indicating if we should suspend activity. If this is true,
    // all of the Ops put into the queuer will fail automatically.
    protected volatile boolean isSuspended = false;

    public MMTopicManager(ServerConfiguration cfg, ZooKeeper zk, 
                          MetadataManagerFactory mmFactory,
                          ScheduledExecutorService scheduler)
            throws UnknownHostException, PubSubException {
        super(cfg, scheduler);
        // initialize topic ownership manager
        this.mm = mmFactory.newTopicOwnershipManager();
        this.hubManager = new ZkHubServerManager(cfg, zk, addr);

        final SynchronousQueue<Either<HubInfo, PubSubException>> queue =
            new SynchronousQueue<Either<HubInfo, PubSubException>>();

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
        logger.info("Start metadata manager based topic manager with hub id : " + myHubInfo);
    }

    @Override
    protected void realGetOwner(final ByteString topic, final boolean shouldClaim,
                                final Callback<HedwigSocketAddress> cb, final Object ctx) {
        // If operations are suspended due to a ZK client disconnect, just error
        // out this call and return.
        if (isSuspended) {
            cb.operationFailed(ctx, new PubSubException.ServiceDownException(
                                    "MMTopicManager service is temporarily suspended!"));
            return;
        }

        if (topics.contains(topic)) {
            cb.operationFinished(ctx, addr);
            return;
        }

        new MMGetOwnerOp(topic, cb, ctx).read();
    }

    /**
     * MetadataManager do topic ledger election using versioned writes.
     */
    class MMGetOwnerOp {
        ByteString topic;
        Callback<HedwigSocketAddress> cb;
        Object ctx;

        public MMGetOwnerOp(ByteString topic,
                            Callback<HedwigSocketAddress> cb, Object ctx) {
            this.topic = topic;
            this.cb = cb;
            this.ctx = ctx;
        }

        protected void read() {
            mm.readOwnerInfo(topic, new Callback<Versioned<HubInfo>>() {
                @Override
                public void operationFinished(final Object ctx, final Versioned<HubInfo> owner) {
                    if (null == owner) {
                        logger.info("{} : No owner found for topic {}",
                                    new Object[] { addr, topic.toStringUtf8() });
                        // no data found
                        choose(Version.NEW);
                        return;
                    }
                    final Version ownerVersion = owner.getVersion();
                    if (null == owner.getValue()) {
                        logger.info("{} : Invalid owner found for topic {}",
                                    new Object[] { addr, topic.toStringUtf8() });
                        choose(ownerVersion);
                        return;
                    }
                    final HubInfo hub = owner.getValue();
                    logger.info("{} : Read owner of topic {} : {}",
                                new Object[] { addr, topic.toStringUtf8(), hub });

                    logger.info("{}, {}", new Object[] { hub, myHubInfo });

                    if (hub.getAddress().equals(addr)) {
                        if (myHubInfo.getZxid() == hub.getZxid()) {
                            claimTopic(ctx);
                            return;
                        } else {
                            choose(ownerVersion);
                            return;
                        }
                    }

                    logger.info("{} : Check whether owner {} for topic {} is still alive.",
                                new Object[] { addr, hub, topic.toStringUtf8() });
                    hubManager.isHubAlive(hub, new Callback<Boolean>() {
                        @Override
                        public void operationFinished(Object ctx, Boolean isAlive) {
                            if (isAlive) {
                                cb.operationFinished(ctx, hub.getAddress());
                            } else {
                                choose(ownerVersion);
                            }
                        }
                        @Override
                        public void operationFailed(Object ctx, PubSubException pse) {
                            cb.operationFailed(ctx, pse);
                        }
                    }, ctx);
                }

                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    cb.operationFailed(ctx, new PubSubException.ServiceDownException(
                                       "Could not read ownership for topic " + topic.toStringUtf8() + " : "
                                       + exception.getMessage()));
                }
            }, ctx);
        }

        public void claim(final Version prevOwnerVersion) {
            logger.info("{} : claiming topic {} 's owner to be {}",
                        new Object[] { addr, topic.toStringUtf8(), myHubInfo });
            mm.writeOwnerInfo(topic, myHubInfo, prevOwnerVersion, new Callback<Version>() {
                @Override
                public void operationFinished(Object ctx, Version newVersion) {
                    claimTopic(ctx);
                }
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    if (exception instanceof PubSubException.NoTopicOwnerInfoException ||
                        exception instanceof PubSubException.BadVersionException) {
                        // some one has updated the owner
                        logger.info("{} : Some one has claimed topic {} 's owner. Try to read the owner again.",
                                    new Object[] { addr, topic.toStringUtf8() });
                        read();
                        return;
                    }
                    cb.operationFailed(ctx, new PubSubException.ServiceDownException(
                                       "Exception when writing owner info to claim ownership of topic "
                                       + topic.toStringUtf8() + " : " + exception.getMessage()));
                }
            }, ctx);
        }

        protected void claimTopic(Object ctx) {
            logger.info("{} : claimed topic {} 's owner to be {}",
                        new Object[] { addr, topic.toStringUtf8(), myHubInfo });
            notifyListenersAndAddToOwnedTopics(topic, cb, ctx);
            hubManager.uploadSelfLoadData(myHubLoad.setNumTopics(topics.size()));
        }

        public void choose(final Version prevOwnerVersion) {
            hubManager.chooseLeastLoadedHub(new Callback<HubInfo>() {
                @Override
                public void operationFinished(Object ctx, HubInfo owner) {
                    logger.info("{} : Least loaded owner {} is chosen for topic {}",
                                new Object[] { addr, owner, topic.toStringUtf8() });
                    if (owner.getAddress().equals(addr)) {
                        claim(prevOwnerVersion);
                    } else {
                        setOwner(owner, prevOwnerVersion);
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

        public void setOwner(final HubInfo ownerHubInfo, final Version prevOwnerVersion) {
            logger.info("{} : setting topic {} 's owner to be {}",
                        new Object[] { addr, topic.toStringUtf8(), ownerHubInfo });
            mm.writeOwnerInfo(topic, ownerHubInfo, prevOwnerVersion, new Callback<Version>() {
                @Override
                public void operationFinished(Object ctx, Version newVersion) {
                    logger.info("{} : Set topic {} 's owner to be {}",
                                new Object[] { addr, topic.toStringUtf8(), ownerHubInfo });
                    cb.operationFinished(ctx, ownerHubInfo.getAddress());
                }
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    if (exception instanceof PubSubException.NoTopicOwnerInfoException ||
                        exception instanceof PubSubException.BadVersionException) {
                        // some one has updated the owner
                        logger.info("{} : Some one has set topic {} 's owner. Try to read the owner again.",
                                    new Object[] { addr, topic.toStringUtf8() });
                        read();
                        return;
                    }
                    cb.operationFailed(ctx, new PubSubException.ServiceDownException(
                                       "Exception when writing owner info to claim ownership of topic "
                                       + topic.toStringUtf8() + " : " + exception.getMessage()));
                }
            }, ctx);
        }
    }

    @Override
    protected void postReleaseCleanup(final ByteString topic,
                                      final Callback<Void> cb, final Object ctx) {
        mm.readOwnerInfo(topic, new Callback<Versioned<HubInfo>>() {
            @Override
            public void operationFinished(Object ctx, Versioned<HubInfo> owner) {
                if (null == owner) {
                    // Node has somehow disappeared from under us, live with it
                    logger.warn("No owner info found when cleaning up topic " + topic.toStringUtf8());
                    cb.operationFinished(ctx, null);
                    return;
                }
                // no valid hub info found, just return
                if (null == owner.getValue()) {
                    logger.warn("No valid owner info found when cleaning up topic " + topic.toStringUtf8());
                    cb.operationFinished(ctx, null);
                    return;
                }
                HedwigSocketAddress ownerAddr = owner.getValue().getAddress();
                if (!ownerAddr.equals(addr)) {
                    logger.warn("Wanted to clean up self owner info for topic " + topic.toStringUtf8()
                                + " but owner " + owner + " found, leaving untouched");
                    // Not our node, someone else's, leave it alone
                    cb.operationFinished(ctx, null);
                    return;
                }

                mm.deleteOwnerInfo(topic, owner.getVersion(), new Callback<Void>() {
                    @Override
                    public void operationFinished(Object ctx, Void result) {
                        cb.operationFinished(ctx, null);
                    }
                    @Override
                    public void operationFailed(Object ctx, PubSubException exception) {
                        if (exception instanceof PubSubException.NoTopicOwnerInfoException) {
                            logger.warn("Wanted to clean up self owner info for topic " + topic.toStringUtf8()
                                      + " but it has been removed.");
                            cb.operationFinished(ctx, null);
                            return;
                        }
                        logger.error("Exception when deleting self-ownership metadata for topic "
                                     + topic.toStringUtf8() + " : ", exception);
                        cb.operationFailed(ctx, new PubSubException.ServiceDownException(exception));
                    }
                }, ctx);
            }
            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                logger.error("Exception when cleaning up owner info of topic " + topic.toStringUtf8() + " : ", exception);
                cb.operationFailed(ctx, new PubSubException.ServiceDownException(exception));
            }
        }, ctx);
    }

    @Override
    public void stop() {
        // we just unregister it with zookeeper to make it unavailable from hub servers list
        try {
            hubManager.unregisterSelf();
        } catch (IOException e) {
            logger.error("Error unregistering hub server " + myHubInfo + " : ", e);
        }
        super.stop();
    }

}
