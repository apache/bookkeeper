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
package org.apache.hedwig.server.meta;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRanges;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionState;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.topics.HubInfo;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.zookeeper.SafeAsyncZKCallback;
import org.apache.hedwig.zookeeper.ZkUtils;
import static com.google.common.base.Charsets.UTF_8;

/**
 * ZooKeeper-based Metadata Manager.
 */
public class ZkMetadataManagerFactory extends MetadataManagerFactory {
    protected final static Logger logger = LoggerFactory.getLogger(ZkMetadataManagerFactory.class);

    static final int CUR_VERSION = 1;

    ZooKeeper zk;
    ServerConfiguration cfg;

    @Override
    public int getCurrentVersion() {
        return CUR_VERSION;
    }

    @Override
    public MetadataManagerFactory initialize(ServerConfiguration cfg,
                                             ZooKeeper zk,
                                             int version)
    throws IOException {
        if (CUR_VERSION != version) {
            throw new IOException("Incompatible ZkMetadataManagerFactory version " + version
                                + " found, expected version " + CUR_VERSION);
        }
        this.cfg = cfg;
        this.zk = zk;
        return this;
    }

    @Override
    public void shutdown() {
        // do nothing here, because zookeeper handle is passed from outside
        // we don't need to stop it.
    }

    @Override
    public Iterator<ByteString> getTopics() throws IOException {
        List<String> topics;
        try {
            topics = zk.getChildren(cfg.getZkTopicsPrefix(new StringBuilder()).toString(), false);
        } catch (KeeperException ke) {
            throw new IOException("Failed to get topics list : ", ke);
        } catch (InterruptedException ie) {
            throw new IOException("Interrupted on getting topics list : ", ie);
        }
        final Iterator<String> iter = topics.iterator();
        return new Iterator<ByteString>() {
            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }
            @Override
            public ByteString next() {
                String t = iter.next();
                return ByteString.copyFromUtf8(t);
            }
            @Override
            public void remove() {
                iter.remove();
            }
        };
    }

    @Override
    public TopicPersistenceManager newTopicPersistenceManager() {
        return new ZkTopicPersistenceManagerImpl(cfg, zk);
    }

    @Override
    public SubscriptionDataManager newSubscriptionDataManager() {
        return new ZkSubscriptionDataManagerImpl(cfg, zk);
    }

    @Override
    public TopicOwnershipManager newTopicOwnershipManager() {
        return new ZkTopicOwnershipManagerImpl(cfg, zk);
    }

    /**
     * ZooKeeper based topic persistence manager.
     */
    static class ZkTopicPersistenceManagerImpl implements TopicPersistenceManager {

        ZooKeeper zk;
        ServerConfiguration cfg;

        ZkTopicPersistenceManagerImpl(ServerConfiguration conf, ZooKeeper zk) {
            this.cfg = conf;
            this.zk = zk;
        }

        @Override
        public void close() throws IOException {
            // do nothing in zookeeper based impl
        }

        /**
         * Get znode path to store persistence info of a topic.
         *
         * @param topic
         *          Topic name
         * @return znode path to store persistence info.
         */
        private String ledgersPath(ByteString topic) {
            return cfg.getZkTopicPath(new StringBuilder(), topic).append("/ledgers").toString();
        }

        /**
         * Parse ledger ranges data and return it thru callback.
         *
         * @param topic
         *          Topic name
         * @param data
         *          Topic Ledger Ranges data
         * @param version
         *          Version of the topic ledger ranges data
         * @param callback
         *          Callback to return ledger ranges
         * @param ctx
         *          Context of the callback
         */
        private void parseAndReturnTopicLedgerRanges(ByteString topic, byte[] data, int version,
                                                     Callback<Versioned<LedgerRanges>> callback, Object ctx) {
            try {
                Versioned<LedgerRanges> ranges = new Versioned<LedgerRanges>(LedgerRanges.parseFrom(data),
                                                                             new ZkVersion(version));
                callback.operationFinished(ctx, ranges);
                return;
            } catch (InvalidProtocolBufferException e) {
                String msg = "Ledger ranges for topic:" + topic.toStringUtf8() + " could not be deserialized";
                logger.error(msg, e);
                callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                return;
            }
        }

        @Override
        public void readTopicPersistenceInfo(final ByteString topic,
                                             final Callback<Versioned<LedgerRanges>> callback,
                                             Object ctx) {
            // read topic ledgers node data
            final String zNodePath = ledgersPath(topic);

            zk.getData(zNodePath, false, new SafeAsyncZKCallback.DataCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    if (rc == Code.OK.intValue()) {
                        parseAndReturnTopicLedgerRanges(topic, data, stat.getVersion(), callback, ctx);
                        return;
                    }

                    if (rc == Code.NONODE.intValue()) {
                        // we don't create the znode until we first write it.
                        callback.operationFinished(ctx, null);
                        return;
                    }

                    // otherwise some other error
                    KeeperException ke =
                        ZkUtils.logErrorAndCreateZKException("Could not read ledgers node for topic: "
                                                             + topic.toStringUtf8(), path, rc);
                    callback.operationFailed(ctx, new PubSubException.ServiceDownException(ke));
                }
            }, ctx);
        }

        private void createTopicPersistenceInfo(final ByteString topic, LedgerRanges ranges,
                                                final Callback<Version> callback, Object ctx) {
            final String zNodePath = ledgersPath(topic);
            final byte[] data = ranges.toByteArray();
            // create it
            ZkUtils.createFullPathOptimistic(zk, zNodePath, data, Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT, new SafeAsyncZKCallback.StringCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx, String name) {
                    if (rc == Code.NODEEXISTS.intValue()) {
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.TOPIC_PERSISTENCE_INFO_EXISTS,
                                                      "Persistence info of topic " + topic.toStringUtf8() + " existed."));
                        return;
                    }
                    if (rc != Code.OK.intValue()) {
                        KeeperException ke = ZkUtils.logErrorAndCreateZKException(
                                             "Could not create ledgers node for topic: " + topic.toStringUtf8(),
                                             path, rc);
                        callback.operationFailed(ctx, new PubSubException.ServiceDownException(ke));
                        return;
                    }
                    // initial version is version 0
                    callback.operationFinished(ctx, new ZkVersion(0));
                }
            }, ctx);
            return;
        }

        @Override
        public void writeTopicPersistenceInfo(final ByteString topic, LedgerRanges ranges, final Version version,
                                              final Callback<Version> callback, Object ctx) {
            if (Version.NEW == version) {
                createTopicPersistenceInfo(topic, ranges, callback, ctx);
                return;
            }

            final String zNodePath = ledgersPath(topic);
            final byte[] data = ranges.toByteArray();

            if (!(version instanceof ZkVersion)) {
                callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(
                                              "Invalid version provided to update persistence info for topic " + topic.toStringUtf8()));
                return;
            }

            int znodeVersion = ((ZkVersion)version).getZnodeVersion();
            zk.setData(zNodePath, data, znodeVersion, new SafeAsyncZKCallback.StatCallback() {
                    @Override
                    public void safeProcessResult(int rc, String path, Object ctx, Stat stat) {
                        if (rc == Code.NONODE.intValue()) {
                            // no node
                            callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_TOPIC_PERSISTENCE_INFO,
                                                          "No persistence info found for topic " + topic.toStringUtf8()));
                            return;
                        } else if (rc == Code.BADVERSION.intValue()) {
                            // bad version
                            callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                                          "Bad version provided to update persistence info of topic " + topic.toStringUtf8()));
                            return;
                        } else if (rc == Code.OK.intValue()) {
                            callback.operationFinished(ctx, new ZkVersion(stat.getVersion()));
                            return;
                        } else {
                            KeeperException ke = ZkUtils.logErrorAndCreateZKException(
                                    "Could not write ledgers node for topic: " + topic.toStringUtf8(), path, rc);
                            callback.operationFailed(ctx, new PubSubException.ServiceDownException(ke));
                            return;
                        }
                    }
            }, ctx);
        }

        @Override
        public void deleteTopicPersistenceInfo(final ByteString topic, final Version version,
                                               final Callback<Void> callback, Object ctx) {
            final String zNodePath = ledgersPath(topic);

            int znodeVersion = -1;
            if (Version.ANY != version) {
                if (!(version instanceof ZkVersion)) {
                    callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(
                                                  "Invalid version provided to delete persistence info for topic " + topic.toStringUtf8()));
                    return;
                } else {
                    znodeVersion = ((ZkVersion)version).getZnodeVersion();
                }
            }
            zk.delete(zNodePath, znodeVersion, new SafeAsyncZKCallback.VoidCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx) {
                    if (rc == Code.OK.intValue()) {
                        callback.operationFinished(ctx, null);
                        return;
                    } else if (rc == Code.NONODE.intValue()) {
                        // no node
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_TOPIC_PERSISTENCE_INFO,
                                                      "No persistence info found for topic " + topic.toStringUtf8()));
                        return;
                    } else if (rc == Code.BADVERSION.intValue()) {
                        // bad version
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                                      "Bad version provided to delete persistence info of topic " + topic.toStringUtf8()));
                        return;
                    }

                    KeeperException e = ZkUtils.logErrorAndCreateZKException("Topic: " + topic.toStringUtf8()
                                        + " failed to delete persistence info @version " + version + " : ", path, rc);
                    callback.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                }
            }, ctx);
        }
    }

    /**
     * ZooKeeper based subscription data manager.
     */
    static class ZkSubscriptionDataManagerImpl implements SubscriptionDataManager {

        ZooKeeper zk;
        ServerConfiguration cfg;

        ZkSubscriptionDataManagerImpl(ServerConfiguration conf, ZooKeeper zk) {
            this.cfg = conf;
            this.zk = zk;
        }

        @Override
        public void close() throws IOException {
            // do nothing in zookeeper based impl
        }

        /**
         * Get znode path to store subscription states.
         *
         * @param sb
         *          String builder to store the znode path.
         * @param topic
         *          Topic name.
         *
         * @return string builder to store znode path.
         */
        private StringBuilder topicSubscribersPath(StringBuilder sb, ByteString topic) {
            return cfg.getZkTopicPath(sb, topic).append("/subscribers");
        }

        /**
         * Get znode path to store subscription state for a specified subscriber.
         *
         * @param topic
         *          Topic name.
         * @param subscriber
         *          Subscriber id.
         * @return znode path to store subscription state.
         */
        private String topicSubscriberPath(ByteString topic, ByteString subscriber) {
            return topicSubscribersPath(new StringBuilder(), topic).append("/").append(subscriber.toStringUtf8())
                   .toString();
        }

        @Override
        public boolean isPartialUpdateSupported() {
            return false;
        }

        @Override
        public void createSubscriptionData(final ByteString topic, final ByteString subscriberId, final SubscriptionData data,
                                           final Callback<Version> callback, final Object ctx) {
            ZkUtils.createFullPathOptimistic(zk, topicSubscriberPath(topic, subscriberId), data.toByteArray(),
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new SafeAsyncZKCallback.StringCallback() {

                @Override
                public void safeProcessResult(int rc, String path, Object ctx, String name) {

                    if (rc == Code.NODEEXISTS.intValue()) {
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.SUBSCRIPTION_STATE_EXISTS,
                                                      "Subscription state for (topic:" + topic.toStringUtf8() + ", subscriber:"
                                                      + subscriberId.toStringUtf8() + ") existed."));
                        return;
                    } else if (rc == Code.OK.intValue()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully recorded subscription for topic: " + topic.toStringUtf8()
                                         + " subscriberId: " + subscriberId.toStringUtf8() + " data: "
                                         + SubscriptionStateUtils.toString(data));
                        }
                        callback.operationFinished(ctx, new ZkVersion(0));
                    } else {
                        KeeperException ke = ZkUtils.logErrorAndCreateZKException(
                                                 "Could not record new subscription for topic: " + topic.toStringUtf8()
                                                 + " subscriberId: " + subscriberId.toStringUtf8(), path, rc);
                        callback.operationFailed(ctx, new PubSubException.ServiceDownException(ke));
                    }
                }
            }, ctx);
        }

        @Override
        public void updateSubscriptionData(final ByteString topic, final ByteString subscriberId, final SubscriptionData data,
                                           final Version version, final Callback<Version> callback, final Object ctx) {
            throw new UnsupportedOperationException("ZooKeeper based metadata manager doesn't support partial update!");
        }

        @Override
        public void replaceSubscriptionData(final ByteString topic, final ByteString subscriberId, final SubscriptionData data,
                                            final Version version, final Callback<Version> callback, final Object ctx) {
            int znodeVersion = -1;
            if (Version.NEW == version) {
                callback.operationFailed(ctx, 
                        new PubSubException.BadVersionException("Can not replace Version.New subscription data"));
                return;
            } else if (Version.ANY != version) {
                if (!(version instanceof ZkVersion)) {
                    callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(
                                                  "Invalid version provided to replace subscription data for topic  " 
                                                  + topic.toStringUtf8() + " subscribe id: " + subscriberId));
                    return;
                } else {
                    znodeVersion = ((ZkVersion)version).getZnodeVersion();
                }
            }
            zk.setData(topicSubscriberPath(topic, subscriberId), data.toByteArray(), 
                    znodeVersion, new SafeAsyncZKCallback.StatCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx, Stat stat) {
                    if (rc == Code.NONODE.intValue()) {
                        // no node
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_SUBSCRIPTION_STATE,
                                                      "No subscription state found for (topic:" + topic.toStringUtf8() + ", subscriber:"
                                                      + subscriberId.toStringUtf8() + ")."));
                        return;
                    } else if (rc == Code.BADVERSION.intValue()) {
                        // bad version
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                                      "Bad version provided to replace subscription data of topic " 
                                                      + topic.toStringUtf8() + " subscriberId " + subscriberId));
                        return;
                    } else if (rc != Code.OK.intValue()) {
                        KeeperException e = ZkUtils.logErrorAndCreateZKException("Topic: " + topic.toStringUtf8()
                                            + " subscriberId: " + subscriberId.toStringUtf8()
                                            + " could not set subscription data: " + SubscriptionStateUtils.toString(data),
                                            path, rc);
                        callback.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully updated subscription for topic: " + topic.toStringUtf8()
                                         + " subscriberId: " + subscriberId.toStringUtf8() + " data: "
                                         + SubscriptionStateUtils.toString(data));
                        }

                        callback.operationFinished(ctx, new ZkVersion(stat.getVersion()));
                    }
                }
            }, ctx);
        }

        @Override
        public void deleteSubscriptionData(final ByteString topic, final ByteString subscriberId, Version version,
                                           final Callback<Void> callback, Object ctx) {
            
            int znodeVersion = -1;
            if (Version.NEW == version) {
                callback.operationFailed(ctx, 
                        new PubSubException.BadVersionException("Can not delete Version.New subscription data"));
                return;
            } else if (Version.ANY != version) {
                if (!(version instanceof ZkVersion)) {
                    callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(
                                                  "Invalid version provided to delete subscription data for topic  " 
                                                  + topic.toStringUtf8() + " subscribe id: " + subscriberId));
                    return;
                } else {
                    znodeVersion = ((ZkVersion)version).getZnodeVersion();
                }
            }
            
            zk.delete(topicSubscriberPath(topic, subscriberId), znodeVersion, new SafeAsyncZKCallback.VoidCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx) {
                    if (rc == Code.NONODE.intValue()) {
                        // no node
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_SUBSCRIPTION_STATE,
                                                      "No subscription state found for (topic:" + topic.toStringUtf8() + ", subscriber:"
                                                      + subscriberId.toStringUtf8() + ")."));
                        return;
                    } else if (rc == Code.BADVERSION.intValue()) {
                        // bad version
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                                      "Bad version provided to delete subscription data of topic " 
                                                      + topic.toStringUtf8() + " subscriberId " + subscriberId));
                        return;
                    } else if (rc == Code.OK.intValue()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully deleted subscription for topic: " + topic.toStringUtf8()
                                         + " subscriberId: " + subscriberId.toStringUtf8());
                        }

                        callback.operationFinished(ctx, null);
                        return;
                    }

                    KeeperException e = ZkUtils.logErrorAndCreateZKException("Topic: " + topic.toStringUtf8()
                                        + " subscriberId: " + subscriberId.toStringUtf8() + " failed to delete subscription", path, rc);
                    callback.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                }
            }, ctx);
        }

        @Override
        public void readSubscriptionData(final ByteString topic, final ByteString subscriberId,
                                         final Callback<Versioned<SubscriptionData>> callback, final Object ctx) {
            zk.getData(topicSubscriberPath(topic, subscriberId), false, new SafeAsyncZKCallback.DataCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    if (rc == Code.NONODE.intValue()) {
                        callback.operationFinished(ctx, null);
                        return;
                    }
                    if (rc != Code.OK.intValue()) {
                        KeeperException e = ZkUtils.logErrorAndCreateZKException(
                                                "Could not read subscription data for topic: " + topic.toStringUtf8()
                                                + ", subscriberId: " + subscriberId.toStringUtf8(), path, rc);
                        callback.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                        return;
                    }
                    
                    Versioned<SubscriptionData> subData;
                    try {
                        subData = new Versioned<SubscriptionData>(
                                        SubscriptionStateUtils.parseSubscriptionData(data), 
                                        new ZkVersion(stat.getVersion()));
                    } catch (InvalidProtocolBufferException ex) {
                        String msg = "Failed to deserialize subscription data for topic: " + topic.toStringUtf8()
                                     + " subscriberId: " + subscriberId.toStringUtf8();
                        logger.error(msg, ex);
                        callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                        return;
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("Found subscription while acquiring topic: " + topic.toStringUtf8()
                                     + " subscriberId: " + subscriberId.toStringUtf8()
                                     + " data: " + SubscriptionStateUtils.toString(subData.getValue()));
                    }
                    callback.operationFinished(ctx, subData);
                }
            }, ctx);
        }

        @Override
        public void readSubscriptions(final ByteString topic,
                                      final Callback<Map<ByteString, Versioned<SubscriptionData>>> cb, final Object ctx) {
            String topicSubscribersPath = topicSubscribersPath(new StringBuilder(), topic).toString();
            zk.getChildren(topicSubscribersPath, false, new SafeAsyncZKCallback.ChildrenCallback() {
                @Override
                public void safeProcessResult(int rc, String path, final Object ctx, final List<String> children) {

                    if (rc != Code.OK.intValue() && rc != Code.NONODE.intValue()) {
                        KeeperException e = ZkUtils.logErrorAndCreateZKException("Could not read subscribers for topic "
                                            + topic.toStringUtf8(), path, rc);
                        cb.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                        return;
                    }

                    final Map<ByteString, Versioned<SubscriptionData>> topicSubs = 
                            new ConcurrentHashMap<ByteString, Versioned<SubscriptionData>>();

                    if (rc == Code.NONODE.intValue() || children.size() == 0) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("No subscriptions found while acquiring topic: " + topic.toStringUtf8());
                        }
                        cb.operationFinished(ctx, topicSubs);
                        return;
                    }

                    final AtomicBoolean failed = new AtomicBoolean();
                    final AtomicInteger count = new AtomicInteger();

                    for (final String child : children) {

                        final ByteString subscriberId = ByteString.copyFromUtf8(child);
                        final String childPath = path + "/" + child;

                        zk.getData(childPath, false, new SafeAsyncZKCallback.DataCallback() {
                            @Override
                            public void safeProcessResult(int rc, String path, Object ctx, byte[] data, Stat stat) {

                                if (rc != Code.OK.intValue()) {
                                    KeeperException e = ZkUtils.logErrorAndCreateZKException(
                                                            "Could not read subscription data for topic: " + topic.toStringUtf8()
                                                            + ", subscriberId: " + subscriberId.toStringUtf8(), path, rc);
                                    reportFailure(new PubSubException.ServiceDownException(e));
                                    return;
                                }

                                if (failed.get()) {
                                    return;
                                }

                                Versioned<SubscriptionData> subData;
                                try {
                                    subData = new Versioned<SubscriptionData>(
                                            SubscriptionStateUtils.parseSubscriptionData(data), 
                                            new ZkVersion(stat.getVersion()));
                                } catch (InvalidProtocolBufferException ex) {
                                    String msg = "Failed to deserialize subscription data for topic: " + topic.toStringUtf8()
                                                 + " subscriberId: " + subscriberId.toStringUtf8();
                                    logger.error(msg, ex);
                                    reportFailure(new PubSubException.UnexpectedConditionException(msg));
                                    return;
                                }

                                if (logger.isDebugEnabled()) {
                                    logger.debug("Found subscription while acquiring topic: " + topic.toStringUtf8()
                                                 + " subscriberId: " + child + "state: "
                                                 + SubscriptionStateUtils.toString(subData.getValue()));
                                }

                                topicSubs.put(subscriberId, subData);
                                if (count.incrementAndGet() == children.size()) {
                                    assert topicSubs.size() == count.get();
                                    cb.operationFinished(ctx, topicSubs);
                                }
                            }

                            private void reportFailure(PubSubException e) {
                                if (failed.compareAndSet(false, true))
                                    cb.operationFailed(ctx, e);
                            }
                        }, ctx);
                    }
                }
            }, ctx);
        }
    }

    /**
     * ZooKeeper base topic ownership manager.
     */
    static class ZkTopicOwnershipManagerImpl implements TopicOwnershipManager {

        ZooKeeper zk;
        ServerConfiguration cfg;

        ZkTopicOwnershipManagerImpl(ServerConfiguration conf, ZooKeeper zk) {
            this.cfg = conf;
            this.zk = zk;
        }

        @Override
        public void close() throws IOException {
            // do nothing in zookeeper based impl
        }

        /**
         * Return znode path to store topic owner.
         *
         * @param topic
         *          Topic Name
         * @return znode path to store topic owner.
         */
        String hubPath(ByteString topic) {
            return cfg.getZkTopicPath(new StringBuilder(), topic).append("/hub").toString();
        }

        @Override
        public void readOwnerInfo(final ByteString topic, final Callback<Versioned<HubInfo>> callback, Object ctx) {
            String ownerPath = hubPath(topic);
            zk.getData(ownerPath, false, new SafeAsyncZKCallback.DataCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    if (Code.NONODE.intValue() == rc) {
                        callback.operationFinished(ctx, null);
                        return;
                    }

                    if (Code.OK.intValue() != rc) {
                        KeeperException e = ZkUtils.logErrorAndCreateZKException("Could not read ownership for topic: "
                                            + topic.toStringUtf8(), path, rc);
                        callback.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                        return;
                    }
                    HubInfo owner = null;
                    try {
                        owner = HubInfo.parse(new String(data, UTF_8));
                    } catch (HubInfo.InvalidHubInfoException ihie) {
                        logger.warn("Failed to parse hub info for topic " + topic.toStringUtf8() + " : ", ihie);
                    }
                    int version = stat.getVersion();
                    callback.operationFinished(ctx, new Versioned<HubInfo>(owner, new ZkVersion(version)));
                    return;
                }
            }, ctx);
        }

        @Override
        public void writeOwnerInfo(final ByteString topic, final HubInfo owner, final Version version,
                                   final Callback<Version> callback, Object ctx) {
            if (Version.NEW == version) {
                createOwnerInfo(topic, owner, callback, ctx);
                return;
            }

            if (!(version instanceof ZkVersion)) {
                callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(
                                              "Invalid version provided to update owner info for topic " + topic.toStringUtf8()));
                return;
            }

            int znodeVersion = ((ZkVersion)version).getZnodeVersion();
            zk.setData(hubPath(topic), owner.toString().getBytes(UTF_8), znodeVersion,
                       new SafeAsyncZKCallback.StatCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx, Stat stat) {
                    if (rc == Code.NONODE.intValue()) {
                        // no node
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_TOPIC_OWNER_INFO,
                                                      "No owner info found for topic " + topic.toStringUtf8()));
                        return;
                    } else if (rc == Code.BADVERSION.intValue()) {
                        // bad version
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                                      "Bad version provided to update owner info of topic " + topic.toStringUtf8()));
                        return;
                    } else if (Code.OK.intValue() == rc) {
                        callback.operationFinished(ctx, new ZkVersion(stat.getVersion()));
                        return;
                    } else {
                        KeeperException e = ZkUtils.logErrorAndCreateZKException(
                            "Failed to update ownership of topic " + topic.toStringUtf8() +
                            " to " + owner, path, rc);
                        callback.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                        return;
                    }
                }
            }, ctx);
        }

        protected void createOwnerInfo(final ByteString topic, final HubInfo owner,
                                       final Callback<Version> callback, Object ctx) {
            String ownerPath = hubPath(topic);
            ZkUtils.createFullPathOptimistic(zk, ownerPath, owner.toString().getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE,
                                             CreateMode.PERSISTENT, new SafeAsyncZKCallback.StringCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx, String name) {
                    if (Code.OK.intValue() == rc) {
                        // assume the initial version is 0
                        callback.operationFinished(ctx, new ZkVersion(0));
                        return;
                    } else if (Code.NODEEXISTS.intValue() == rc) {
                        // node existed
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.TOPIC_OWNER_INFO_EXISTS,
                                                      "Owner info of topic " + topic.toStringUtf8() + " existed."));
                        return;
                    } else {
                        KeeperException e = ZkUtils.logErrorAndCreateZKException(
                                                "Failed to create znode for ownership of topic: "
                                                + topic.toStringUtf8(), path, rc);
                        callback.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                        return;
                    }
                }
            }, ctx);
        }

        @Override
        public void deleteOwnerInfo(final ByteString topic, final Version version,
                                    final Callback<Void> callback, Object ctx) {
            int znodeVersion = -1;
            if (Version.ANY != version) {
                if (!(version instanceof ZkVersion)) {
                    callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(
                                                  "Invalid version provided to delete owner info for topic " + topic.toStringUtf8()));
                    return;
                } else {
                    znodeVersion = ((ZkVersion)version).getZnodeVersion();
                }
            }

            zk.delete(hubPath(topic), znodeVersion, new SafeAsyncZKCallback.VoidCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx) {
                    if (Code.OK.intValue() == rc) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully deleted owner info for topic " + topic.toStringUtf8() + ".");
                        }
                        callback.operationFinished(ctx, null);
                        return;
                    } else if (Code.NONODE.intValue() == rc) {
                        // no node
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_TOPIC_OWNER_INFO,
                                                      "No owner info found for topic " + topic.toStringUtf8()));
                        return;
                    } else if (Code.BADVERSION.intValue() == rc) {
                        // bad version
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                                      "Bad version provided to delete owner info of topic " + topic.toStringUtf8()));
                        return;
                    } else {
                        KeeperException e = ZkUtils.logErrorAndCreateZKException(
                                                "Failed to delete owner info for topic "
                                                + topic.toStringUtf8(), path, rc);
                        callback.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                    }
                }
            }, ctx);
        }
    }

    @Override
    public void format(ServerConfiguration cfg, ZooKeeper zk) throws IOException {
        try {
            ZKUtil.deleteRecursive(zk, cfg.getZkTopicsPrefix(new StringBuilder()).toString());
        } catch (KeeperException.NoNodeException e) {
            logger.debug("Hedwig root node doesn't exist in zookeeper to delete");
        } catch (KeeperException ke) {
            throw new IOException(ke);
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }
}
