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
package org.apache.hedwig.client.netty.impl;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import com.google.protobuf.ByteString;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.NoResponseHandlerException;
import org.apache.hedwig.client.handlers.MessageConsumeCallback;
import org.apache.hedwig.client.handlers.SubscribeResponseHandler;
import org.apache.hedwig.client.netty.CleanupChannelMap;
import org.apache.hedwig.client.netty.HChannel;
import org.apache.hedwig.client.netty.HChannelManager;
import org.apache.hedwig.client.netty.NetUtils;
import org.apache.hedwig.client.netty.SubscriptionEventEmitter;
import org.apache.hedwig.client.ssl.SslClientContextFactory;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.filter.ClientMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageHeader;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.util.Callback;
import static org.apache.hedwig.util.VarArgs.va;

/**
 * Basic HChannel Manager Implementation
 */
public abstract class AbstractHChannelManager implements HChannelManager {

    private static Logger logger = LoggerFactory.getLogger(AbstractHChannelManager.class);

    // Empty Topic List
    private final static Set<ByteString> EMPTY_TOPIC_SET =
        new HashSet<ByteString>();

    // Boolean indicating if the channel manager is running or has been closed.
    // Once we stop the manager, we should sidestep all of the connect, write callback
    // and channel disconnected logic.
    protected boolean closed = false;
    protected final ReentrantReadWriteLock closedLock =
        new ReentrantReadWriteLock();

    // Global counter used for generating unique transaction ID's for
    // publish and subscribe requests
    protected final AtomicLong globalCounter = new AtomicLong();

    // Concurrent Map to store the mapping from the Topic to the Host.
    // This could change over time since servers can drop mastership of topics
    // for load balancing or failover. If a server host ever goes down, we'd
    // also want to remove all topic mappings the host was responsible for.
    // The second Map is used as the inverted version of the first one.
    protected final ConcurrentMap<ByteString, InetSocketAddress> topic2Host =
        new ConcurrentHashMap<ByteString, InetSocketAddress>();
    // The inverse mapping is used only when clearing all topics. For performance
    // consideration, we don't guarantee host2Topics to be consistent with
    // topic2Host. it would be better to not rely on this mapping for anything
    // significant.
    protected final ConcurrentMap<InetSocketAddress, Set<ByteString>> host2Topics =
        new ConcurrentHashMap<InetSocketAddress, Set<ByteString>>();

    // This channels will be used for publish and unsubscribe requests
    protected final CleanupChannelMap<InetSocketAddress> host2NonSubscriptionChannels =
        new CleanupChannelMap<InetSocketAddress>();

    private final ClientConfiguration cfg;
    // The Netty socket factory for making connections to the server.
    protected final ChannelFactory socketFactory;
    // PipelineFactory to create non-subscription netty channels to the appropriate server
    private final ClientChannelPipelineFactory nonSubscriptionChannelPipelineFactory;
    // ssl context factory
    private SslClientContextFactory sslFactory = null;

    // default server channel
    private final HChannel defaultServerChannel;

    // Each client instantiation will have a Timer for running recurring
    // threads. One such timer task thread to is to timeout long running
    // PubSubRequests that are waiting for an ack response from the server.
    private final Timer clientTimer = new Timer(true);
    // a common consume callback for all consume requests.
    private final MessageConsumeCallback consumeCb;
    // A event emitter to emit subscription events
    private final SubscriptionEventEmitter eventEmitter;

    protected AbstractHChannelManager(ClientConfiguration cfg,
                                      ChannelFactory socketFactory) {
        this.cfg = cfg;
        this.socketFactory = socketFactory;
        this.nonSubscriptionChannelPipelineFactory =
            new NonSubscriptionChannelPipelineFactory(cfg, this);

        // create a default server channel
        defaultServerChannel =
            new DefaultServerChannel(cfg.getDefaultServerHost(), this);

        if (cfg.isSSLEnabled()) {
            sslFactory = new SslClientContextFactory(cfg);
        }

        consumeCb = new MessageConsumeCallback(cfg, this);
        eventEmitter = new SubscriptionEventEmitter();

        // Schedule Request Timeout task.
        clientTimer.schedule(new PubSubRequestTimeoutTask(), 0,
                             cfg.getTimeoutThreadRunInterval());
    }

    @Override
    public SubscriptionEventEmitter getSubscriptionEventEmitter() {
        return eventEmitter;
    }

    public MessageConsumeCallback getConsumeCallback() {
        return consumeCb;
    }

    public SslClientContextFactory getSslFactory() {
        return sslFactory;
    }

    protected ChannelFactory getChannelFactory() {
        return socketFactory;
    }

    protected ClientChannelPipelineFactory getNonSubscriptionChannelPipelineFactory() {
        return this.nonSubscriptionChannelPipelineFactory;
    }

    protected abstract ClientChannelPipelineFactory getSubscriptionChannelPipelineFactory();

    @Override
    public void schedule(final TimerTask task, final long delay) {
        this.closedLock.readLock().lock();
        try {
            if (closed) {
                logger.warn("Task {} is not scheduled due to the channel manager is closed.",
                            task);
                return;
            }
            clientTimer.schedule(task, delay);
        } finally {
            this.closedLock.readLock().unlock();
        }
    }

    @Override
    public void submitOpAfterDelay(final PubSubData pubSubData, final long delay) {
        this.closedLock.readLock().lock();
        try {
            if (closed) {
                pubSubData.getCallback().operationFailed(pubSubData.context,
                    new ServiceDownException("Client has been closed."));
                return;
            }
            clientTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    logger.debug("Submit request {} in {} ms later.",
                                 va(pubSubData, delay));
                    submitOp(pubSubData);
                }
            }, delay);
        } finally {
            closedLock.readLock().unlock();
        }
    }

    @Override
    public void submitOp(PubSubData pubSubData) {
        HChannel hChannel;
        if (OperationType.PUBLISH.equals(pubSubData.operationType) ||
            OperationType.UNSUBSCRIBE.equals(pubSubData.operationType)) {
            hChannel = getNonSubscriptionChannelByTopic(pubSubData.topic);
        } else {
            TopicSubscriber ts = new TopicSubscriber(pubSubData.topic,
                                                     pubSubData.subscriberId);
            hChannel = getSubscriptionChannelByTopicSubscriber(ts);
        }
        // no channel found to submit pubsub data
        // choose the default server
        if (null == hChannel) {
            hChannel = defaultServerChannel;
        }
        hChannel.submitOp(pubSubData);
    }

    @Override
    public void redirectToHost(PubSubData pubSubData, InetSocketAddress host) {
        logger.debug("Submit operation {} to host {}.",
                     va(pubSubData, host));
        HChannel hChannel;
        if (OperationType.PUBLISH.equals(pubSubData.operationType) ||
            OperationType.UNSUBSCRIBE.equals(pubSubData.operationType)) {
            hChannel = getNonSubscriptionChannel(host);
            if (null == hChannel) {
                // create a channel to connect to specified host
                hChannel = createAndStoreNonSubscriptionChannel(host);
            }
        } else {
            hChannel = getSubscriptionChannel(host);
            if (null == hChannel) {
                // create a subscription channel to specified host
                hChannel = createAndStoreSubscriptionChannel(host);
            }
        }
        // no channel found to submit pubsub data
        // choose the default server
        if (null == hChannel) {
            hChannel = defaultServerChannel;
        }
        hChannel.submitOp(pubSubData);
    }

    void submitOpThruChannel(PubSubData pubSubData, Channel channel) {
        logger.debug("Submit operation {} to thru channel {}.",
                     va(pubSubData, channel));
        HChannel hChannel;
        if (OperationType.PUBLISH.equals(pubSubData.operationType) ||
            OperationType.UNSUBSCRIBE.equals(pubSubData.operationType)) {
            hChannel = createAndStoreNonSubscriptionChannel(channel);
        } else {
            hChannel = createAndStoreSubscriptionChannel(channel);
        }
        hChannel.submitOp(pubSubData);
    }

    @Override
    public void submitOpToDefaultServer(PubSubData pubSubData) {
        logger.debug("Submit operation {} to default server {}.",
                     va(pubSubData, defaultServerChannel));
        defaultServerChannel.submitOp(pubSubData);
    }

    // Synchronized method to store the host2Channel mapping (if it doesn't
    // exist yet). Retrieve the hostname info from the Channel created via the
    // RemoteAddress tied to it.
    private HChannel createAndStoreNonSubscriptionChannel(Channel channel) {
        InetSocketAddress host = NetUtils.getHostFromChannel(channel);
        HChannel newHChannel = new HChannelImpl(host, channel, this,
                                                getNonSubscriptionChannelPipelineFactory());
        return storeNonSubscriptionChannel(host, newHChannel);
    }

    private HChannel createAndStoreNonSubscriptionChannel(InetSocketAddress host) {
        HChannel newHChannel = new HChannelImpl(host, this,
                                                getNonSubscriptionChannelPipelineFactory());
        return storeNonSubscriptionChannel(host, newHChannel);
    }

    private HChannel storeNonSubscriptionChannel(InetSocketAddress host,
                                                 HChannel newHChannel) {
        return host2NonSubscriptionChannels.addChannel(host, newHChannel);
    }

    /**
     * Is there a {@link HChannel} existed for a given host.
     *
     * @param host
     *          Target host address.
     */
    private HChannel getNonSubscriptionChannel(InetSocketAddress host) {
        return host2NonSubscriptionChannels.getChannel(host);
    }

    /**
     * Get a non-subscription channel for a given <code>topic</code>.
     *
     * @param topic
     *          Topic Name
     * @return if <code>topic</code>'s owner is unknown, return null.
     *         if <code>topic</code>'s owner is know and there is a channel
     *         existed before, return the existed channel, otherwise created
     *         a new one.
     */
    private HChannel getNonSubscriptionChannelByTopic(ByteString topic) {
        InetSocketAddress host = topic2Host.get(topic);
        if (null == host) {
            // we don't know where is the topic
            return null;
        } else {
            // we had know which server owned the topic
            HChannel channel = getNonSubscriptionChannel(host);
            if (null == channel) {
                // create a channel to connect to specified host
                channel = createAndStoreNonSubscriptionChannel(host);
            }
            return channel;
        }
    }

    /**
     * Handle the disconnected event from a non-subscription {@link HChannel}.
     *
     * @param host
     *          Which host is disconnected.
     * @param channel
     *          The underlying established channel.
     */
    protected void onNonSubscriptionChannelDisconnected(InetSocketAddress host,
                                                        Channel channel) {
        // Only remove the Channel from the mapping if this current
        // disconnected channel is the same as the cached entry.
        // Due to race concurrency situations, it is possible to
        // create multiple channels to the same host for publish
        // and unsubscribe requests.
        HChannel hChannel = host2NonSubscriptionChannels.getChannel(host);
        if (null == hChannel) {
            return;
        }
        Channel underlyingChannel = hChannel.getChannel();
        if (null == underlyingChannel ||
            !underlyingChannel.equals(channel)) {
            return;
        }
        logger.info("NonSubscription Channel {} to {} disconnected.",
                    va(channel, host));
        // remove existed channel
        if (host2NonSubscriptionChannels.removeChannel(host, hChannel)) {
            clearAllTopicsForHost(host);
        }
    }

    /**
     * Create and store a subscription {@link HChannel} thru the underlying established
     * <code>channel</code>
     *
     * @param channel
     *          The underlying established subscription channel.
     */
    protected abstract HChannel createAndStoreSubscriptionChannel(Channel channel);

    /**
     * Create and store a subscription {@link HChannel} to target host.
     *
     * @param host
     *          Target host address.
     */
    protected abstract HChannel createAndStoreSubscriptionChannel(InetSocketAddress host);

    /**
     * Is there a subscription {@link HChannel} existed for a given host.
     *
     * @param host
     *          Target host address.
     */
    protected abstract HChannel getSubscriptionChannel(InetSocketAddress host);

    /**
     * Get a subscription channel for a given <code>topicSubscriber</code>.
     *
     * @param topicSubscriber
     *          Topic Subscriber
     * @return if <code>topic</code>'s owner is unknown, return null.
     *         if <code>topic</code>'s owner is know and there is a channel
     *         existed before, return the existed channel, otherwise created
     *         a new one for the <code>topicSubscriber</code>.
     */
    protected abstract HChannel getSubscriptionChannelByTopicSubscriber(TopicSubscriber topicSubscriber);

    /**
     * Handle the disconnected event from a subscription {@link HChannel}.
     *
     * @param host
     *          Which host is disconnected.
     * @param channel
     *          The underlying established channel.
     */
    protected abstract void onSubscriptionChannelDisconnected(InetSocketAddress host,
                                                              Channel channel);

    private void sendConsumeRequest(final TopicSubscriber topicSubscriber,
                                    final MessageSeqId messageSeqId,
                                    final Channel channel) {
        PubSubRequest.Builder pubsubRequestBuilder =
            NetUtils.buildConsumeRequest(nextTxnId(), topicSubscriber, messageSeqId);  

        // For Consume requests, we will send them from the client in a fire and
        // forget manner. We are not expecting the server to send back an ack
        // response so no need to register this in the ResponseHandler. There
        // are no callbacks to invoke since this isn't a client initiated
        // action. Instead, just have a future listener that will log an error
        // message if there was a problem writing the consume request.
        logger.debug("Writing a Consume request to host: {} with messageSeqId: {} for {}",
                     va(NetUtils.getHostFromChannel(channel), messageSeqId, topicSubscriber));
        ChannelFuture future = channel.write(pubsubRequestBuilder.build());
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    logger.error("Error writing a Consume request to host: {} with messageSeqId: {} for {}",
                                 va(NetUtils.getHostFromChannel(channel),
                                    messageSeqId, topicSubscriber));
                }
            }
        });
    }

    /**
     * Helper method to store the topic2Host mapping in the channel manager cache
     * map. This method is assumed to be called when we've done a successful
     * connection to the correct server topic master.
     *
     * @param topic
     *            Topic Name
     * @param host
     *            Host Address
     */
    protected void storeTopic2HostMapping(ByteString topic, InetSocketAddress host) {
        InetSocketAddress oldHost = topic2Host.putIfAbsent(topic, host);
        if (null != oldHost && oldHost.equals(host)) {
            // Entry in map exists for the topic but it is the same as the
            // current host. In this case there is nothing to do.
            return;
        }

        if (null != oldHost) {
            if (topic2Host.replace(topic, oldHost, host)) {
                // Store the relevant mappings for this topic and host combination.
                logger.debug("Storing info for topic: {}, old host: {}, new host: {}.",
                             va(topic.toStringUtf8(), oldHost, host));
                clearHostForTopic(topic, oldHost);
            } else {
                logger.warn("Ownership of topic: {} has been changed from {} to {} when storeing host: {}",
                            va(topic.toStringUtf8(), oldHost, topic2Host.get(topic), host));
                return;
            }
        } else {
            logger.debug("Storing info for topic: {}, host: {}.",
                         va(topic.toStringUtf8(), host));
        }
        Set<ByteString> topicsForHost = host2Topics.get(host);
        if (null == topicsForHost) {
            Set<ByteString> newTopicsSet = new HashSet<ByteString>();
            topicsForHost = host2Topics.putIfAbsent(host, newTopicsSet);
            if (null == topicsForHost) {
              topicsForHost = newTopicsSet;
            }
        }
        synchronized (topicsForHost) {
            // check whether the ownership changed, since it might happened
            // after replace succeed
            if (host.equals(topic2Host.get(topic))) {
                topicsForHost.add(topic);
            }
        }
    }

    // If a server host goes down or the channel to it gets disconnected,
    // we want to clear out all relevant cached information. We'll
    // need to remove all of the topic mappings that the host was
    // responsible for.
    protected void clearAllTopicsForHost(InetSocketAddress host) {
        logger.debug("Clearing all topics for host: {}", host);
        // For each of the topics that the host was responsible for,
        // remove it from the topic2Host mapping.
        Set<ByteString> topicsForHost = host2Topics.get(host);
        if (null != topicsForHost) {
            synchronized (topicsForHost) {
                for (ByteString topic : topicsForHost) {
                    logger.debug("Removing mapping for topic: {} from host: {}.",
                                 va(topic.toStringUtf8(), host));
                    topic2Host.remove(topic, host);
                }
            }
            // Now it is safe to remove the host2Topics mapping entry.
            host2Topics.remove(host, topicsForHost);
        }
    }

    // If a subscribe channel goes down, the topic might have moved.
    // We only clear out that topic for the host and not all cached information.
    public void clearHostForTopic(ByteString topic, InetSocketAddress host) {
        logger.debug("Clearing topic: {} from host: {}.",
                     va(topic.toStringUtf8(), host));
        if (topic2Host.remove(topic, host)) {
            logger.debug("Removed topic to host mapping for topic: {} and host: {}.",
                         va(topic.toStringUtf8(), host));
        }
        Set<ByteString> topicsForHost = host2Topics.get(host);
        if (null != topicsForHost) {
            boolean removed;
            synchronized (topicsForHost) {
                removed = topicsForHost.remove(topic);
            }
            if (removed) {
                logger.debug("Removed topic: {} from host: {}.",
                             topic.toStringUtf8(), host);
                if (topicsForHost.isEmpty()) {
                    // remove only topic list is empty
                    host2Topics.remove(host, EMPTY_TOPIC_SET);
                }
            }
        }
    }

    @Override
    public long nextTxnId() {
        return globalCounter.incrementAndGet();
    }

    // We need to deal with the possible problem of a PubSub request being
    // written to successfully to the server host but for some reason, the
    // ack message back never comes. What could happen is that the VoidCallback
    // stored in the ResponseHandler.txn2PublishData map will never be called.
    // We should have a configured timeout so if that passes from the time a
    // write was successfully done to the server, we can fail this async PubSub
    // transaction. The caller could possibly redo the transaction if needed at
    // a later time. Creating a timeout cleaner TimerTask to do this here.
    class PubSubRequestTimeoutTask extends TimerTask {
        /**
         * Implement the TimerTask's abstract run method.
         */
        @Override
        public void run() {
            if (isClosed()) {
                return;
            }
            logger.debug("Running the PubSubRequest Timeout Task");
            // First check those non-subscription channels
            for (HChannel channel : host2NonSubscriptionChannels.getChannels()) {
                try {
                    HChannelHandler channelHandler =
                        HChannelImpl.getHChannelHandlerFromChannel(channel.getChannel());
                    channelHandler.checkTimeoutRequests();
                } catch (NoResponseHandlerException nrhe) {
                    continue;
                }
            }
            // Then check those subscription channels
            checkTimeoutRequestsOnSubscriptionChannels();
        }
    }

    protected abstract void restartDelivery(TopicSubscriber topicSubscriber)
        throws ClientNotSubscribedException, AlreadyStartDeliveryException;

    /**
     * Chekout the pub/sub requests on subscription channels.
     */
    protected abstract void checkTimeoutRequestsOnSubscriptionChannels();

    @Override
    public boolean isClosed() {
        closedLock.readLock().lock();
        try {
            return closed; 
        } finally {
            closedLock.readLock().unlock();
        }
    }

    /**
     * Close all subscription channels when close channel manager.
     */
    protected abstract void closeSubscriptionChannels();

    @Override
    public void close() {
        logger.info("Shutting down the channels manager.");
        closedLock.writeLock().lock();
        try {
            // Not first time to close
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closedLock.writeLock().unlock();
        }
        clientTimer.cancel();
        // Clear all existed channels
        host2NonSubscriptionChannels.close();

        // clear all subscription channels
        closeSubscriptionChannels();

        // Clear out all Maps
        topic2Host.clear();
        host2Topics.clear();
    }

}
