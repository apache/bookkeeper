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
package org.apache.hedwig.server.regions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooKeeper;

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.netty.HedwigSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.RegionSpecificSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.TopicOpQueuer;
import org.apache.hedwig.server.persistence.PersistRequest;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.subscriptions.SubscriptionEventListener;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.CallbackUtils;
import org.apache.hedwig.util.HedwigSocketAddress;

public class RegionManager implements SubscriptionEventListener {

    protected static final Logger LOGGER = LoggerFactory.getLogger(RegionManager.class);

    private final ByteString mySubId;
    private final PersistenceManager pm;
    private final ArrayList<HedwigHubClient> clients = new ArrayList<HedwigHubClient>();
    private final TopicOpQueuer queue;
    private final String myRegion;
    // Timer for running a retry thread task to retry remote-subscription in asynchronous mode.
    private final Timer timer = new Timer(true);
    private final HashMap<HedwigHubClient, Set<ByteString>> retryMap =
            new HashMap<HedwigHubClient, Set<ByteString>>();
    // map used to track whether a topic is remote subscribed or not
    private final ConcurrentMap<ByteString, Boolean> topicStatuses =
            new ConcurrentHashMap<ByteString, Boolean>();

    /**
     * This is the Timer Task for retrying subscribing to remote regions
     */
    class RetrySubscribeTask extends TimerTask {

        @Override
        public void run() {
            Set<HedwigHubClient> hubClients = new HashSet<HedwigHubClient>();
            synchronized (retryMap) {
                hubClients.addAll(retryMap.keySet());
            }
            if (hubClients.isEmpty()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[" + myRegion + "] There is no hub client needs to retry subscriptions.");
                }
                return;
            }
            for (HedwigHubClient client : hubClients) {
                Set<ByteString> topics = null;
                synchronized (retryMap) {
                    topics = retryMap.remove(client);
                }
                if (null == topics || topics.isEmpty()) {
                    continue;
                }
                final CountDownLatch done = new CountDownLatch(1);
                Callback<Void> postCb = new Callback<Void>() {
                    @Override
                    public void operationFinished(Object ctx,
                            Void resultOfOperation) {
                        finish();
                    }
                    @Override
                    public void operationFailed(Object ctx,
                            PubSubException exception) {
                        finish();
                    }
                    void finish() {
                        done.countDown();
                    }
                };
                Callback<Void> mcb = CallbackUtils.multiCallback(topics.size(), postCb, null);
                for (ByteString topic : topics) {
                    Boolean doRemoteSubscribe = topicStatuses.get(topic);
                    // topic has been removed, no retry again
                    if (null == doRemoteSubscribe) {
                        mcb.operationFinished(null, null);
                        continue;
                    }
                    retrySubscribe(client, topic, mcb);
                }
                try {
                    done.await();
                } catch (InterruptedException e) {
                    LOGGER.warn("Exception during retrying remote subscriptions : ", e);
                }
            }
        }

    }

    public RegionManager(final PersistenceManager pm, final ServerConfiguration cfg, final ZooKeeper zk,
                         ScheduledExecutorService scheduler, HedwigHubClientFactory hubClientFactory) {
        this.pm = pm;
        mySubId = ByteString.copyFromUtf8(SubscriptionStateUtils.HUB_SUBSCRIBER_PREFIX + cfg.getMyRegion());
        queue = new TopicOpQueuer(scheduler);
        for (final String hub : cfg.getRegions()) {
            clients.add(hubClientFactory.create(new HedwigSocketAddress(hub)));
        }
        myRegion = cfg.getMyRegionByteString().toStringUtf8();
        if (cfg.getRetryRemoteSubscribeThreadRunInterval() > 0) {
            timer.schedule(new RetrySubscribeTask(), 0, cfg.getRetryRemoteSubscribeThreadRunInterval());
        }
    }

    private void putTopicInRetryMap(HedwigHubClient client, ByteString topic) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[" + myRegion + "] Put topic in retry map : " + topic.toStringUtf8());
        }
        synchronized (retryMap) {
            Set<ByteString> topics = retryMap.get(client);
            if (null == topics) {
                topics = new HashSet<ByteString>();
                retryMap.put(client, topics);
            }
            topics.add(topic);
        }
    }
    
    /**
     * Do remote subscribe for a specified topic.
     *
     * @param client
     *          Hedwig Hub Client to subscribe remote topic.
     * @param topic
     *          Topic to subscribe.
     * @param synchronous
     *          Whether to wait for the callback of subscription.
     * @param mcb
     *          Callback to trigger after subscription is done.
     * @param contex
     *          Callback context
     */
    private void doRemoteSubscribe(final HedwigHubClient client, final ByteString topic, final boolean synchronous,
                                   final Callback<Void> mcb, final Object context) {
        final HedwigSubscriber sub = client.getSubscriber();
        try {
            if (sub.hasSubscription(topic, mySubId)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[" + myRegion + "] cross-region subscription for topic "
                                 + topic.toStringUtf8() + " has existed before.");
                }
                mcb.operationFinished(null, null);
                return;
            }
        } catch (PubSubException e) {
            LOGGER.error("[" + myRegion + "] checking cross-region subscription for topic "
                         + topic.toStringUtf8() + " failed (this is should not happen): ", e);
            mcb.operationFailed(context, e);
            return;
        }
        sub.asyncSubscribe(topic, mySubId, CreateOrAttach.CREATE_OR_ATTACH, new Callback<Void>() {
            @Override
            public void operationFinished(Object ctx, Void resultOfOperation) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("[" + myRegion + "] cross-region subscription done for topic " + topic.toStringUtf8());
                try {
                    sub.startDelivery(topic, mySubId, new MessageHandler() {
                        @Override
                        public void deliver(final ByteString topic, ByteString subscriberId, Message msg,
                        final Callback<Void> callback, final Object context) {
                            // When messages are first published
                            // locally, the PublishHandler sets the
                            // source region in the Message.
                            if (msg.hasSrcRegion()) {
                                Message.newBuilder(msg).setMsgId(
                                    MessageSeqId.newBuilder(msg.getMsgId()).addRemoteComponents(
                                        RegionSpecificSeqId.newBuilder().setRegion(
                                            msg.getSrcRegion()).setSeqId(
                                            msg.getMsgId().getLocalComponent())));
                            }
                            pm.persistMessage(new PersistRequest(topic, msg, new Callback<MessageSeqId>() {
                                @Override
                                public void operationFinished(Object ctx, MessageSeqId resultOfOperation) {
                                    if (LOGGER.isDebugEnabled())
                                        LOGGER.debug("[" + myRegion + "] cross-region recv-fwd succeeded for topic "
                                                     + topic.toStringUtf8());
                                    callback.operationFinished(context, null);
                                }

                                @Override
                                public void operationFailed(Object ctx, PubSubException exception) {
                                    if (LOGGER.isDebugEnabled())
                                        LOGGER.error("[" + myRegion + "] cross-region recv-fwd failed for topic "
                                                     + topic.toStringUtf8(), exception);
                                    callback.operationFailed(context, exception);
                                }
                            }, null));
                        }
                    });
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("[" + myRegion + "] cross-region start-delivery succeeded for topic "
                                     + topic.toStringUtf8());
                    mcb.operationFinished(ctx, null);
                } catch (PubSubException ex) {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.error(
                                "[" + myRegion + "] cross-region start-delivery failed for topic " + topic.toStringUtf8(), ex);
                    mcb.operationFailed(ctx, ex);
                } catch (AlreadyStartDeliveryException ex) {
                    LOGGER.error("[" + myRegion + "] cross-region start-delivery failed for topic "
                               + topic.toStringUtf8(), ex);
                    mcb.operationFailed(ctx, new PubSubException.UnexpectedConditionException("cross-region start-delivery failed : " + ex.getMessage()));
                }
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.error("[" + myRegion + "] cross-region subscribe failed for topic " + topic.toStringUtf8(),
                                 exception);
                if (!synchronous) {
                    putTopicInRetryMap(client, topic);
                }
                mcb.operationFailed(ctx, exception);
            }
        }, null);
    }

    private void retrySubscribe(final HedwigHubClient client, final ByteString topic, final Callback<Void> cb) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[" + myRegion + "] Retry remote subscribe topic : " + topic.toStringUtf8());
        }
        queue.pushAndMaybeRun(topic, queue.new AsynchronousOp<Void>(topic, cb, null) {
            @Override
            public void run() {
                Boolean doRemoteSubscribe = topicStatuses.get(topic);
                // topic has been removed, no retry again
                if (null == doRemoteSubscribe) {
                    cb.operationFinished(ctx, null);
                    return;
                }
                doRemoteSubscribe(client, topic, false, cb, ctx);
            }
        });
    }

    @Override
    public void onFirstLocalSubscribe(final ByteString topic, final boolean synchronous, final Callback<Void> cb) {
        topicStatuses.put(topic, true);
        // Whenever we acquire a topic due to a (local) subscribe, subscribe on
        // it to all the other regions (currently using simple all-to-all
        // topology).
        queue.pushAndMaybeRun(topic, queue.new AsynchronousOp<Void>(topic, cb, null) {
            @Override
            public void run() {
                Callback<Void> postCb = synchronous ? cb : CallbackUtils.logger(LOGGER, 
                        "[" + myRegion + "] all cross-region subscriptions succeeded", 
                        "[" + myRegion + "] at least one cross-region subscription failed");
                final Callback<Void> mcb = CallbackUtils.multiCallback(clients.size(), postCb, ctx);
                for (final HedwigHubClient client : clients) {
                    doRemoteSubscribe(client, topic, synchronous, mcb, ctx);
                }
                if (!synchronous)
                    cb.operationFinished(null, null);
            }
        });

    }

    @Override
    public void onLastLocalUnsubscribe(final ByteString topic) {
        topicStatuses.remove(topic);
        // TODO may want to ease up on the eager unsubscribe; this is dropping
        // cross-region subscriptions ASAP
        queue.pushAndMaybeRun(topic, queue.new AsynchronousOp<Void>(topic, new Callback<Void>() {

            @Override
            public void operationFinished(Object ctx, Void result) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("[" + myRegion + "] cross-region unsubscribes succeeded for topic " + topic.toStringUtf8());
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.error("[" + myRegion + "] cross-region unsubscribes failed for topic " + topic.toStringUtf8(), exception);
            }

        }, null) {
            @Override
            public void run() {
                Callback<Void> mcb = CallbackUtils.multiCallback(clients.size(), cb, ctx);
                for (final HedwigHubClient client : clients) {
                    final HedwigSubscriber sub = client.getSubscriber();
                    try {
                        if (!sub.hasSubscription(topic, mySubId)) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("[" + myRegion + "] cross-region subscription for topic "
                                             + topic.toStringUtf8() + " has existed before.");
                            }
                            mcb.operationFinished(null, null);
                            continue;
                        }
                    } catch (PubSubException e) {
                        LOGGER.error("[" + myRegion + "] checking cross-region subscription for topic "
                                     + topic.toStringUtf8() + " failed (this is should not happen): ", e);
                        mcb.operationFailed(ctx, e);
                        continue;
                    }
                    sub.asyncUnsubscribe(topic, mySubId, mcb, null);
                }
            }
        });
    }

    // Method to shutdown and stop all of the cross-region Hedwig clients.
    public void stop() {
        timer.cancel();
        for (HedwigHubClient client : clients) {
            client.close();
        }
    }

}
