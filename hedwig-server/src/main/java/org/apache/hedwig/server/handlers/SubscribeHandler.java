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
package org.apache.hedwig.server.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import com.google.protobuf.ByteString;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ServerNotResponsibleForTopicException;
import org.apache.hedwig.filter.PipelineFilter;
import org.apache.hedwig.filter.ServerMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.protoextensions.PubSubResponseUtils;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.delivery.ChannelEndPoint;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.netty.ServerStats;
import org.apache.hedwig.server.netty.ServerStats.OpStats;
import org.apache.hedwig.server.netty.UmbrellaHandler;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.subscriptions.SubscriptionManager;
import org.apache.hedwig.server.subscriptions.AllToAllTopologyFilter;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.util.Callback;
import static org.apache.hedwig.util.VarArgs.va;

public class SubscribeHandler extends BaseHandler {
    static Logger logger = LoggerFactory.getLogger(SubscribeHandler.class);

    private final DeliveryManager deliveryMgr;
    private final PersistenceManager persistenceMgr;
    private final SubscriptionManager subMgr;
    private final SubscriptionChannelManager subChannelMgr;

    // op stats
    private final OpStats subStats;

    public SubscribeHandler(ServerConfiguration cfg, TopicManager topicMgr,
                            DeliveryManager deliveryManager,
                            PersistenceManager persistenceMgr,
                            SubscriptionManager subMgr,
                            SubscriptionChannelManager subChannelMgr) {
        super(topicMgr, cfg);
        this.deliveryMgr = deliveryManager;
        this.persistenceMgr = persistenceMgr;
        this.subMgr = subMgr;
        this.subChannelMgr = subChannelMgr;
        subStats = ServerStats.getInstance().getOpStats(OperationType.SUBSCRIBE);
    }

    @Override
    public void handleRequestAtOwner(final PubSubRequest request, final Channel channel) {

        if (!request.hasSubscribeRequest()) {
            UmbrellaHandler.sendErrorResponseToMalformedRequest(channel, request.getTxnId(),
                    "Missing subscribe request data");
            subStats.incrementFailedOps();
            return;
        }

        final ByteString topic = request.getTopic();

        MessageSeqId seqId;
        try {
            seqId = persistenceMgr.getCurrentSeqIdForTopic(topic);
        } catch (ServerNotResponsibleForTopicException e) {
            channel.write(PubSubResponseUtils.getResponseForException(e, request.getTxnId())).addListener(
                ChannelFutureListener.CLOSE);
            logger.error("Error getting current seq id for topic " + topic.toStringUtf8()
                       + " when processing subscribe request (txnid:" + request.getTxnId() + ") :", e);
            subStats.incrementFailedOps();
            ServerStats.getInstance().incrementRequestsRedirect();
            return;
        }

        final SubscribeRequest subRequest = request.getSubscribeRequest();
        final ByteString subscriberId = subRequest.getSubscriberId();

        MessageSeqId lastSeqIdPublished = MessageSeqId.newBuilder(seqId).setLocalComponent(seqId.getLocalComponent()).build();

        final long requestTime = MathUtils.now();
        subMgr.serveSubscribeRequest(topic, subRequest, lastSeqIdPublished, new Callback<SubscriptionData>() {

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                channel.write(PubSubResponseUtils.getResponseForException(exception, request.getTxnId())).addListener(
                    ChannelFutureListener.CLOSE);
                logger.error("Error serving subscribe request (" + request.getTxnId() + ") for (topic: "
                           + topic.toStringUtf8() + " , subscriber: " + subscriberId.toStringUtf8() + ")", exception);
                subStats.incrementFailedOps();
            }

            @Override
            public void operationFinished(Object ctx, final SubscriptionData subData) {

                TopicSubscriber topicSub = new TopicSubscriber(topic, subscriberId);
                synchronized (channel) {
                    if (!channel.isConnected()) {
                        // channel got disconnected while we were processing the
                        // subscribe request,
                        // nothing much we can do in this case
                        subStats.incrementFailedOps();
                        return;
                    }
                }
                // initialize the message filter
                PipelineFilter filter = new PipelineFilter();
                try {
                    // the filter pipeline should be
                    // 1) AllToAllTopologyFilter to filter cross-region messages
                    filter.addLast(new AllToAllTopologyFilter());
                    // 2) User-Customized MessageFilter
                    if (subData.hasPreferences() &&
                        subData.getPreferences().hasMessageFilter()) {
                        String messageFilterName = subData.getPreferences().getMessageFilter();
                        filter.addLast(ReflectionUtils.newInstance(messageFilterName, ServerMessageFilter.class));
                    }
                    // initialize the filter
                    filter.initialize(cfg.getConf());
                    filter.setSubscriptionPreferences(topic, subscriberId,
                                                      subData.getPreferences());
                } catch (RuntimeException re) {
                    String errMsg = "RuntimeException caught when instantiating message filter for (topic:"
                                  + topic.toStringUtf8() + ", subscriber:" + subscriberId.toStringUtf8() + ")."
                                  + "It might be introduced by programming error in message filter.";
                    logger.error(errMsg, re);
                    PubSubException pse = new PubSubException.InvalidMessageFilterException(errMsg, re);
                    subStats.incrementFailedOps();
                    // we should not close the subscription channel, just response error
                    // client decide to close it or not.
                    channel.write(PubSubResponseUtils.getResponseForException(pse, request.getTxnId()));
                    return;
                } catch (Throwable t) {
                    String errMsg = "Failed to instantiate message filter for (topic:" + topic.toStringUtf8()
                                  + ", subscriber:" + subscriberId.toStringUtf8() + ").";
                    logger.error(errMsg, t);
                    PubSubException pse = new PubSubException.InvalidMessageFilterException(errMsg, t);
                    subStats.incrementFailedOps();
                    channel.write(PubSubResponseUtils.getResponseForException(pse, request.getTxnId()))
                    .addListener(ChannelFutureListener.CLOSE);
                    return;
                }
                boolean forceAttach = false;
                if (subRequest.hasForceAttach()) {
                    forceAttach = subRequest.getForceAttach();
                }
                // Try to store the subscription channel for the topic subscriber
                Channel oldChannel = subChannelMgr.put(topicSub, channel, forceAttach);
                if (null != oldChannel) {
                    PubSubException pse = new PubSubException.TopicBusyException(
                        "Subscriber " + subscriberId.toStringUtf8() + " for topic " + topic.toStringUtf8()
                        + " is already being served on a different channel " + oldChannel + ".");
                    subStats.incrementFailedOps();
                    channel.write(PubSubResponseUtils.getResponseForException(pse, request.getTxnId()))
                    .addListener(ChannelFutureListener.CLOSE);
                    return;
                }

                // want to start 1 ahead of the consume ptr
                MessageSeqId lastConsumedSeqId = subData.getState().getMsgId();
                MessageSeqId seqIdToStartFrom = MessageSeqId.newBuilder(lastConsumedSeqId).setLocalComponent(
                                                    lastConsumedSeqId.getLocalComponent() + 1).build();
                deliveryMgr.startServingSubscription(topic, subscriberId,
                        subData.getPreferences(), seqIdToStartFrom, new ChannelEndPoint(channel), filter,
                        new Callback<Void>() {
                            @Override
                            public void operationFinished(Object ctx, Void result) {
                                // First write success and then tell the delivery manager,
                                // otherwise the first message might go out before the response
                                // to the subscribe
                                SubscribeResponse.Builder subRespBuilder = SubscribeResponse.newBuilder()
                                    .setPreferences(subData.getPreferences());
                                ResponseBody respBody = ResponseBody.newBuilder()
                                    .setSubscribeResponse(subRespBuilder).build();
                                channel.write(PubSubResponseUtils.getSuccessResponse(request.getTxnId(), respBody));
                                logger.info("Subscribe request (" + request.getTxnId() + ") for (topic:"
                                            + topic.toStringUtf8() + ", subscriber:" + subscriberId.toStringUtf8()
                                            + ") from channel " + channel.getRemoteAddress()
                                            + " succeed - its subscription data is "
                                            + SubscriptionStateUtils.toString(subData));
                                subStats.updateLatency(MathUtils.now() - requestTime);
                            }
                            @Override
                            public void operationFailed(Object ctx, PubSubException exception) {
                                // would not happened
                            }
                        }, null);
            }
        }, null);

    }

}
