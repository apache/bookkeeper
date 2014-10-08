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
package org.apache.hedwig.client.netty.impl.multiplex;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.netty.HChannelManager;
import org.apache.hedwig.client.netty.HChannel;
import org.apache.hedwig.client.netty.NetUtils;
import org.apache.hedwig.client.netty.impl.AbstractSubscribeResponseHandler;
import org.apache.hedwig.client.netty.impl.ActiveSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.UnexpectedConditionException;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.Either;
import static org.apache.hedwig.util.VarArgs.va;

public class MultiplexSubscribeResponseHandler extends AbstractSubscribeResponseHandler {

    private static Logger logger =
        LoggerFactory.getLogger(MultiplexSubscribeResponseHandler.class);

    // the underlying subscription channel
    volatile HChannel hChannel;
    private final MultiplexHChannelManager sChannelManager;

    protected MultiplexSubscribeResponseHandler(ClientConfiguration cfg,
                                                HChannelManager channelManager) {
        super(cfg, channelManager);
        sChannelManager = (MultiplexHChannelManager) channelManager;
    }

    @Override
    public void handleResponse(PubSubResponse response, PubSubData pubSubData,
                               Channel channel) throws Exception {
        if (null == hChannel) {
            InetSocketAddress host = NetUtils.getHostFromChannel(channel);
            hChannel = sChannelManager.getSubscriptionChannel(host);
            if (null == hChannel ||
                !channel.equals(hChannel.getChannel())) {
                PubSubException pse =
                    new UnexpectedConditionException("Failed to get subscription channel of " + host);
                pubSubData.getCallback().operationFailed(pubSubData.context, pse);
                return;
            }
        }
        super.handleResponse(response, pubSubData, channel);
    }

    @Override
    protected Either<StatusCode, HChannel> handleSuccessResponse(
        TopicSubscriber ts, PubSubData pubSubData, Channel channel) {
        // Store the mapping for the TopicSubscriber to the Channel.
        // This is so we can control the starting and stopping of
        // message deliveries from the server on that Channel. Store
        // this only on a successful ack response from the server.
        Either<Boolean, HChannel> result =
            sChannelManager.storeSubscriptionChannel(ts, pubSubData, hChannel);
        if (result.left()) {
            return Either.of(StatusCode.SUCCESS, result.right());
        } else {
            StatusCode code;
            if (pubSubData.isResubscribeRequest()) {
                code = StatusCode.RESUBSCRIBE_EXCEPTION;
            } else {
                code = StatusCode.CLIENT_ALREADY_SUBSCRIBED;
            }
            return Either.of(code, null);
        }
    }

    @Override
    public void asyncCloseSubscription(final TopicSubscriber topicSubscriber,
                                       final Callback<ResponseBody> callback,
                                       final Object context) {
        final ActiveSubscriber ss = getActiveSubscriber(topicSubscriber);
        if (null == ss || null == hChannel) {
            logger.debug("No subscription {} found when closing its subscription from {}.",
                         va(topicSubscriber, hChannel));
            callback.operationFinished(context, (ResponseBody)null);
            return;
        }
        Callback<ResponseBody> closeCb = new Callback<ResponseBody>() {
            @Override
            public void operationFinished(Object ctx, ResponseBody respBody) {
                removeSubscription(topicSubscriber, ss);
                sChannelManager.removeSubscriptionChannel(topicSubscriber, hChannel);
                callback.operationFinished(context, null);
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                callback.operationFailed(context, exception);
            }
        };
        PubSubData closeOp = new PubSubData(topicSubscriber.getTopic(), null,
                                            topicSubscriber.getSubscriberId(),
                                            OperationType.CLOSESUBSCRIPTION,
                                            null, closeCb, context);
        hChannel.submitOp(closeOp);
    }

}
