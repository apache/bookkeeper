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
package org.apache.hedwig.client.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.netty.HChannelManager;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.util.Callback;
import static org.apache.hedwig.util.VarArgs.va;

public class CloseSubscriptionResponseHandler extends AbstractResponseHandler {

    private static Logger logger =
        LoggerFactory.getLogger(CloseSubscriptionResponseHandler.class);

    public CloseSubscriptionResponseHandler(ClientConfiguration cfg,
                                            HChannelManager channelManager) {
        super(cfg, channelManager);
    }

    @Override
    public void handleResponse(final PubSubResponse response, final PubSubData pubSubData,
                               final Channel channel)
            throws Exception {
        switch (response.getStatusCode()) {
        case SUCCESS:
            pubSubData.getCallback().operationFinished(pubSubData.context, null);
            break;
        case CLIENT_NOT_SUBSCRIBED:
            // For closesubscription requests, the server says that the client was
            // never subscribed to the topic.
            pubSubData.getCallback().operationFailed(pubSubData.context, new ClientNotSubscribedException(
                                                    "Client was never subscribed to topic: " +
                                                        pubSubData.topic.toStringUtf8() + ", subscriberId: " +
                                                        pubSubData.subscriberId.toStringUtf8()));
            break;
        case SERVICE_DOWN:
            // Response was service down failure so just invoke the callback's
            // operationFailed method.
            pubSubData.getCallback().operationFailed(pubSubData.context, new ServiceDownException(
                                                    "Server responded with a SERVICE_DOWN status"));
            break;
        case NOT_RESPONSIBLE_FOR_TOPIC:
            // Redirect response so we'll need to repost the original
            // Unsubscribe Request
            handleRedirectResponse(response, pubSubData, channel);
            break;
        default:
            // Consider all other status codes as errors, operation failed
            // cases.
            logger.error("Unexpected error response from server for PubSubResponse: " + response);
            pubSubData.getCallback().operationFailed(pubSubData.context, new ServiceDownException(
                                                    "Server responded with a status code of: " +
                                                        response.getStatusCode()));
            break;
        }
    }

}
