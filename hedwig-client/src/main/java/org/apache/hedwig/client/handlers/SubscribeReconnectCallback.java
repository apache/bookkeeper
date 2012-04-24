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

import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.netty.HedwigClientImpl;
import org.apache.hedwig.client.netty.HedwigSubscriber;
import org.apache.hedwig.exceptions.PubSubException;

import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.util.Callback;

/**
 * This class is used when a Subscribe channel gets disconnected and we attempt
 * to re-establish the connection. Once the connection to the server host for
 * the topic is completed, we need to restart delivery for that topic if that
 * was the case before the original channel got disconnected. This async
 * callback will be the hook for this.
 *
 */
public class SubscribeReconnectCallback implements Callback<Void> {

    private static Logger logger = LoggerFactory.getLogger(SubscribeReconnectCallback.class);

    // Private member variables
    private final PubSubData origSubData;
    private final HedwigClientImpl client;
    private final HedwigSubscriber sub;
    private final ClientConfiguration cfg;

    // Constructor
    public SubscribeReconnectCallback(PubSubData origSubData, HedwigClientImpl client) {
        this.origSubData = origSubData;
        this.client = client;
        this.sub = client.getSubscriber();
        this.cfg = client.getConfiguration();
    }

    class SubscribeReconnectRetryTask extends TimerTask {
        @Override
        public void run() {
            if (logger.isDebugEnabled())
                logger.debug("Retrying subscribe reconnect request for origSubData: " + origSubData);
            // Clear out all of the servers we've contacted or attempted to from
            // this request.
            origSubData.clearServersList();
            client.doConnect(origSubData, cfg.getDefaultServerHost());
        }
    }

    public void operationFinished(Object ctx, Void resultOfOperation) {
        if (logger.isDebugEnabled())
            logger.debug("Subscribe reconnect succeeded for origSubData: " + origSubData);
        // Now we want to restart delivery for the subscription channel only
        // if delivery was started at the time the original subscribe channel
        // was disconnected.
        try {
            sub.restartDelivery(origSubData.topic, origSubData.subscriberId);
        } catch (ClientNotSubscribedException e) {
            // This exception should never be thrown here but just in case,
            // log an error and just keep retrying the subscribe request.
            logger.error("Subscribe was successful but error starting delivery for topic: "
                         + origSubData.topic.toStringUtf8() + ", subscriberId: "
                         + origSubData.subscriberId.toStringUtf8(), e);
            retrySubscribeRequest();
        } catch (AlreadyStartDeliveryException asde) {
            // should not reach here
        }
    }

    public void operationFailed(Object ctx, PubSubException exception) {
        // If the subscribe reconnect fails, just keep retrying the subscribe
        // request. There isn't a way to flag to the application layer that
        // a topic subscription has failed. So instead, we'll just keep
        // retrying in the background until success.
        logger.error("Subscribe reconnect failed with error: " + exception.getMessage());
        retrySubscribeRequest();
    }

    private void retrySubscribeRequest() {
        // If the client has stopped, there is no need to proceed with any
        // callback logic here.
        if (client.hasStopped())
            return;

        // Retry the subscribe request but only after waiting for a
        // preconfigured amount of time.
        client.getClientTimer().schedule(new SubscribeReconnectRetryTask(),
                                         client.getConfiguration().getSubscribeReconnectRetryWaitTime());
    }
}
