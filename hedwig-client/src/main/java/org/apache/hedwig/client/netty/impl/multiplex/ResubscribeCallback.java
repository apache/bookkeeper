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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.util.Callback;
import static org.apache.hedwig.util.VarArgs.va;

/**
 * This class is used when a Subscribe channel gets disconnected and we attempt
 * to resubmit subscribe requests existed in that channel. Once the resubscribe
 * the topic is completed, we need to restart delivery for that topic.
 */
class ResubscribeCallback implements Callback<ResponseBody> {

    private static Logger logger = LoggerFactory.getLogger(ResubscribeCallback.class);

    // Private member variables
    private final TopicSubscriber origTopicSubscriber;
    private final PubSubData origSubData;
    private final MultiplexHChannelManager channelManager;
    private final long retryWaitTime;

    // Constructor
    ResubscribeCallback(TopicSubscriber origTopicSubscriber,
                        PubSubData origSubData,
                        MultiplexHChannelManager channelManager,
                        long retryWaitTime) {
        this.origTopicSubscriber = origTopicSubscriber;
        this.origSubData = origSubData;
        this.channelManager = channelManager;
        this.retryWaitTime = retryWaitTime;
    }

    @Override
    public void operationFinished(Object ctx, ResponseBody resultOfOperation) {
        if (logger.isDebugEnabled())
            logger.debug("Resubscribe succeeded for origSubData: " + origSubData);
        // Now we want to restart delivery for the subscription channel only
        // if delivery was started at the time the original subscribe channel
        // was disconnected.
        try {
            channelManager.restartDelivery(origTopicSubscriber);
        } catch (ClientNotSubscribedException e) {
            // This exception should never be thrown here but just in case,
            // log an error and just keep retrying the subscribe request.
            logger.error("Subscribe was successful but error starting delivery for {} : {}",
                         va(origTopicSubscriber, e.getMessage()));
            retrySubscribeRequest();
        } catch (AlreadyStartDeliveryException asde) {
            // should not reach here
        }
    }

    @Override
    public void operationFailed(Object ctx, PubSubException exception) {
        // If the resubscribe fails, just keep retrying the subscribe
        // request. There isn't a way to flag to the application layer that
        // a topic subscription has failed. So instead, we'll just keep
        // retrying in the background until success.
        logger.error("Resubscribe failed with error: " + exception.getMessage());
        retrySubscribeRequest();
    }

    private void retrySubscribeRequest() {
        origSubData.clearServersList();
        logger.debug("Resubmit subscribe request for {} in {} ms later.",
                     va(origTopicSubscriber, retryWaitTime));
        channelManager.submitOpAfterDelay(origSubData, retryWaitTime);
    }
}
