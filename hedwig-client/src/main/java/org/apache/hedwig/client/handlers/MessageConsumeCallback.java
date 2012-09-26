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

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.MessageConsumeData;
import org.apache.hedwig.client.netty.HChannelManager;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protoextensions.MessageIdUtils;
import org.apache.hedwig.util.Callback;
import static org.apache.hedwig.util.VarArgs.va;

/**
 * This is the Callback used by the MessageHandlers on the client app when
 * they've finished consuming a subscription message sent from the server
 * asynchronously. This callback back to the client libs will be stateless so we
 * can use a singleton for the class. The object context used should be the
 * MessageConsumeData type. That will contain all of the information needed to
 * call the message consume logic in the client lib HChannelHandler.
 *
 */
public class MessageConsumeCallback implements Callback<Void> {

    private static Logger logger = LoggerFactory.getLogger(MessageConsumeCallback.class);

    private final HChannelManager channelManager;
    private final long consumeRetryWaitTime;

    public MessageConsumeCallback(ClientConfiguration cfg,
                                  HChannelManager channelManager) {
        this.channelManager = channelManager;
        this.consumeRetryWaitTime =
            cfg.getMessageConsumeRetryWaitTime();
    }

    class MessageConsumeRetryTask extends TimerTask {
        private final MessageConsumeData messageConsumeData;

        public MessageConsumeRetryTask(MessageConsumeData messageConsumeData) {
            this.messageConsumeData = messageConsumeData;
        }

        @Override
        public void run() {
            // Try to consume the message again
            SubscribeResponseHandler subscribeHChannelHandler =
                channelManager.getSubscribeResponseHandler(messageConsumeData.topicSubscriber);
            if (null == subscribeHChannelHandler ||
                !subscribeHChannelHandler.hasSubscription(messageConsumeData.topicSubscriber)) {
                logger.warn("No subscription {} found to retry delivering message {}.",
                            va(messageConsumeData.topicSubscriber,
                               MessageIdUtils.msgIdToReadableString(messageConsumeData.msg.getMsgId())));
                return;
            }

            subscribeHChannelHandler.asyncMessageDeliver(messageConsumeData.topicSubscriber,
                                                         messageConsumeData.msg);
        }
    }

    public void operationFinished(Object ctx, Void resultOfOperation) {
        MessageConsumeData messageConsumeData = (MessageConsumeData) ctx;

        SubscribeResponseHandler subscribeHChannelHandler =
            channelManager.getSubscribeResponseHandler(messageConsumeData.topicSubscriber);
        if (null == subscribeHChannelHandler ||
            !subscribeHChannelHandler.hasSubscription(messageConsumeData.topicSubscriber)) {
            logger.warn("No subscription {} found to consume message {}.",
                        va(messageConsumeData.topicSubscriber,
                           MessageIdUtils.msgIdToReadableString(messageConsumeData.msg.getMsgId())));
            return;
        }

        // Message has been successfully consumed by the client app so callback
        // to the HChannelHandler indicating that the message is consumed.
        subscribeHChannelHandler.messageConsumed(messageConsumeData.topicSubscriber,
                                                 messageConsumeData.msg);
    }

    public void operationFailed(Object ctx, PubSubException exception) {
        // Message has NOT been successfully consumed by the client app so
        // callback to the HChannelHandler to try the async MessageHandler
        // Consume logic again.
        MessageConsumeData messageConsumeData = (MessageConsumeData) ctx;
        logger.error("Message was not consumed successfully by client MessageHandler: {}",
                     messageConsumeData);

        // Sleep a pre-configured amount of time (in milliseconds) before we
        // do the retry. In the future, we can have more dynamic logic on
        // what duration to sleep based on how many times we've retried, or
        // perhaps what the last amount of time we slept was. We could stick
        // some of this meta-data into the MessageConsumeData when we retry.
        channelManager.schedule(new MessageConsumeRetryTask(messageConsumeData),
                                consumeRetryWaitTime);
    }

}
