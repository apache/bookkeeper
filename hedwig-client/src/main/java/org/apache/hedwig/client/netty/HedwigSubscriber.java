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
package org.apache.hedwig.client.netty;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.client.handlers.PubSubCallback;
import org.apache.hedwig.client.handlers.SubscribeResponseHandler;
import org.apache.hedwig.filter.ClientMessageFilter;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.SubscriptionListener;

/**
 * This is the Hedwig Netty specific implementation of the Subscriber interface.
 *
 */
public class HedwigSubscriber implements Subscriber {

    private static Logger logger = LoggerFactory.getLogger(HedwigSubscriber.class);

    protected final ClientConfiguration cfg;
    protected final HChannelManager channelManager;

    public HedwigSubscriber(HedwigClientImpl client) {
        this.cfg = client.getConfiguration();
        this.channelManager = client.getHChannelManager();
    }

    public void addSubscriptionListener(SubscriptionListener listener) {
        channelManager.getSubscriptionEventEmitter()
                      .addSubscriptionListener(listener);
    }

    public void removeSubscriptionListener(SubscriptionListener listener) {
        channelManager.getSubscriptionEventEmitter()
                      .removeSubscriptionListener(listener);
    }

    // Private method that holds the common logic for doing synchronous
    // Subscribe or Unsubscribe requests. This is for code reuse since these
    // two flows are very similar. The assumption is that the input
    // OperationType is either SUBSCRIBE or UNSUBSCRIBE.
    private void subUnsub(ByteString topic, ByteString subscriberId, OperationType operationType,
                          SubscriptionOptions options)
            throws CouldNotConnectException, ClientAlreadySubscribedException,
        ClientNotSubscribedException, ServiceDownException {
        if (logger.isDebugEnabled()) {
            StringBuilder debugMsg = new StringBuilder().append("Calling a sync subUnsub request for topic: ")
                                     .append(topic.toStringUtf8()).append(", subscriberId: ")
                                     .append(subscriberId.toStringUtf8()).append(", operationType: ")
                                     .append(operationType);
            if (null != options) {
                debugMsg.append(", createOrAttach: ").append(options.getCreateOrAttach())
                        .append(", messageBound: ").append(options.getMessageBound());
            }
            logger.debug(debugMsg.toString());
        }
        PubSubData pubSubData = new PubSubData(topic, null, subscriberId, operationType, options, null, null);
        synchronized (pubSubData) {
            PubSubCallback pubSubCallback = new PubSubCallback(pubSubData);
            asyncSubUnsub(topic, subscriberId, pubSubCallback, null, operationType, options);
            try {
                while (!pubSubData.isDone)
                    pubSubData.wait();
            } catch (InterruptedException e) {
                throw new ServiceDownException("Interrupted Exception while waiting for async subUnsub call");
            }
            // Check from the PubSubCallback if it was successful or not.
            if (!pubSubCallback.getIsCallSuccessful()) {
                // See what the exception was that was thrown when the operation
                // failed.
                PubSubException failureException = pubSubCallback.getFailureException();
                if (failureException == null) {
                    // This should not happen as the operation failed but a null
                    // PubSubException was passed. Log a warning message but
                    // throw a generic ServiceDownException.
                    logger.error("Sync SubUnsub operation failed but no PubSubException was passed!");
                    throw new ServiceDownException("Server ack response to SubUnsub request is not successful");
                }
                // For the expected exceptions that could occur, just rethrow
                // them.
                else if (failureException instanceof CouldNotConnectException)
                    throw (CouldNotConnectException) failureException;
                else if (failureException instanceof ClientAlreadySubscribedException)
                    throw (ClientAlreadySubscribedException) failureException;
                else if (failureException instanceof ClientNotSubscribedException)
                    throw (ClientNotSubscribedException) failureException;
                else if (failureException instanceof ServiceDownException)
                    throw (ServiceDownException) failureException;
                else {
                    logger.error("Unexpected PubSubException thrown: ", failureException);
                    // Throw a generic ServiceDownException but wrap the
                    // original PubSubException within it.
                    throw new ServiceDownException(failureException);
                }
            }
        }
    }

    // Private method that holds the common logic for doing asynchronous
    // Subscribe or Unsubscribe requests. This is for code reuse since these two
    // flows are very similar. The assumption is that the input OperationType is
    // either SUBSCRIBE or UNSUBSCRIBE.
    private void asyncSubUnsub(ByteString topic, ByteString subscriberId,
                               Callback<ResponseBody> callback, Object context,
                               OperationType operationType, SubscriptionOptions options) {
        if (logger.isDebugEnabled()) {
            StringBuilder debugMsg = new StringBuilder().append("Calling a async subUnsub request for topic: ")
                                     .append(topic.toStringUtf8()).append(", subscriberId: ")
                                     .append(subscriberId.toStringUtf8()).append(", operationType: ")
                                     .append(operationType);
            if (null != options) {
                debugMsg.append(", createOrAttach: ").append(options.getCreateOrAttach())
                        .append(", messageBound: ").append(options.getMessageBound());
            }
            logger.debug(debugMsg.toString());
        }
        if (OperationType.SUBSCRIBE.equals(operationType)) {
            if (options.getMessageBound() <= 0 &&
                cfg.getSubscriptionMessageBound() > 0) {
                SubscriptionOptions.Builder soBuilder =
                    SubscriptionOptions.newBuilder(options).setMessageBound(
                        cfg.getSubscriptionMessageBound());
                options = soBuilder.build();
            }
        }
        PubSubData pubSubData = new PubSubData(topic, null, subscriberId, operationType,
                                               options, callback, context);
        channelManager.submitOp(pubSubData);
    }

    public void subscribe(ByteString topic, ByteString subscriberId, CreateOrAttach mode)
            throws CouldNotConnectException, ClientAlreadySubscribedException, ServiceDownException,
        InvalidSubscriberIdException {
        SubscriptionOptions options = SubscriptionOptions.newBuilder().setCreateOrAttach(mode).build();
        subscribe(topic, subscriberId, options, false);
    }

    public void subscribe(ByteString topic, ByteString subscriberId, SubscriptionOptions options)
            throws CouldNotConnectException, ClientAlreadySubscribedException, ServiceDownException,
         InvalidSubscriberIdException {
        subscribe(topic, subscriberId, options, false);
    }

    protected void subscribe(ByteString topic, ByteString subscriberId, SubscriptionOptions options, boolean isHub)
            throws CouldNotConnectException, ClientAlreadySubscribedException, ServiceDownException,
        InvalidSubscriberIdException {
        // Validate that the format of the subscriberId is valid either as a
        // local or hub subscriber.
        if (!isValidSubscriberId(subscriberId, isHub)) {
            throw new InvalidSubscriberIdException("SubscriberId passed is not valid: " + subscriberId.toStringUtf8()
                                                   + ", isHub: " + isHub);
        }
        try {
            subUnsub(topic, subscriberId, OperationType.SUBSCRIBE, options);
        } catch (ClientNotSubscribedException e) {
            logger.error("Unexpected Exception thrown: ", e);
            // This exception should never be thrown here. But just in case,
            // throw a generic ServiceDownException but wrap the original
            // Exception within it.
            throw new ServiceDownException(e);
        }
    }

    public void asyncSubscribe(ByteString topic, ByteString subscriberId, CreateOrAttach mode, Callback<Void> callback,
                               Object context) {
        SubscriptionOptions options = SubscriptionOptions.newBuilder().setCreateOrAttach(mode).build();
        asyncSubscribe(topic, subscriberId, options, callback, context, false);
    }

    public void asyncSubscribe(ByteString topic, ByteString subscriberId, SubscriptionOptions options,
                               Callback<Void> callback, Object context) {
        asyncSubscribe(topic, subscriberId, options, callback, context, false);
    }

    protected void asyncSubscribe(ByteString topic, ByteString subscriberId,
                                  SubscriptionOptions options,
                                  Callback<Void> callback, Object context, boolean isHub) {
        // Validate that the format of the subscriberId is valid either as a
        // local or hub subscriber.
        if (!isValidSubscriberId(subscriberId, isHub)) {
            callback.operationFailed(context, new ServiceDownException(new InvalidSubscriberIdException(
                                         "SubscriberId passed is not valid: " + subscriberId.toStringUtf8() + ", isHub: " + isHub)));
            return;
        }
        asyncSubUnsub(topic, subscriberId,
                      new VoidCallbackAdapter<ResponseBody>(callback), context,
                      OperationType.SUBSCRIBE, options);
    }

    public void unsubscribe(ByteString topic, ByteString subscriberId) throws CouldNotConnectException,
        ClientNotSubscribedException, ServiceDownException, InvalidSubscriberIdException {
        unsubscribe(topic, subscriberId, false);
    }

    protected void unsubscribe(ByteString topic, ByteString subscriberId, boolean isHub)
            throws CouldNotConnectException, ClientNotSubscribedException, ServiceDownException,
        InvalidSubscriberIdException {
        // Validate that the format of the subscriberId is valid either as a
        // local or hub subscriber.
        if (!isValidSubscriberId(subscriberId, isHub)) {
            throw new InvalidSubscriberIdException("SubscriberId passed is not valid: " + subscriberId.toStringUtf8()
                                                   + ", isHub: " + isHub);
        }
        // Synchronously close the subscription on the client side. Even
        // if the unsubscribe request to the server errors out, we won't be
        // delivering messages for this subscription to the client. The client
        // can later retry the unsubscribe request to the server so they are
        // "fully" unsubscribed from the given topic.
        closeSubscription(topic, subscriberId);
        try {
            subUnsub(topic, subscriberId, OperationType.UNSUBSCRIBE, null);
        } catch (ClientAlreadySubscribedException e) {
            logger.error("Unexpected Exception thrown: ", e);
            // This exception should never be thrown here. But just in case,
            // throw a generic ServiceDownException but wrap the original
            // Exception within it.
            throw new ServiceDownException(e);
        }
    }

    public void asyncUnsubscribe(final ByteString topic, final ByteString subscriberId,
                                 final Callback<Void> callback, final Object context) {
        doAsyncUnsubscribe(topic, subscriberId,
                           new VoidCallbackAdapter<ResponseBody>(callback),
                           context, false);
    }

    protected void asyncUnsubscribe(final ByteString topic, final ByteString subscriberId,
                                    final Callback<Void> callback, final Object context, boolean isHub) {
        doAsyncUnsubscribe(topic, subscriberId,
                           new VoidCallbackAdapter<ResponseBody>(callback),
                           context, isHub);
    }

    private void doAsyncUnsubscribe(final ByteString topic, final ByteString subscriberId,
                                    final Callback<ResponseBody> callback,
                                    final Object context, boolean isHub) {
        // Validate that the format of the subscriberId is valid either as a
        // local or hub subscriber.
        if (!isValidSubscriberId(subscriberId, isHub)) {
            callback.operationFailed(context, new ServiceDownException(new InvalidSubscriberIdException(
                                         "SubscriberId passed is not valid: " + subscriberId.toStringUtf8() + ", isHub: " + isHub)));
            return;
        }
        // Asynchronously close the subscription. On the callback to that
        // operation once it completes, post the async unsubscribe request.
        doAsyncCloseSubscription(topic, subscriberId, new Callback<ResponseBody>() {
            @Override
            public void operationFinished(Object ctx, ResponseBody resultOfOperation) {
                asyncSubUnsub(topic, subscriberId, callback, context, OperationType.UNSUBSCRIBE, null);
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                callback.operationFailed(context, exception);
            }
        }, null);
    }

    // This is a helper method to determine if a subscriberId is valid as either
    // a hub or local subscriber
    private boolean isValidSubscriberId(ByteString subscriberId, boolean isHub) {
        if ((isHub && !SubscriptionStateUtils.isHubSubscriber(subscriberId))
                || (!isHub && SubscriptionStateUtils.isHubSubscriber(subscriberId)))
            return false;
        else
            return true;
    }

    public void consume(ByteString topic, ByteString subscriberId, MessageSeqId messageSeqId)
            throws ClientNotSubscribedException {
        TopicSubscriber topicSubscriber = new TopicSubscriber(topic, subscriberId);
        logger.debug("Calling consume for {}, messageSeqId: {}.",
                     topicSubscriber, messageSeqId);

        SubscribeResponseHandler subscribeResponseHandler =
            channelManager.getSubscribeResponseHandler(topicSubscriber);
        // Check that this topic subscription on the client side exists.
        if (null == subscribeResponseHandler ||
            !subscribeResponseHandler.hasSubscription(topicSubscriber)) {
            throw new ClientNotSubscribedException(
                "Cannot send consume message since client is not subscribed to topic: "
                + topic.toStringUtf8() + ", subscriberId: " + subscriberId.toStringUtf8());
        }
        // Send the consume message to the server using the same subscribe
        // channel that the topic subscription uses.
        subscribeResponseHandler.consume(topicSubscriber, messageSeqId);
    }

    public boolean hasSubscription(ByteString topic, ByteString subscriberId) throws CouldNotConnectException,
        ServiceDownException {
        // The subscription type of info should be stored on the server end, not
        // the client side. Eventually, the server will have the Subscription
        // Manager part that ties into Zookeeper to manage this info.
        // Commenting out these type of API's related to that here for now until
        // this data is available on the server. Will figure out what the
        // correct way to contact the server to get this info is then.
        // The client side just has soft memory state for client subscription
        // information.
        TopicSubscriber topicSubscriber = new TopicSubscriber(topic, subscriberId);
        SubscribeResponseHandler subscribeResponseHandler =
            channelManager.getSubscribeResponseHandler(topicSubscriber);
        return !(null == subscribeResponseHandler ||
                 !subscribeResponseHandler.hasSubscription(topicSubscriber));
    }

    public List<ByteString> getSubscriptionList(ByteString subscriberId) throws CouldNotConnectException,
        ServiceDownException {
        // Same as the previous hasSubscription method, this data should reside
        // on the server end, not the client side.
        return null;
    }

    public void startDelivery(final ByteString topic, final ByteString subscriberId,
                              MessageHandler messageHandler)
            throws ClientNotSubscribedException, AlreadyStartDeliveryException {
        TopicSubscriber topicSubscriber = new TopicSubscriber(topic, subscriberId);
        logger.debug("Starting delivery for {}.", topicSubscriber);
        channelManager.startDelivery(topicSubscriber, messageHandler); 
    }

    public void startDeliveryWithFilter(final ByteString topic, final ByteString subscriberId,
                                        MessageHandler messageHandler,
                                        ClientMessageFilter messageFilter)
            throws ClientNotSubscribedException, AlreadyStartDeliveryException {
        if (null == messageHandler || null == messageFilter) {
            throw new NullPointerException("Null message handler or message filter is       provided.");
        }
        TopicSubscriber topicSubscriber = new TopicSubscriber(topic, subscriberId);
        messageHandler = new FilterableMessageHandler(messageHandler, messageFilter);
        logger.debug("Starting delivery with filter for {}.", topicSubscriber);
        channelManager.startDelivery(topicSubscriber, messageHandler);
    }

    public void stopDelivery(final ByteString topic, final ByteString subscriberId)
    throws ClientNotSubscribedException {
        TopicSubscriber topicSubscriber = new TopicSubscriber(topic, subscriberId);
        logger.debug("Stopping delivery for {}.", topicSubscriber);
        channelManager.stopDelivery(topicSubscriber); 
    }

    public void closeSubscription(ByteString topic, ByteString subscriberId) throws ServiceDownException {
        PubSubData pubSubData = new PubSubData(topic, null, subscriberId, null, null, null, null);
        synchronized (pubSubData) {
            PubSubCallback pubSubCallback = new PubSubCallback(pubSubData);
            doAsyncCloseSubscription(topic, subscriberId, pubSubCallback, null);
            try {
                while (!pubSubData.isDone)
                    pubSubData.wait();
            } catch (InterruptedException e) {
                throw new ServiceDownException("Interrupted Exception while waiting for asyncCloseSubscription call");
            }
            // Check from the PubSubCallback if it was successful or not.
            if (!pubSubCallback.getIsCallSuccessful()) {
                throw new ServiceDownException("Exception while trying to close the subscription for topic: "
                                               + topic.toStringUtf8() + ", subscriberId: " + subscriberId.toStringUtf8());
            }
        }
    }

    public void asyncCloseSubscription(final ByteString topic, final ByteString subscriberId,
                                       final Callback<Void> callback, final Object context) {
        doAsyncCloseSubscription(topic, subscriberId,
                                 new VoidCallbackAdapter<ResponseBody>(callback), context);
    }

    private void doAsyncCloseSubscription(final ByteString topic, final ByteString subscriberId,
                                          final Callback<ResponseBody> callback, final Object context) {
        TopicSubscriber topicSubscriber = new TopicSubscriber(topic, subscriberId);
        logger.debug("Stopping delivery for {} before closing subscription.", topicSubscriber);
        // We only stop delivery here not in channel manager
        // Because channelManager#asyncCloseSubscription will called
        // when subscription channel disconnected to clear local subscription
        try {
            channelManager.stopDelivery(topicSubscriber); 
        } catch (ClientNotSubscribedException cnse) {
            // it is OK to ignore the exception when closing subscription
        }
        logger.debug("Closing subscription asynchronously for {}.", topicSubscriber);
        channelManager.asyncCloseSubscription(topicSubscriber, callback, context);
    }
}
