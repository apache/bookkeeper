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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.handlers.PubSubCallback;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.util.Callback;

/**
 * This is the Hedwig Netty specific implementation of the Publisher interface.
 *
 */
public class HedwigPublisher implements Publisher {

    private static Logger logger = LoggerFactory.getLogger(HedwigPublisher.class);

    private final HChannelManager channelManager;

    protected HedwigPublisher(HedwigClientImpl client) {
        this.channelManager = client.getHChannelManager();
    }

    public PublishResponse publish(ByteString topic, Message msg)
        throws CouldNotConnectException, ServiceDownException {

        if (logger.isDebugEnabled()) {
            logger.debug("Calling a sync publish for topic: {}, msg: {}.",
                         topic.toStringUtf8(), msg);
        }
        PubSubData pubSubData = new PubSubData(topic, msg, null, OperationType.PUBLISH, null, null, null);
        synchronized (pubSubData) {
            PubSubCallback pubSubCallback = new PubSubCallback(pubSubData);
            asyncPublishWithResponseImpl(topic, msg, pubSubCallback, null);
            try {
                while (!pubSubData.isDone)
                    pubSubData.wait();
            } catch (InterruptedException e) {
                throw new ServiceDownException("Interrupted Exception while waiting for async publish call");
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
                    logger.error("Sync Publish operation failed but no PubSubException was passed!");
                    throw new ServiceDownException("Server ack response to publish request is not successful");
                }
                // For the expected exceptions that could occur, just rethrow
                // them.
                else if (failureException instanceof CouldNotConnectException) {
                    throw (CouldNotConnectException) failureException;
                } else if (failureException instanceof ServiceDownException) {
                    throw (ServiceDownException) failureException;
                } else {
                    // For other types of PubSubExceptions, just throw a generic
                    // ServiceDownException but log a warning message.
                    logger.error("Unexpected exception type when a sync publish operation failed: ",
                                 failureException);
                    throw new ServiceDownException("Server ack response to publish request is not successful");
                }
            }

            ResponseBody respBody = pubSubCallback.getResponseBody();
            if (null == respBody) {
                return null;
            }
            return respBody.hasPublishResponse() ? respBody.getPublishResponse() : null;
        }
    }

    public void asyncPublish(ByteString topic, Message msg,
                             final Callback<Void> callback, Object context) {
        asyncPublishWithResponseImpl(topic, msg,
                                     new VoidCallbackAdapter<ResponseBody>(callback), context);
    }

    public void asyncPublishWithResponse(ByteString topic, Message msg,
                                         Callback<PublishResponse> callback,
                                         Object context) {
        // adapt the callback.
        asyncPublishWithResponseImpl(topic, msg,
                                     new PublishResponseCallbackAdapter(callback), context);
    }

    private void asyncPublishWithResponseImpl(ByteString topic, Message msg,
                                              Callback<ResponseBody> callback,
                                              Object context) {
        if (logger.isDebugEnabled()) {
            logger.debug("Calling an async publish for topic: {}, msg: {}.",
                         topic.toStringUtf8(), msg);
        }
        PubSubData pubSubData = new PubSubData(topic, msg, null, OperationType.PUBLISH, null,
                                               callback, context);
        channelManager.submitOp(pubSubData);
    }

    private static class PublishResponseCallbackAdapter implements Callback<ResponseBody>{

        private final Callback<PublishResponse> delegate;

        private PublishResponseCallbackAdapter(Callback<PublishResponse> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void operationFinished(Object ctx, ResponseBody resultOfOperation) {
            if (null == resultOfOperation) {
                delegate.operationFinished(ctx, null);
            } else {
                delegate.operationFinished(ctx, resultOfOperation.getPublishResponse());
            }
        }

        @Override
        public void operationFailed(Object ctx, PubSubException exception) {
            delegate.operationFailed(ctx, exception);
        }
    }
}
