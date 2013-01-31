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
package org.apache.hedwig.jms.spi;

import org.apache.hedwig.jms.SessionImpl;
import org.apache.hedwig.jms.message.MessageImpl;
import org.apache.hedwig.jms.message.MessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

/**
 *
 */
public class TopicPublisherImpl extends MessageProducerImpl implements TopicPublisher {

    private static final Logger logger = LoggerFactory.getLogger(TopicPublisherImpl.class);

    private final HedwigMessagingSessionFacade facade;
    private final Topic topic;

    public TopicPublisherImpl(HedwigMessagingSessionFacade facade, SessionImpl session, Topic topic) {
        super(session);
        this.facade = facade;
        this.topic = topic;
    }

    @Override
    public Topic getTopic() throws JMSException {
        return topic;
    }

    @Override
    public void publish(Message message) throws JMSException {
        if (null == getTopic()) throw new UnsupportedOperationException("Need to specify topic");
        publish(getTopic(), message);
    }

    @Override
    public void publish(Topic topic, Message message) throws JMSException {
        publish(topic, message, getDeliveryMode(), getPriority(), getTimeToLive());
    }

    @Override
    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        if (null == getTopic()) throw new UnsupportedOperationException("Need to specify topic");
        publish(getTopic(), message, deliveryMode, priority, timeToLive);
    }

    // all publish/send methods delegate to this ...
    @Override
    public void publish(final Topic topic, final Message message, final int deliveryMode,
                        final int priority, final long timeToLive) throws JMSException {

        // Simulating this in provider ...
        // if (0 != timeToLive) throw new JMSException("We do not support TTL for messages right now.
        // Specified TTL : " + timeToLive);

        if (MessageProducerImpl.DEFAULT_PRIORITY != priority) {
            if (logger.isInfoEnabled())
              logger.info("We do not support message priorities right now. Specified priority : " + priority);
        }
        if (DeliveryMode.PERSISTENT != deliveryMode) {
            if (logger.isInfoEnabled())
              logger.info("We support only PERSISTENT delivery mode. Unsupported mode : " + deliveryMode);
        }

        if (null == topic){
            throw new InvalidDestinationException("Topic must be specified to publish " + topic);
        }

        final MessageImpl copiedMessageImpl;
        if (message instanceof MessageImpl) copiedMessageImpl = MessageUtil.createCloneForDispatch(
            getSession(), (MessageImpl) message, topic.getTopicName(), null);
        else copiedMessageImpl = MessageUtil.createMessageCopy(getSession(), message);

        // Note: Ensure that we set properties below on both message (user input) and copiedMessageImpl
        // (the cloned/copied message).
        // We are doing set on both instead of set followed by close/copy to prevent cases where message
        // implementation drops
        // headers (like our own impl earlier !)

        // priority ...
        {
            // Set the message priority
            // 3.4.10 JMSPriority "When a message is sent, this field is ignored. After completion of
            // the send, it holds the value specified by the method sending the message."
            // On other hand, we have
            // 3.4.12 Overriding Message Header Fields : "JMS permits an administrator to configure
            // JMS to override the client-specified
            // values for JMSDeliveryMode, JMSExpiration and JMSPriority. If this is done, the header
            // field value must reflect the
            // administratively specified value."
            // For now, to unblock testcases, setting to msgPriority :-) Actually, I think we should
            // set it to Message.DEFAULT_PRIORITY ...
            message.setJMSPriority(priority);
            copiedMessageImpl.setJMSPriority(priority);
            // message.setJMSPriority(Message.DEFAULT_PRIORITY);
            // copiedMessageImpl.setJMSPriority(Message.DEFAULT_PRIORITY);
        }

        // delivery mode ...
        {

            // 3.4.2 JMSDeliveryMode "The JMSDeliveryMode header field contains the delivery mode
            // specified when the message was sent.
            // When a message is sent, this field is ignored. After completion of the send, it holds
            // the delivery mode specified by the sending method."
            message.setJMSDeliveryMode(deliveryMode);
            copiedMessageImpl.setJMSDeliveryMode(deliveryMode);
        }

        // destination ...
        {
            // 3.4.1 JMSDestination "The JMSDestination header field contains the destination to which
            // the message is being sent.
            // When a message is sent, this field is ignored. After completion of the send, it holds
            // the destination object
            // specified by the sending method. When a message is received, its destination value
            // must be equivalent to the
            // value assigned when it was sent."
            message.setJMSDestination(getSession().createTopic(topic.getTopicName()));
            copiedMessageImpl.setJMSDestination(getSession().createTopic(topic.getTopicName()));
        }

        {
            // 3.4.4 JMSTimestamp
            // "The JMSTimestamp header field contains the time a message was handed off to a provider to be sent.
            // It is not the time the message was actually transmitted because the actual send may occur later
            // due to transactions or other client side queueing of messages."
            final long timestamp = SessionImpl.currentTimeMillis();
            message.setJMSTimestamp(timestamp);
            copiedMessageImpl.setJMSTimestamp(timestamp);
        }

        if (timeToLive > 0) {
            final long expiryTime = SessionImpl.currentTimeMillis() + timeToLive;
            message.setJMSExpiration(expiryTime);
            copiedMessageImpl.setJMSExpiration(expiryTime);
        }
        else {
            // no expiry.
            message.setJMSExpiration(0);
        }


        if (getSession().getTransacted()){
            // enqueue if within transactions.
            getSession().enqueuePublishWithinTransaction(topic.getTopicName(), copiedMessageImpl, message);
            return ;
        }

        if (logger.isTraceEnabled()) logger.trace("Publishing message ... recepient " + topic.getTopicName());
        // facade.getPublisher().publish(ByteString.copyFromUtf8(topic.getTopicName()),
        // copiedMessageImpl.generateHedwigMessage(this));
        String msgId = facade.publish(topic.getTopicName(), copiedMessageImpl);
        getSession().addToLocallyPublishedMessageIds(msgId);
        if (message instanceof MessageImpl) ((MessageImpl) message).setJMSMessageIDInternal(msgId);
        else message.setJMSMessageID(msgId);

        if (logger.isTraceEnabled()) logger.trace("Publishing message ... recepient " +
            topic.getTopicName() + ", msgId : " + msgId + " DONE");

        // This is not required, we already do this as part of copiedMessageImpl.generateHedwigMessage()
        // message.setJMSTimestamp(SessionImpl.currentTimeMillis());

    }

    @Override
    public Destination getDestination() throws JMSException {
        return topic;
    }

    @Override
    public void close() throws JMSException {
        // This will be a noop actually ... session.close() takes care of closing the publisher.
    }

    @Override
    public void send(Message message) throws JMSException {
        publish(message);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        if (!(destination instanceof Topic))
          throw new JMSException("Expected destination to be a Topic : " + destination);
        publish((Topic) destination, message);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        publish(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode,
                     int priority, long timeToLive) throws JMSException {
        if (!(destination instanceof Topic))
          throw new JMSException("Expected destination to be a Topic : " + destination);

        publish((Topic) destination, message, deliveryMode, priority, timeToLive);
    }
}
