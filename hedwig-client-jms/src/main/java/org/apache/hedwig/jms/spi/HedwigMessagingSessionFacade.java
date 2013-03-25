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

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.jms.MessagingSessionFacade;
import org.apache.hedwig.jms.SessionImpl;
import org.apache.hedwig.jms.DebugUtil;
import org.apache.hedwig.jms.message.MessageImpl;
import org.apache.hedwig.jms.message.MessageUtil;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

/**
 * Implementation of hedwig specific implementation. <br/>
 * JMS VIOLATION: This implementation creates a single backend hedwig connection PER session - and
 * DOES NOT share multiple sessoins on top of a single connection.
 * <p/>
 * This is a wilful violation of JMS specification, but exists only because Hedwig does not have
 * any notion to support this. <br/>
 * Once hedwig does allow for session multiplexing, we will need to revisit this (or create a new impl)
 * to take into account the changes.
 *
 */
public class HedwigMessagingSessionFacade implements MessagingSessionFacade, MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(HedwigMessagingSessionFacade.class);

            // We simulate noLocal through the connection - which will be shared across sessions.
    private final HedwigConnectionImpl connection;
    private final SessionImpl session;
    private HedwigClient hedwigClient;
    private volatile boolean stopped = false;

    /*
     Hedwig server has a ack-until-N approach to acknoledgements : that is, if we acknowledge message N,
     all previous N-1 message are also
     acknowledged.
     But hedwig-client DOES NOT support this : particularly in context of throttling.

     So, when we are in CLIENT_ACKNOWLEDGE mode and NOT in transacted session, I am modifying the behavior
     to mirror expectation of both
     hedwig client and server here in SessionImpl itself (instead of facade where this probably belong better).

     This approach does not seem to work fine due to implicit assumptions in hedwig client ... I am
     modifying it in following way :
     a) For each message receieved, maintain it in List.
     b) Acknowledging a message means traversing this list to find message with same seq-id : and
     acknowledge ALL message until that in the list.
     Since hedwig does ack until, inctead of individual ack, this violation of JMS spec is consistent with hedwig.
     Note that even though hedwig does ack until, hedwig client on other hand DOES NOT ! It will
     throttle connection if we do not ack individually ...
     sigh :-(
      */
    private final List<SessionImpl.ReceivedMessage> unAckMessageList = new LinkedList<SessionImpl.ReceivedMessage>();

    // Both of these synchronized on deliveryStartInfoSet.
    private final Set<DeliveryStartInfo> deliveryStartInfoSet = new HashSet<DeliveryStartInfo>(32);
    private final Set<DeliveryStartInfo> subscribeInfoSet = new HashSet<DeliveryStartInfo>(32);

    private static final class DeliveryStartInfo {
        private final String topicName;
        private final String subscriberId;

        private DeliveryStartInfo(String subscriberId, String topicName) {
            this.subscriberId = subscriberId;
            this.topicName = topicName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DeliveryStartInfo that = (DeliveryStartInfo) o;

            if (subscriberId != null ? !subscriberId.equals(that.subscriberId) : that.subscriberId != null)
                return false;
            if (topicName != null ? !topicName.equals(that.topicName) : that.topicName != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = topicName != null ? topicName.hashCode() : 0;
            result = 31 * result + (subscriberId != null ? subscriberId.hashCode() : 0);
            return result;
        }
    }


    public HedwigMessagingSessionFacade(HedwigConnectionImpl connection, SessionImpl session) throws JMSException {
        this.connection = connection;
        this.session = session;
        // always create client ...
        final ClientConfiguration cfg = connection.getHedwigClientConfig();
        if (null == cfg) throw new JMSException("Unable to fetch client config ?");
        this.hedwigClient = new HedwigClient(cfg);
        resetStartInfoSet();
    }

    @Override
    public void start() throws JMSException {
        if (!connection.isInStartMode()) throw new JMSException("Connection not yet started ?");
        if (logger.isTraceEnabled()) logger.trace("Creating HedwigClient");
        // create only if there is need for it.
        if (null == this.hedwigClient) {
            this.hedwigClient = new HedwigClient(connection.getHedwigClientConfig());
            resetStartInfoSet();
        }
        this.stopped = false;
    }

    @Override
    public void stop() {
        // stopping does not inhibit send.
        if (logger.isTraceEnabled()) logger.trace("Stopping HedwigClient");
        /*
        HedwigClient client = this.hedwigClient;
        this.hedwigClient = null;
        client.close();
        */
        this.stopped = true;
    }


    @Override
    public void close() {
        HedwigClient client = this.hedwigClient;
        resetStartInfoSet();

        this.stopped = true;
        this.hedwigClient = null;
        if (logger.isTraceEnabled()) logger.trace("Closing HedwigClient");
        client.close();
    }

    private void resetStartInfoSet(){
        synchronized (deliveryStartInfoSet){
            deliveryStartInfoSet.clear();
            subscribeInfoSet.clear();
        }
    }

    @Override
    public DestinationType findDestinationType(String destination) throws JMSException {
        // TODO: For now, we support ONLY topic's, so always returning that.
        return DestinationType.TOPIC;
    }

    @Override
    public DestinationType findDestinationType(Destination destination) throws JMSException {
        if (destination instanceof Topic) return DestinationType.TOPIC;
        if (destination instanceof Queue) return DestinationType.QUEUE;

        // TODO: For now, we support ONLY topic's, so always returning that when unknown.
        return DestinationType.TOPIC;
    }

    @Override
    public TopicPublisher createTopicPublisher(Destination destination) throws JMSException {
        return new TopicPublisherImpl(this, session, null != destination ?
            session.createTopic(session.toName(destination)) : null);
    }

    @Override
    public TopicSubscriber createTopicSubscriber(Destination destination) throws JMSException {
        session.subscriberCreated();
        connection.initConnectionClientID();
        return new TopicSubscriberImpl(session, session.createTopic(session.toName(destination)),
                session.createSubscriberId(SessionImpl.generateRandomString()), true);
    }

    @Override
    public TopicSubscriber createTopicSubscriber(Destination destination,
                                                 String messageSelector, boolean noLocal) throws JMSException {
        session.subscriberCreated();
        connection.initConnectionClientID();
        return new TopicSubscriberImpl(session,
                session.createTopic(session.toName(destination)),
                session.createSubscriberId(SessionImpl.generateRandomString()), messageSelector, noLocal, true);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String subscribedId) throws JMSException {
        if (null != session.getMessageListener()) {
            throw new JMSException("Message listener is set - not other form of message receipt can be used");
        }
        session.subscriberCreated();

        TopicSubscriberImpl subscriber = new TopicSubscriberImpl(session, topic, subscribedId, false);
        subscriber.start();
        return subscriber;
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String subscribedId,
                                                   String messageSelector, boolean noLocal) throws JMSException {
        if (null != session.getMessageListener()) {
            throw new JMSException("Message listener is set - not other form of message receipt can be used");
        }
        session.subscriberCreated();
        connection.initConnectionClientID();

        return new TopicSubscriberImpl(session, topic, subscribedId, messageSelector, noLocal, false);
    }

    /*
    @Override
    public void unsubscribe(String subscriberId) throws JMSException {
        throw new JMSException("Hedwig requires BOTH topic name and subscriberId to unsubscribe -
        unlike JMS. Need to figure this out.");
    }
    */

    // Note: order SENSITIVE !!
    @Override
    public void registerUnAcknowledgedMessage(SessionImpl.ReceivedMessage message) {
        synchronized (unAckMessageList){
            unAckMessageList.add(message);
        }
    }

    @Override
    // public void acknowledge(String topicName, String subscriberId, String jmsMessageID)
    public void acknowledge(MessageImpl message) throws JMSException {
        if (this.stopped || null == hedwigClient)
          throw new javax.jms.IllegalStateException("session in stopped or closed state, cant acknowledge message");

        /*
         This approach does not seem to work fine due to implicit assumptions in hedwig client ...
         I am modifying it in following way :
         a) For each message receieved, maintain it in List.
         b) Acknowledging a message means traversing this list to find message with same seq-id :
         and acknowledge ALL message until that in the list.
         Since hedwig does ack until, inctead of individual ack, this violation of JMS spec is consistent with hedwig.
         Note that even though hedwig does ack until, hedwig client on other hand DOES NOT ! It will
          throttle connection if we do not ack individually ...
         sigh :-(
          */
        // sendAcknowledge(topicName, subscriberId, seqId);

        LinkedList<SessionImpl.ReceivedMessage> ackList = new LinkedList<SessionImpl.ReceivedMessage>();
        synchronized (unAckMessageList){
            // Should I simply copy and release ?
            ListIterator<SessionImpl.ReceivedMessage> iter = unAckMessageList.listIterator();

            boolean found = false;
            while (iter.hasNext()){
                if (iter.next().originalMessage.getServerJmsMessageId().equals(message.getServerJmsMessageId())){
                    found = true;
                    break;
                }
            }

            // probably already acknowledged ?
            if (!found) return ;
            while (iter.hasPrevious()){
                ackList.addFirst(iter.previous());
                iter.remove();
            }
        }

        // Now acknowledge the messages in ackList by running its runnable.
        if (logger.isTraceEnabled()) {
            logger.trace("facade acknowledge ackList (" + ackList.size() + ") ... " + ackList);
        }
        for (SessionImpl.ReceivedMessage msg : ackList){
            try {
                msg.originalMessage.getAckRunnable().run();
            } catch (Exception ex){
                // Ignore any exception thrown.
                if (logger.isDebugEnabled()) {
                    logger.debug("Ignoring exception thrown while acknowledging messages", ex);
                }
            }
        }

    }

    private void sendAcknowledge(String topicName, String subscriberId, PubSubProtocol.MessageSeqId seqId)
        throws JMSException {

        if (logger.isTraceEnabled()) logger.trace("Acknowledging " +
            MessageUtil.generateJMSMessageIdFromSeqId(seqId) + " for " + topicName + " by " + subscriberId);
        try {
            hedwigClient.getSubscriber().consume(ByteString.copyFromUtf8(topicName),
                ByteString.copyFromUtf8(subscriberId), seqId);
        } catch (PubSubException.ClientNotSubscribedException e) {
            JMSException jEx = new JMSException("Client not subscribed .. " + e);
            jEx.setLinkedException(e);
            throw jEx;
        }
    }


    public void subscribeToTopic(String topicName, String subscribedId) throws JMSException {
        if (null == hedwigClient)
          throw new javax.jms.IllegalStateException("session in closed state, cant subscribe to topic " + topicName);

        final DeliveryStartInfo info = new DeliveryStartInfo(topicName, subscribedId);
        final boolean start;
        synchronized (deliveryStartInfoSet){
            start =  ! subscribeInfoSet.contains(info);

            if (start) {
                subscribeInfoSet.add(info);
            }
        }

        if (! start) {
            if (logger.isDebugEnabled()) logger.debug("Client already subscribed ?");
            return ;
        }

        try {
            SubscriptionOptions opts = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).build();
            hedwigClient.getSubscriber().subscribe(ByteString.copyFromUtf8(topicName),
                    ByteString.copyFromUtf8(subscribedId), opts);
        } catch (PubSubException.CouldNotConnectException e) {
            JMSException je = new JMSException("receive failed, could not connect .. " + e);
            je.setLinkedException(e);
            throw je;
        } catch (PubSubException.ClientAlreadySubscribedException e) {
            JMSException je = new JMSException("receive failed, already subscribed .. " + e);
            je.setLinkedException(e);
            throw je;
        } catch (PubSubException.ServiceDownException e) {
            JMSException je = new JMSException("receive failed, hedwig service down .. " + e);
            je.setLinkedException(e);
            throw je;
        } catch (InvalidSubscriberIdException e) {
            JMSException je = new JMSException("receive failed, invalid subscriber .. " + e);
            je.setLinkedException(e);
            throw je;
        }
    }

    public void unsubscribeFromTopic(String topicName, String subscribedId) throws JMSException {
        if (null == hedwigClient)
          throw new javax.jms.IllegalStateException("session in closed state, cant acknowledge message");

        // Also implies removal of delivery, right ?
        final DeliveryStartInfo info = new DeliveryStartInfo(topicName, subscribedId);
        synchronized (deliveryStartInfoSet){
            deliveryStartInfoSet.remove(info);
            subscribeInfoSet.remove(info);
        }

        try {
            hedwigClient.getSubscriber().unsubscribe(ByteString.copyFromUtf8(topicName),
                ByteString.copyFromUtf8(subscribedId));
        } catch (PubSubException.CouldNotConnectException e) {
            JMSException je = new JMSException("receive failed, could not connect .. " + e);
            je.setLinkedException(e);
            throw je;
        } catch (PubSubException.ServiceDownException e) {
            JMSException je = new JMSException("receive failed, hedwig service down .. " + e);
            je.setLinkedException(e);
            throw je;
        } catch (InvalidSubscriberIdException e) {
            JMSException je = new JMSException("receive failed, invalid subscriber .. " + e);
            je.setLinkedException(e);
            throw je;
        } catch (PubSubException.ClientNotSubscribedException e) {
            JMSException je = new JMSException("receive failed, client not subscribed .. " + e);
            je.setLinkedException(e);
            throw je;
        }
    }

    public void stopTopicDelivery(String topicName, String subscribedId) throws JMSException {
        if (null == hedwigClient)
          throw new javax.jms.IllegalStateException("session in closed state, cant acknowledge message");

        DeliveryStartInfo info = new DeliveryStartInfo(topicName, subscribedId);
        synchronized (deliveryStartInfoSet){
            deliveryStartInfoSet.remove(info);
        }

        try {
            hedwigClient.getSubscriber().stopDelivery(ByteString.copyFromUtf8(topicName),
                ByteString.copyFromUtf8(subscribedId));
        } catch (PubSubException.ClientNotSubscribedException e) {
            if (logger.isTraceEnabled()) logger.trace("Client not subscribed or already unsubscribed ? ", e);
        }
    }

    public void startTopicDelivery(String topicName, String subscribedId) throws JMSException {
        if (null == hedwigClient)
          throw new javax.jms.IllegalStateException("session in closed state, cant acknowledge message");

        final DeliveryStartInfo info = new DeliveryStartInfo(topicName, subscribedId);
        final boolean start;
        synchronized (deliveryStartInfoSet){
            start =  ! deliveryStartInfoSet.contains(info);

            if (start) {
                deliveryStartInfoSet.add(info);
            }
        }

        if (! start) {
            if (logger.isDebugEnabled()) logger.debug("Client already started delivery ?");
            return ;
        }

        try {
            if (logger.isTraceEnabled()) logger.trace("Start topic delivery for " + topicName +
                ", subscriberId " + subscribedId);
            hedwigClient.getSubscriber().startDelivery(ByteString.copyFromUtf8(topicName),
                ByteString.copyFromUtf8(subscribedId), this);
            if (logger.isTraceEnabled()) logger.trace("Start topic delivery for " + topicName +
                ", subscriberId " + subscribedId + " DONE");
        } catch (PubSubException.ClientNotSubscribedException e) {
            if (logger.isDebugEnabled()) logger.debug("Client not subscribed or already unsubscribed ? ", e);
        } catch (AlreadyStartDeliveryException e) {
            if (logger.isDebugEnabled()) logger.debug("Client already started delivery ? ", e);
        }
    }

    @Override
    public void deliver(ByteString topic, ByteString subscriberId, PubSubProtocol.Message msg,
                        final Callback<Void> callback, final Object context) {
        // Deliver the message to the session.

        if (this.stopped) {
            if (logger.isDebugEnabled()) logger.debug("Ignoring message while in stopped mode .. topic - " +
                topic.toStringUtf8() + ", subscriber - " + subscriberId.toStringUtf8() + ", msg - " + msg);
            return ;
        }

        if (logger.isTraceEnabled()) logger.trace("recieved message from server : topic - " +
                topic.toStringUtf8() + ", subscriber - " + subscriberId.toStringUtf8() + ", msg - " + msg);

        // I am assuming that we can defer the acknowledgement of the message ...
        final String topicName = topic.toStringUtf8();
        final String sid = subscriberId.toStringUtf8();
        final PubSubProtocol.MessageSeqId seqId = msg.getMsgId();
        final Runnable ack = new Runnable(){
            public void run() {
                callback.operationFinished(context, null);
                // Only when auto-send is NOT enabled.
                if (! connection.getHedwigClientConfig().isAutoSendConsumeMessageEnabled()) {
                    try {
                        sendAcknowledge(topicName, sid, seqId);
                    } catch (JMSException e) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Unable to send acknowledgement ... " + topicName + ", " +
                                sid + ", seqId : " + seqId);
                            DebugUtil.dumpJMSStacktrace(logger, e);
                        }
                    }
                }
            }
        };

        try {
            if (logger.isTraceEnabled()) logger.trace("Pushing to session " + session);

            MessageImpl messageImpl = MessageUtil.processHedwigMessage(session, msg, topicName, sid, ack);
            session.messageReceived(messageImpl, DestinationType.TOPIC);
        } catch (JMSException e) {
            // Unable to process the incoming message - log and ignore ?
            if (logger.isDebugEnabled()) {
                logger.debug("Unable to consume message");
                DebugUtil.dumpJMSStacktrace(logger, e);
            }
        }
    }

    public String getSubscriberId(TopicSubscriber topicSubscriber) throws JMSException {
        if (! (topicSubscriber instanceof TopicSubscriberImpl) )
          throw new JMSException("TopicSubscriber not instanceof of TopicSubscriberImpl ? " +
              topicSubscriber.getClass());

        return ((TopicSubscriberImpl) topicSubscriber).getSubscriberId();
    }

    @Override
    public boolean enqueueReceivedMessage(MessageConsumer messageConsumer, SessionImpl.ReceivedMessage receivedMessage,
                                          boolean addFirst) throws JMSException {
        if (! (messageConsumer instanceof TopicSubscriberImpl) )
          throw new JMSException("TopicSubscriber not instanceof of TopicSubscriberImpl ? " +
              messageConsumer.getClass());

        return ((TopicSubscriberImpl) messageConsumer).enqueueReceivedMessage(receivedMessage, addFirst);
    }

    public Publisher getPublisher() throws javax.jms.IllegalStateException {
        if (null == hedwigClient)
          throw new javax.jms.IllegalStateException("session in closed state, cant acknowledge message");
        return hedwigClient.getPublisher();
    }

    public String publish(String topicName, MessageImpl message) throws JMSException {
        try {
            PubSubProtocol.PublishResponse response = getPublisher().publish(
                ByteString.copyFromUtf8(topicName), message.generateHedwigMessage());
            PubSubProtocol.MessageSeqId seqId =
                (null != response && response.hasPublishedMsgId() ? response.getPublishedMsgId() : null);
            if (null == seqId){
                // if (logger.isDebugEnabled())
                // logger.debug("Unexpected NOT to receive the sequence id in response to publish " + response);
                logger.warn("Unexpected NOT to receive the sequence id in response to publish " + response);
                return null;
            }

            return MessageUtil.generateJMSMessageIdFromSeqId(seqId);
        } catch (PubSubException.CouldNotConnectException e) {
            JMSException jmsEx = new JMSException("Cant publish to " + topicName + " .. " + e);
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (PubSubException.ServiceDownException e) {
            JMSException jmsEx = new JMSException("Cant publish to " + topicName + " .. " + e);
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }

    // Queue methods which are NOT supported yet.
    @Override
    public QueueSender createQueueSender(Destination destination) throws JMSException {
        throw new JMSException("hedwig does not support queues yet");
    }

    @Override
    public QueueReceiver createQueueReceiver(Destination destination) throws JMSException {
        throw new JMSException("hedwig does not support queues yet");
    }

    @Override
    public QueueReceiver createQueueReceiver(Destination destination, String messageSelector) throws JMSException {
        throw new JMSException("hedwig does not support queues yet");
    }

    @Override
    public QueueReceiver createQueueReceiver(Destination destination, String messageSelector,
                                             boolean noLocal) throws JMSException {
        throw new JMSException("hedwig does not support queues yet");
    }

    @Override
    public String getSubscriberId(QueueReceiver queueReceiver) throws JMSException {
        throw new JMSException("hedwig does not support queues yet");
    }

    @Override
    public void stopQueueDelivery(String queueName, String subscribedId) throws JMSException {
        throw new JMSException("hedwig does not support queues yet");
    }

    @Override
    public void startQueueDelivery(String queueName, String subscriberId) throws JMSException {
        throw new JMSException("hedwig does not support queues yet");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        throw new JMSException("hedwig does not support queues yet");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        throw new JMSException("hedwig does not support queues yet");
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new JMSException("hedwig does not support queues yet");
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new JMSException("hedwig does not support queues yet");
    }

}
