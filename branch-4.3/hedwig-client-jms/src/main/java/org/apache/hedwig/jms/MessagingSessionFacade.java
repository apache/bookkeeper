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
package org.apache.hedwig.jms;

import org.apache.hedwig.jms.message.MessageImpl;

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

/**
 * Encapsulates the actual implementation away from the rest of the system. <br/>
 * This will allow us to, potentially, change the underlying messaging implementation with minimal
 * disruption to the provide code.<br/>
 * <p/>
 * Note that provider specific validation to conform to JMS spec must be done BEFORE invoking the
 * business methods here.<br/>
 * These are supposed to handle ONLY implementation specific logic, not JMS specific constraint
 * enforcement(s), etc.<br/>
 * <p/>
 * Since hedwig itself might undergo changes due to the JMS provider effort, this will allow us to
 * decouple the changes.<br/>
 * In addition, it is an easy way for us to create proxy objects to allow for testing various
 * aspects of the provider without depending
 * on hedwig itself (via test facade impl's). <br/>
 * <p/>
 * Most of the javadoc's are pretty much verbatim copied from corresponding JMS javadoc's :-)
 *
 * <p/>
 * Note that, typically, actual ConnectionImpl and MessagingSessionFacade used are tightly coupled.
 * ConnectionImpl is the base class to the actual implementations ...
 */
public interface MessagingSessionFacade {



    public enum DestinationType { QUEUE, TOPIC }

    /**
     * Starts the session.
     *
     * @throws JMSException If we are unable to initialize hedwig client (typically).
     */
    public void start() throws JMSException;

    /**
     * Stop the session.
     *
     * @throws JMSException If we are unable to stop hedwig client (typically).
     */
    public void stop() throws JMSException;

    /**
     * Close the session.
     */
    public void close();

    /**
     * Given a destination, find out if it is a queue or topic. Required for createProducer(),
     * createConsumer() methods in session.
     * @param destination The specified destination.
     * @return Its type.
     * @throws javax.jms.JMSException In case of internal errors.
     */
    public DestinationType findDestinationType(String destination) throws JMSException;

    /**
     * Given a destination, find out if it is a queue or topic. Required for createProducer(),
     * createConsumer() methods in session.
     * @param destination The specified destination.
     * @return Its type.
     * @throws javax.jms.JMSException In case of internal errors.
     */
    public DestinationType findDestinationType(Destination destination) throws JMSException;

    /**
     * Create a topic publisher to the specified destination.
     * @param destination The topic to publish to
     * @return A topic publisher for the topic specified
     * @throws javax.jms.JMSException In case of internal error.
     */
    public TopicPublisher createTopicPublisher(Destination destination) throws JMSException;


    /**
     * Create a queue sender to the specified destination.
     * @param destination The queue to publish to
     * @return A queue sender for the queue specified
     * @throws javax.jms.JMSException In case of internal error.
     */
    public QueueSender createQueueSender(Destination destination) throws JMSException;

    /**
     * Create a topic subscriber for the specified destination.
     * @param destination The topic name
     * @return Topic subscriber for the topic.
     * @throws javax.jms.JMSException In case of internal error.
     */
    public TopicSubscriber createTopicSubscriber(Destination destination) throws JMSException;

    /**
     * Create a queue receiver for the specified destination.
     * @param destination The queue name
     * @return Queue receiver for the queue.
     * @throws javax.jms.JMSException In case of internal error.
     */
    public QueueReceiver createQueueReceiver(Destination destination) throws JMSException;

    /**
     * Create a queue receiver for the specified destination.
     * @param destination destination
     * @param messageSelector selector to apply
     * @return A queue receiver conforming to the constraints.
     * @throws javax.jms.JMSException In case of internal error or unsatisfiable constraints.
     */
    public QueueReceiver createQueueReceiver(Destination destination, String messageSelector) throws JMSException;

    /**
     * Create a topic subscriber for the specified destination.
     * @param destination destination
     * @param messageSelector selector to apply
     * @param noLocal should locally published messages be received. Note, for now, we do not (yet) support this.
     * @return A topic subscriber conforming to the constraints.
     * @throws javax.jms.JMSException In case of internal error or unsatisfiable constraints.
     */
    public TopicSubscriber createTopicSubscriber(Destination destination, String messageSelector,
                                                 boolean noLocal) throws JMSException;

    /**
     * Create a queue receiver for the specified destination.
     * @param destination destination
     * @param messageSelector selector to apply
     * @param noLocal should locally published messages be received. Note, for now, we do not (yet) support this.
     * @return A queue receiver conforming to the constraints.
     * @throws javax.jms.JMSException In case of internal error or unsatisfiable constraints.
     */
    public QueueReceiver createQueueReceiver(Destination destination, String messageSelector,
                                             boolean noLocal) throws JMSException;


    /**
     * Creates a durable subscriber to the specified topic. <br/>
     * <p/>
     * If a client needs to receive all the messages published on a topic, including the ones
     * published while the
     * subscriber is inactive, it uses a durable TopicSubscriber.
     * The JMS provider retains a record of this durable subscription and insures that all messages
     * from the
     * topic's publishers are retained until they are acknowledged by this durable subscriber or
     * they have expired.
     * <p/>
     * Sessions with durable subscribers must always provide the same client identifier.
     * In addition, each client must specify a subscribedId that uniquely identifies (within client
     * identifier) each durable
     * subscription it creates. Only one session at a time can have a TopicSubscriber for a particular
     * durable subscription.
     * <p/>
     * A client can change an existing durable subscription by creating a durable TopicSubscriber
     * with the same subscribedId
     * and a new topic and/or message selector.
     * Changing a durable subscriber is equivalent to unsubscribing (deleting) the old one and
     * creating a new one.
     * <p/>
     * In some cases, a connection may both publish and subscribe to a topic.
     * The subscriber NoLocal attribute allows a subscriber to inhibit the delivery of messages
     * published by its own connection.
     * The default value for this attribute is false.
     *
     * @param topic The topic to subscribe to.
     * @param subscribedId Name used to identify the subscription. This should be a combination
     *  of the client-id and the session and is expected to be unique.
     * Only a single subscription can be active for a given subscribedId.
     * @return The topicsubscriber which will recieve messages for the topic.
     * @throws JMSException if failure due to some error
     */
    public TopicSubscriber createDurableSubscriber(Topic topic, String subscribedId) throws JMSException;

    /**
     * Creates a durable subscriber to the specified topic. <br/>
     * <p/>
     * If a client needs to receive all the messages published on a topic, including the ones published while the
     * subscriber is inactive, it uses a durable TopicSubscriber.
     * The JMS provider retains a record of this durable subscription and insures that all messages
     * from the
     * topic's publishers are retained until they are acknowledged by this durable subscriber or
     * they have expired.
     * <p/>
     * Sessions with durable subscribers must always provide the same client identifier.
     * In addition, each client must specify a subscribedId that uniquely identifies (within client
     * identifier) each durable
     * subscription it creates. Only one session at a time can have a TopicSubscriber for a particular
     * durable subscription.
     * <p/>
     * A client can change an existing durable subscription by creating a durable TopicSubscriber with
     * the same subscribedId
     * and a new topic and/or message selector.
     * Changing a durable subscriber is equivalent to unsubscribing (deleting) the old one and
     * creating a new one.
     * <p/>
     * In some cases, a connection may both publish and subscribe to a topic.
     * The subscriber NoLocal attribute allows a subscriber to inhibit the delivery of messages
     * published by its own connection.
     * The default value for this attribute is false.
     *
     * @param topic The topic to subscribe to.
     * @param subscribedId Name used to identify the subscription. This should be a combination of
     *  the client-id and the session and is expected to be unique.
     * Only a single subscription can be active for a given subscribedId.
     * @param messageSelector The selector to filter the messages on.
     * @param noLocal Should local messages be delivered or not.
     * Note that noLocal implementation is NOT currently supported by hedwig and should be simulated
     *  by us in the provider ...
     * @return The topicsubscriber which will recieve messages for the topic.
     * @throws JMSException if failure due to some error
     */
    public TopicSubscriber createDurableSubscriber(Topic topic, String subscribedId,
                                                   String messageSelector, boolean noLocal) throws JMSException;

    /**
     *
     * @param queue The queue
     * @return Create a queue browser for the specified queue.
     */
    public QueueBrowser createBrowser(Queue queue) throws JMSException;

    /**
     *
     * @param queue The queue
     * @param messageSelector The selector to apply
     * @return Create a queue browser for the specified queue.
     */
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException;

    /**
     * Creates a TemporaryTopic object. Its lifetime will be that of the Connection unless it is deleted earlier.
     * JMS VIOLATION: Most probably we will violate JMS spec here since session &lt;-&gt; hedwig
     * connection and not connection &lt;-&gt; hedwig connection ...
     * This is, assuming ofcourse, that we can create temporary topic's !
     *
     * @return A temporary topic.
     * @throws JMSException In case of exceptions creating a queue.
     */
    public TemporaryTopic createTemporaryTopic() throws JMSException;


    /**
     * Creates a TemporaryQueue object. Its lifetime will be that of the Connection unless it is deleted earlier.
     * JMS VIOLATION: Most probably we will violate JMS spec here since session &lt;-&gt; hedwig
     * connection and not connection &lt;-&gt; hedwig connection ...
     * This is, assuming ofcourse, that we can create temporary topic's !
     *
     * @return A temporary queue.
     * @throws JMSException In case of exceptions creating a queue.
     */
    public TemporaryQueue createTemporaryQueue() throws JMSException;

    /**
     * Starts a durable subscription for a client.
     *
     * @param topicName The topic name
     * @param subscribedId The subscription subscribedId
     * @throws JMSException In case of exceptions unsubscribing.
     */
    public void subscribeToTopic(String topicName, String subscribedId) throws JMSException;

    /**
     * Unsubscribes a durable subscription that has been created by a client.
     * <p/>
     * This method deletes the state being maintained on behalf of the subscriber by its provider.
     * <p/>
     * <br/>
     * Note that it is erroneous for a client to delete a durable subscription while there is an
     * active MessageConsumer or TopicSubscriber for the subscription,
     * or while a consumed message is part of a pending transaction or has not been acknowledged in the session.
     * <br/> <b>This validation MUST be done BEFORE invoking this method !</b>
     *
     * @param topicName The topic name
     * @param subscribedId The subscription subscribedId
     * @throws JMSException In case of exceptions unsubscribing.
     */
    public void unsubscribeFromTopic(String topicName, String subscribedId) throws JMSException;

    /**
     * Starts delivery of messages from a Topic. <br/>
     *
     * @param topicName The topic name
     * @param subscribedId The subscription subscribedId
     * @throws JMSException In case of exceptions unsubscribing.
     */
    public void startTopicDelivery(String topicName, String subscribedId) throws JMSException;

    /**
     * Starts delivery of messages from a Queue. <br/>
     *
     * @param queueName The queue name
     * @param subscriberId The subscription subscribedId
     * @throws JMSException In case of exceptions unsubscribing.
     */
    public void startQueueDelivery(String queueName, String subscriberId) throws JMSException;

    /**
     * Stops delivery of messages from a Topic. <br/>
     *
     * @param topicName The topic name
     * @param subscribedId The subscription subscribedId
     * @throws JMSException In case of exceptions unsubscribing.
     */
    public void stopTopicDelivery(String topicName, String subscribedId) throws JMSException;

    /**
     * Stops delivery of messages from a Queue. <br/>
     *
     * @param queueName The queue name
     * @param subscribedId The subscription subscribedId
     * @throws JMSException In case of exceptions unsubscribing.
     */
    public void stopQueueDelivery(String queueName, String subscribedId) throws JMSException;

    /**
     * Register an unacknowledged message. This is to be used when session is going to NOT manage
     * acknowledgements and
     * expects clients to explicitly call message.acknowledge().
     * This is true when : session is not transacted and it is in CLIENT_ACKNOWLEDGE mode.
     *
     * Note that invocation of this method MUST be in the order it was received from the server :
     * since hedwig does ack-until-N.
     *
     * @param message The un-ack message.
     */
    public void registerUnAcknowledgedMessage(SessionImpl.ReceivedMessage message);

    /**
     * Acknowledge the jms message to hedwig.
     *
     * @param message The message to acknowledge.
     * @throws javax.jms.JMSException In case of internal errors while sending acknowledgement to hedwig
     */
    public void acknowledge(MessageImpl message) throws JMSException;

    /**
     * Get the subscriber id of the TopicSubscriber.
     * @param topicSubscriber This must be a topic subscriber created using this facade.
     * @return The subscriber id of the subscriber.
     * @throws JMSException Typically in case this is NOT an instance compatible with this facade.
     */
    public String getSubscriberId(TopicSubscriber topicSubscriber) throws JMSException;

    /**
     * Get the subscriber id of the QueueReceiver.
     * @param queueReceiver  This must be a queue receiver created using this facade.
     * @return The subscriber id of the subscriber.
     * @throws JMSException Typically in case this is NOT an instance compatible with this facade.
     */
    public String getSubscriberId(QueueReceiver queueReceiver) throws JMSException;

    /**
     * Enqueue a message for consumption by the subscriber.
     * This happens when there are one or more 'recieve()' calls possible.
     * <p/>
     * The typically flow is : client DOES NOT use <br/>
     * {@link javax.jms.Session#setMessageListener(javax.jms.MessageListener)}<br/>
     * but directly creates One or more Subscribers via (For Topics) :<br/>
     * {@link javax.jms.Session#createConsumer(javax.jms.Destination)},<br/>
     * {@link javax.jms.Session#createConsumer(javax.jms.Destination, String)},<br/>
     * {@link javax.jms.Session#createConsumer(javax.jms.Destination, String, boolean)},<br/>
     * Or directly using Topic api using.<br/>
     * {@link javax.jms.TopicSession#createSubscriber(javax.jms.Topic)},<br/>
     * {@link javax.jms.TopicSession#createSubscriber(javax.jms.Topic, String, boolean)},<br/>
     * {@link javax.jms.TopicSession#createDurableSubscriber(javax.jms.Topic, String)},<br/>
     * {@link javax.jms.TopicSession#createDurableSubscriber(javax.jms.Topic, String, String, boolean)}<br/>
     * <p/>
     * The message is the enqueued in the subscriber for subsequent consumption by<br/>
     * {@link javax.jms.TopicSubscriber#receive()} or variants,<br/>
     * {@link javax.jms.MessageConsumer#receive()} or variants.<br/>
     *
     * @param subscriber The subscriber of the message.
     * @param receivedMessage The message to dispatch.
     * @param addFirst Add to begining of the received list or at end. (usually addFirst == true for
     * txn rollback recovery).
     * @throws javax.jms.JMSException If not a valid subscriber (for now).
     * @return Was the message successfully enqueud to the subscriber. Typically fails if already closed.
     */
    public boolean enqueueReceivedMessage(MessageConsumer subscriber, SessionImpl.ReceivedMessage receivedMessage,
                                          boolean addFirst) throws JMSException;


    /**
     *
     * Publish a message to the topic specified.
     *
     * @param topicName The topic to publish to.
     * @param message The message to send.
     * @throws JMSException In case of errors publishing message.
     * @return The message-id to be set as JMSMessageID
     */
    public String publish(String topicName, MessageImpl message) throws JMSException;
}
