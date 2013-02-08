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

import org.apache.hedwig.jms.message.BytesMessageImpl;
import org.apache.hedwig.jms.message.MapMessageImpl;
import org.apache.hedwig.jms.message.MessageImpl;
import org.apache.hedwig.jms.message.MessageUtil;
import org.apache.hedwig.jms.message.ObjectMessageImpl;
import org.apache.hedwig.jms.message.StreamMessageImpl;
import org.apache.hedwig.jms.message.TextMessageImpl;
import org.apache.hedwig.jms.selector.Node;
import org.apache.hedwig.jms.selector.SelectorParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;
import javax.jms.TransactionRolledBackException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of jms Session.
 * IS NOT MT-safe (2.8) - except for close()
 *
 * We are yet to support/implement this - must pass a flag through constructor on "how" this object
 * was created and use that to throw exception...
 *
 */
public class SessionImpl implements Session {


    private final static Logger logger = LoggerFactory.getLogger(SessionImpl.class);

    // 8k, too high ?
    public static final int MAX_SESSION_BUFFERED_MESSAGES =
        Integer.getInteger("Session.MAX_BUFFERED_MESSAGES", 1024 * 8);
    // 0.5k too low/high ?
    public static final int MAX_SUBSCRIBER_BUFFERED_MESSAGES =
        Integer.getInteger("Session.MAX_SUBSCRIBER_BUFFERED_MESSAGES", 512);

    // Number of attempts to retry and see if a transaction keeps getting rolled back as part
    // of async delivery of messages.
    public static final int RETRY_DISPATCH_TO_TRANSACTION_ATTEMPTS =
        Integer.getInteger("Session.RETRY_DISPATCH_TO_TRANSACTION_ATTEMPTS", 9);

    private final boolean transacted;
    private final int acknowledgeMode;
    private volatile MessageListener messageListener = null;

    private final ConnectionImpl connection;
    private final MessagingSessionFacade sessionFacade;

    private final Object lockObject = new Object();
    // Message processing locks on this object itself - everything else on lockObject. This is to
    // prevent interactions with hedwig threading idioms.
    // messageList is a leaf in call graph - so it must not cause MT interactions with other locks
    // acquired prior to it.
    private final List<ReceivedMessage> messageList = new LinkedList<ReceivedMessage>();
    private final List<TransactedReceiveOperation> rolledbackMessageList
        = new LinkedList<TransactedReceiveOperation>();

    private StateManager sessionState = new StateManager(StateManager.State.STOPPED, lockObject);

    // Simply encapsulating all state within a single class.
    private final Subscriptions subscriptions = new Subscriptions();

    private boolean messageListenerThreadStarted = false;
    private final Thread messageListenerThread;
    private boolean messageListenerThreadFinished = false;

    public SessionImpl(ConnectionImpl connection, boolean transacted, int acknowledgeMode) throws JMSException {
        if (Session.AUTO_ACKNOWLEDGE != acknowledgeMode &&
            Session.CLIENT_ACKNOWLEDGE != acknowledgeMode &&
            Session.DUPS_OK_ACKNOWLEDGE != acknowledgeMode){
            // On;y if not transacted !
            if (!transacted){
                throw new javax.jms.IllegalStateException("Unknown/unsupported acknowledgeMode specified : " +
                    acknowledgeMode);
            }
        }
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;
        this.connection = connection;
        this.sessionFacade = connection.createMessagingSessionFacade(this);
        this.messageListenerThread = new Thread(this, "JMS message listener thread");
        // not daemon, right ?
        this.messageListenerThread.setDaemon(false);
    }

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        return new BytesMessageImpl(this);
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        return new MapMessageImpl(this);
    }

    @Override
    public Message createMessage() throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        return new MessageImpl(this);
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        return new ObjectMessageImpl(this, null);
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        return new ObjectMessageImpl(this, serializable);
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        return new StreamMessageImpl(this);
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        return new TextMessageImpl(this);
    }

    @Override
    public TextMessage createTextMessage(String payload) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        return new TextMessageImpl(this, payload);
    }

    @Override
    public boolean getTransacted() {
        return transacted;
    }

    @Override
    public int getAcknowledgeMode() {
        return acknowledgeMode;
    }

    @Override
    public void commit() throws JMSException {
        // Apparently, we can send even if connection is not open ?
        // if (!sessionState.isStarted()) throw new javax.jms.IllegalStateException("Session not open");
        if (!getTransacted()) throw new javax.jms.IllegalStateException("Session not transacted");
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");

        commitTransactionState();
    }

    @Override
    public void rollback() throws JMSException {
        if (!sessionState.isStarted()) throw new javax.jms.IllegalStateException("Session not open");
        if (!getTransacted()) throw new javax.jms.IllegalStateException("Session not transacted");
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");

        rollbackTransactionState();
    }

    void start() throws JMSException {
        final StateManager.State prevState;
        final Map<Subscription, CopyOnWriteArrayList<MessageConsumer>> subscriptionToSubscriberMapCopy;

        if (logger.isTraceEnabled()) logger.trace("Attempting to start session");

        synchronized (lockObject){
            // Do not throw exception, it might be connection starting while another thread might
            // be doing a close() - there is a
            // potential race there !
            // if (isClosed()) throw new javax.jms.IllegalStateException("Already closed");
            if (isClosed()) return ;
            if (sessionState.isStarted()) return ;

            if (sessionState.isTransitionState()){
                sessionState.waitForTransientStateChange(StateManager.WAIT_TIME_FOR_TRANSIENT_STATE_CHANGE, logger);
                // Not expected actually, present to guard against future changes ...
                if (sessionState.isTransitionState())
                  throw new JMSException("Connection did not make state change to steady state ?");

                if (isClosed()) throw new javax.jms.IllegalStateException("Already closed");
                if (sessionState.isStarted()) return ;

                assert sessionState.isStopped();
                // try again ...
            }

            prevState = sessionState.getCurrentState();
            sessionState.setCurrentState(StateManager.State.STARTING);

            // Copy to prevent concurrent mod exceptions - is it required here ? Not sure ...
            subscriptionToSubscriberMapCopy = subscriptions.createSubscriptionToSubscriberMapCopy();
        }

        StateManager.State nextState = prevState;

        try {
            rollbackTransactionState();
            // Note: this part of the code IS thread-safe for our private state.

            // Validate state - in terms of listener's, etc : we are relying on the single thread semantics of JMS
            // to NOT do any complex locking, etc.

            if (null != getMessageListener()){
                // There CANNOT be any subscriber with listeners registered.
                for (MessageConsumer consumer : subscriptions.getAllConsumersSet()){
                    if (null != consumer.getMessageListener()) {
                        throw new JMSException("Session's message listener is already set - " +
                            "cannot have a consumer with listener also set.");
                    }
                }
            }


            sessionFacade.start();

            if (logger.isTraceEnabled()) logger.trace("Starting " + subscriptionToSubscriberMapCopy.size() +
                " subscribers");

            // Subscribe to all the subscriberId's
            for (Map.Entry<Subscription, CopyOnWriteArrayList<MessageConsumer>> entry :
                    subscriptionToSubscriberMapCopy.entrySet()){

                if (entry.getValue().isEmpty()) continue;
                if (entry.getKey().isTopic()){
                    TopicSubscription topicSubscription = (TopicSubscription) entry.getKey();
                    try {
                        sessionFacade.subscribeToTopic(topicSubscription.topicName, topicSubscription.subscriberId);
                    } catch (JMSException e) {
                        // Log and ignore
                        // This CAN fail, it is ok to fail !
                        if (logger.isDebugEnabled()) {
                            logger.debug("(Potentially Benign error) Error subscribing from topic for entry : " +
                                topicSubscription);
                            DebugUtil.dumpJMSStacktrace(logger, e);
                        }
                    }
                    try {
                        sessionFacade.startTopicDelivery(topicSubscription.topicName,
                            topicSubscription.subscriberId);
                    } catch (JMSException e) {
                        // Log and ignore
                        if (logger.isDebugEnabled()) {
                            logger.debug("Error starting topic delivery for entry : " + entry.getKey());
                            DebugUtil.dumpJMSStacktrace(logger, e);
                        }
                    }
                }
                else {
                    assert entry.getKey().isQueue();

                    QueueSubscription queueSubscription = (QueueSubscription) entry.getKey();

                    // There is no notion like subscription to queue, right ?
                    /*
                    try {
                        sessionFacade.subscribeToQueue(queueSubscription.queueName,
                          queueSubscription.subscriberId);
                    } catch (JMSException e) {
                        // Log and ignore
                        // This CAN fail, it is ok to fail !
                        if (logger.isDebugEnabled()) {
                            logger.debug("(Potentially Benign error) Error subscribing from queue for entry : " +
                              queueSubscription);
                            Util.dumpJMSStacktrace(logger, e);
                        }
                    }
                    */
                    try {
                        sessionFacade.startQueueDelivery(queueSubscription.queueName, queueSubscription.subscriberId);
                    } catch (JMSException e) {
                        // Log and ignore
                        if (logger.isDebugEnabled()) {
                            logger.debug("Error starting queue delivery for entry : " + entry.getKey());
                            DebugUtil.dumpJMSStacktrace(logger, e);
                        }
                    }
                }
            }
            nextState = StateManager.State.STARTED;
        } finally {
            // set status and notify.
            synchronized (lockObject){
                lockObject.notifyAll();
                sessionState.setCurrentState(nextState);
            }
        }
    }

    void stop() throws JMSException {
        final StateManager.State prevState;
        final Map<Subscription, CopyOnWriteArrayList<MessageConsumer>> subscriptionToSubscriberMapCopy;

        if (logger.isTraceEnabled()) logger.trace("Attempting to stop connection");

        synchronized (lockObject){
            if (isClosed()) throw new javax.jms.IllegalStateException("Already closed");
            if (sessionState.isStopped()) return ;

            if (sessionState.isTransitionState()){
                sessionState.waitForTransientStateChange(StateManager.WAIT_TIME_FOR_TRANSIENT_STATE_CHANGE, logger);
                // Not expected actually, present to guard against future changes ...
                if (sessionState.isTransitionState())
                  throw new JMSException("Connection did not make state change to steady state ?");

                if (isClosed()) throw new javax.jms.IllegalStateException("Already closed");
                if (sessionState.isStopped()) return ;

                assert sessionState.isStarted();

                // try again ...
            }

            prevState = sessionState.getCurrentState();
            sessionState.setCurrentState(StateManager.State.STOPPING);

            // Copy to prevent concurrent mod exceptions.
            subscriptionToSubscriberMapCopy = subscriptions.createSubscriptionToSubscriberMapCopy();
        }

        StateManager.State nextState = prevState;
        try {
            rollbackTransactionState();
            // In case there are other things to be done ...

            // Unsubscribe to all the subscriberId's
            if (logger.isTraceEnabled()) logger.trace("Stopping " +
                subscriptionToSubscriberMapCopy.size() + " subscribers");
            for (Map.Entry<Subscription, CopyOnWriteArrayList<MessageConsumer>> entry :
                    subscriptionToSubscriberMapCopy.entrySet()){

                if (entry.getValue().isEmpty()) continue;
                if (entry.getKey().isTopic()){
                    TopicSubscription topicSubscription = (TopicSubscription) entry.getKey();
                    try {
                        stopTopicDelivery(topicSubscription.topicName, topicSubscription.subscriberId);
                    } catch (JMSException e) {
                        // Log and ignore
                        if (logger.isDebugEnabled()) {
                            logger.debug("Error unsubscribing from topic for entry : " + topicSubscription);
                            DebugUtil.dumpJMSStacktrace(logger, e);
                        }
                    }
                }
                else {
                  assert entry.getKey().isQueue();

                  QueueSubscription queueSubscription = (QueueSubscription) entry.getKey();
                  try {
                      stopQueueDelivery(queueSubscription.queueName, queueSubscription.subscriberId);
                  } catch (JMSException e) {
                      // Log and ignore
                      if (logger.isDebugEnabled()) {
                          logger.debug("Error unsubscribing from queue for entry : " + queueSubscription);
                          DebugUtil.dumpJMSStacktrace(logger, e);
                      }
                  }
                }
            }
            // stop facade AFTER subscriber's are stopped.
            sessionFacade.stop();
            nextState = StateManager.State.STOPPED;
        } finally {
            synchronized (lockObject){
                lockObject.notifyAll();
                sessionState.setCurrentState(nextState);
            }
        }
    }


    /**
     *
     * Closes the session. <br/>
     * Since a provider may allocate some resources on behalf of a session outside the JVM, clients
     * should close the resources
     * when they are not needed. Relying on garbage collection to eventually reclaim these resources
     * may not be timely enough.<br/>
     * <p/>
     * There is no need to close the producers and consumers of a closed session.
     * <p/>
     *
     *
     * A blocked message consumer receive call returns null when this session is closed.
     * <p/>
     * Closing a transacted session must roll back the transaction in progress.<br/>
     * This method is the only Session method that can be called concurrently.<br/>
     * Invoking any other Session method on a closed session must throw a JMSException.IllegalStateException.<br/>
     * Closing a closed session must not throw an exception.<br/>
     *
     */
    private static final ThreadLocal<Boolean> closeFromWithinListener = new ThreadLocal<Boolean>(){
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    @Override
    public void close() throws JMSException {
        final StateManager.State prevState;
        final Set<MessageConsumer> subscriberSetCopy = Collections.newSetFromMap(
            new IdentityHashMap<MessageConsumer, Boolean>());

        if (logger.isTraceEnabled()) logger.trace("Attempting to close session");

        synchronized (lockObject){
            if (isClosed()) return ;
            if (! sessionState.isStopped()) {
                if (sessionState.isTransitionState()){
                    sessionState.waitForTransientStateChange(StateManager.WAIT_TIME_FOR_TRANSIENT_STATE_CHANGE, logger);
                    // Not expected actually, present to guard against future changes ...
                    if (sessionState.isTransitionState())
                      throw new JMSException("Connection did not make state change to steady state ?");

                    if (isClosed()) return ;

                    assert sessionState.isStarted() || sessionState.isStopped();
                }
            }

            prevState = sessionState.getCurrentState();
            sessionState.setCurrentState(StateManager.State.CLOSING);
            // Copy to prevent concurrent mod exceptions.
            subscriberSetCopy.addAll(subscriptions.getAllConsumersSet());
        }

        StateManager.State nextState = prevState;

        try {
            rollbackTransactionState();
            connection.removeSession(this);

            // Close all publishers - doing this within synchronized block to prevent any possibility
            // of race conditions.
            // Potentially expensive, but it is a tradeoff between correctness and performance :-(
            if (logger.isTraceEnabled()) logger.trace("Closing " + subscriberSetCopy.size() + " subscribers");
            for (MessageConsumer subscriber : subscriberSetCopy){
                try {
                    subscriber.close();
                } catch (JMSException e) {
                    // Log and ignore
                    if (logger.isDebugEnabled()) {
                        logger.debug("Error unsubscribing from destination for entry : " + subscriber);
                        DebugUtil.dumpJMSStacktrace(logger, e);
                    }
                }
            }
            sessionFacade.close();
            nextState = StateManager.State.CLOSED;
        } finally {

            // set status and notify.
            synchronized (lockObject){
                lockObject.notifyAll();
                sessionState.setCurrentState(nextState);
            }
        }

        if (logger.isTraceEnabled()) logger.trace(this + "Waiting for messageListenerThreadStarted " +
            messageListenerThreadStarted + ", messageListenerThreadFinished " + messageListenerThreadFinished);

        // spin on messageListenerThreadFinished
        // spin ONLY if we are NOT within the listener already !
        if (! closeFromWithinListener.get()){
            synchronized (lockObject){
                long waitTime = 100;
                long retryCount = StateManager.WAIT_TIME_FOR_TRANSIENT_STATE_CHANGE / waitTime;
                if (messageListenerThreadStarted) {
                    while (!messageListenerThreadFinished) {
                        try {
                            lockObject.wait(waitTime);
                        } catch (InterruptedException e) {
                            // ignore ...
                            if (logger.isDebugEnabled()) logger.debug("interrupted ?", e);
                        }
                        retryCount --;
                        // Fail if we have waiting long enough ... hardcoded for now.
                        if (retryCount <= 0) break;
                    }
                }
            }
        }

        if (logger.isTraceEnabled()) logger.trace("Waiting for messageListenerThreadFinished " +
            messageListenerThreadFinished + " DONE");
    }

    @Override
    public void recover() throws JMSException {
        // Typically will be in stopped state.
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        if (getTransacted())
          throw new javax.jms.IllegalStateException("cannot invoke recover in transacted session.");
        throw new JMSException("recovery : TODO");
    }

    @Override
    public MessageListener getMessageListener() {
        return messageListener;
    }

    @Override
    public void setMessageListener(final MessageListener messageListener) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        // Explicitly forbidding it for now : leads to too many complexities otherwise.
        // start session ONLY AFTER you have set the listener.
        if (messageListener != this.messageListener && sessionState.isInStartMode()) {
            throw new JMSException("Set the message listener BEFORE starting session (and/or connection)");
        }

        this.messageListener = messageListener;
    }

    @Override
    public void run() {
        if (logger.isTraceEnabled()) logger.trace("Session thread started");
        try {
            while (!isClosed()){
                final List<ReceivedMessage> messageListCopy;
                final List<TransactedReceiveOperation> rolledbackMessageListCopy;
                final MessageListener msglistener;
                synchronized (lockObject){
                    while (!isClosed() &&
                            (!sessionState.isStarted() ||
                                (null == getMessageListener() && 0 == subscriptions.getNumSubscribers()) ||
                                (messageList.isEmpty() && rolledbackMessageList.isEmpty())
                            )
                        ) {
                        // Check for buffer over-run's due to no listener being available !
                        if (messageList.size() > MAX_SESSION_BUFFERED_MESSAGES){
                            // simply discard it with an error logged.
                            if (logger.isInfoEnabled()) logger.info("Discarding " + messageList.size() +
                                " messages since there are no consumers for them");
                            messageList.clear();
                        }
                        // Check for buffer over-run's due to no listener being available !
                        if (rolledbackMessageList.size() > MAX_SESSION_BUFFERED_MESSAGES){
                            // simply discard it with an error logged.
                            if (logger.isInfoEnabled()) logger.info("Discarding " + rolledbackMessageList.size() +
                                " messages since there are no consumers for them from recovered list.");
                            rolledbackMessageList.clear();
                        }

                        if (logger.isTraceEnabled()) logger.trace(this + "sessionState : " + sessionState +
                            ", listener " + getMessageListener() + ", numSubscribers : " +
                            subscriptions.getNumSubscribers());
                        try {
                            lockObject.wait(500);
                        } catch (InterruptedException e) {
                            // Should we ignore this ? There is no way this thread can be interrupted currently -
                            // while closing it will cause issues !
                            // Log and forget
                            if (logger.isDebugEnabled()) {
                                logger.debug("Ignoring interrupted exception", e);
                            }
                        }
                    }

                    if (isClosed()) break;
                    msglistener = getMessageListener();
                    messageListCopy = new ArrayList<ReceivedMessage>(messageList);
                    rolledbackMessageListCopy = new ArrayList<TransactedReceiveOperation>(rolledbackMessageList);
                    messageList.clear();
                    rolledbackMessageList.clear();
                    assert subscriptions.getNumSubscribers() > 0 || null != msglistener;
                }

                if (logger.isTraceEnabled()) logger.trace("Processing " + messageListCopy.size() +
                    " messages using listener ? " + (null != msglistener));

                dispatchReceivedMessagesToSubscribers(msglistener, messageListCopy, rolledbackMessageListCopy);
            }
        } finally {
            String msg = null;
            synchronized (lockObject){
                lockObject.notifyAll();
                messageListenerThreadFinished = true;
                if (logger.isTraceEnabled()) msg = "Exiting thread and setting " +
                    messageListenerThreadFinished;
            }
            if (logger.isTraceEnabled()) logger.trace(msg);
        }
    }

    private void dispatchReceivedMessagesToSubscribers(MessageListener sessionMessageListener,
                                                       List<ReceivedMessage> messageListCopy,
                                                       List<TransactedReceiveOperation> rolledbackMessageListCopy) {
        assert null != messageListCopy;

        // Doing it before processing messageList.
        handleRollbackInDispatch(rolledbackMessageListCopy);

        for (final ReceivedMessage receivedMessage : messageListCopy){

            if (isClosed()) break;

            // It is possible that previous listener rolledback transaction ... check that before
            // delivering the other messages !
            // Else we will mess up the oder of message delivery.
            {
                int retryCount = 0;
                while (retryCount < RETRY_DISPATCH_TO_TRANSACTION_ATTEMPTS){
                    if (! handleRollbackInDispatch(null)) break;
                    retryCount ++;
                }
                if (RETRY_DISPATCH_TO_TRANSACTION_ATTEMPTS == retryCount){
                    // we cant do much - close session and abort.
                    try {
                        SessionImpl.this.close();
                    } catch (JMSException e) {
                        if (logger.isDebugEnabled()) logger.debug("Exception closing session", e);
                    }
                    return ;
                }
            }

            final Subscription subscription = createSubscription(receivedMessage.destinationType,
                receivedMessage.originalMessage.getSourceName(), receivedMessage.originalMessage.getSubscriberId());

            // COW - so no need to worry about concurrent-mod's or inconsistent states - other than
            // potential stale state,
            // which is fine since MessageConsumer's are essentially immutable from basic state point
            // of view (subscriberId, destination).
            CopyOnWriteArrayList<? extends MessageConsumer> subscriberList =
                subscriptions.getSubscribers(subscription);
            if (null == subscriberList) continue;

            if (! subscriberList.listIterator().hasNext()) continue;

            // For selector support - pick up the last register
            Node ast = subscriptions.getSelectorExpression(subscription);
            if (logger.isTraceEnabled()) logger.trace("subscription : " + subscription + ", selector : " + ast);
            if (null != ast){
                // final Boolean value = SelectorParser.evaluateSelector(ast, receivedMessage.originalMessage);
                final Boolean value = SelectorParser.evaluateSelector(ast, receivedMessage.msg);

                if (null == value){
                    if (logger.isDebugEnabled()) {
                        logger.debug("Unable to evaluate selector ? ... ignoring message");
                        logger.debug("Message : " + receivedMessage.msg);
                    }
                    receivedMessage.originalMessage.getAckRunnable().run();
                    continue;
                }
                if (! Boolean.TRUE.equals(value)){
                    if (logger.isTraceEnabled()) logger.trace("Selector DID NOT evaluate to true (" +
                        value + "), ignore message ignoring message");
                    receivedMessage.originalMessage.getAckRunnable().run();
                    continue;
                }
            }


            if (null != sessionMessageListener){
                // Since there was atleast one subscriber when we started this loop (which might
                // not be case anymore, but that is just an uncontrollable harmless race)
                // we can send it to messageListener for the session.
                if (logger.isTraceEnabled()) logger.trace("Dispatching " + receivedMessage.originalMessage +
                    " to session listener");

                if (isMessageExpired(receivedMessage.originalMessage)){
                    // message already expired.
                    // This means we acknowledge for all subscribers with this subscription id ...
                    receivedMessage.originalMessage.getAckRunnable().run();
                    continue;
                }

                try {
                    final MessageImpl message = MessageUtil.createCloneForDispatch(this,
                        receivedMessage.originalMessage, receivedMessage.originalMessage.getSourceName(),
                        receivedMessage.originalMessage.getSubscriberId());
                    deliverToListener(sessionMessageListener, receivedMessage, message, false);
                } catch (JMSException e) {
                    // Unexpected not to be able to clone ...
                    if (logger.isDebugEnabled()) {
                        logger.debug("Unexpected exception trying to process message");
                        DebugUtil.dumpJMSStacktrace(logger, e);
                    }
                }
                continue;
            }

            for (final MessageConsumer subscriber : subscriberList){
                if (isClosed()) break;
                try {
                    final MessageListener subscriberListener = subscriber.getMessageListener();
                    // Clone - since each subscrber can modify the message.  We are optimizing this
                    // to clone only if subscriberList
                    // has more than one subscriber to avoid the (potentially) expensive creation.
                    if (getNoLocal(subscription, subscriber)){
                        if (isLocallyPublished(receivedMessage.originalMessage.getJMSMessageID())){
                            // This means we acknowledge for all subscribers with this subscription id ...
                            receivedMessage.originalMessage.getAckRunnable().run();
                            continue;
                        }
                    }
                    if (isMessageExpired(receivedMessage.originalMessage)){
                        receivedMessage.originalMessage.getAckRunnable().run();
                        continue;
                    }

                    final MessageImpl message = MessageUtil.createCloneForDispatch(this,
                        receivedMessage.originalMessage, receivedMessage.originalMessage.getSourceName(),
                        receivedMessage.originalMessage.getSubscriberId());

                    if (logger.isTraceEnabled()) logger.trace("Dispatching " + message +
                        " to subscriber subscriberListener ? " + (subscriberListener != null));

                    if (null != subscriberListener) {
                        deliverToListener(subscriberListener, receivedMessage, message, false);
                    }
                    else {
                        sessionFacade.enqueueReceivedMessage(subscriber,
                            new ReceivedMessage(receivedMessage.originalMessage, message,
                                receivedMessage.destinationType), false);
                    }

                    if (logger.isTraceEnabled()) logger.trace("Dispatching " + message +
                        " to subscriberListener ? " + (subscriberListener != null) + ", DONE");
                } catch (JMSException e) {
                    // Unexpected not to be able to clone ...
                    if (logger.isDebugEnabled()) {
                        logger.debug("Unexpected exception trying to process message", e);
                    }
                    continue ;
                }
            }
        }

        if (logger.isTraceEnabled()) logger.trace("dispatchReceivedMessagesToSubscribers() DONE");
    }

    private boolean getNoLocal(Subscription subscription, MessageConsumer subscriber) throws JMSException {
        if (subscription.isTopic()) return ((TopicSubscriber) subscriber).getNoLocal();
        // nothing equivalent for queue.
        // if (subscription.isQueue()) return ((QueueReceiver) subscriber).getNoLocal();
        return false;
    }

    // Note that rollback can happen WHILE a listener is being run - so we need to check this
    // between EACH message delivery :-(
    // Not just as part of block draining of the queue.
    // returns true if there was any async operation to rollback (specifically async !).
    private boolean handleRollbackInDispatch(List<TransactedReceiveOperation> rolledbackMessageListCopy) {
        if (null == rolledbackMessageListCopy) {
            // Attempt to drain the queue.
            synchronized (lockObject){
                if (rolledbackMessageList.isEmpty()) return false;
                rolledbackMessageListCopy = new ArrayList<TransactedReceiveOperation>(rolledbackMessageList);
                rolledbackMessageList.clear();
            }
        }

        if (logger.isTraceEnabled()) logger.trace("rolledbackMessageList (" +
            rolledbackMessageListCopy.size() + ") ... " + rolledbackMessageListCopy);

        LinkedList<TransactedReceiveOperation> listenerDeliveryList = new LinkedList<TransactedReceiveOperation>();
        for (TransactedReceiveOperation receiveOp : rolledbackMessageListCopy){
            if (isClosed()) break;
            receiveOp.recover(listenerDeliveryList);
        }
        for (TransactedReceiveOperation receiveOp : listenerDeliveryList){
            if (isClosed()) break;
            receiveOp.recoverForListener();
        }

        return listenerDeliveryList.size() > 0;
    }

    private void deliverToListener(MessageListener sessionMessageListener, ReceivedMessage receivedMessage,
                                   MessageImpl theMessage, boolean redelivery) {

      // NOT re-enterent method ...
        closeFromWithinListener.set(true);
        try {
            int errorRetry = 0;
            boolean success = false;
            final int retryFor =
                    (!getTransacted() &&
                            (Session.AUTO_ACKNOWLEDGE == getAcknowledgeMode() ||
                                    Session.CLIENT_ACKNOWLEDGE == getAcknowledgeMode()))
                    ? 3 : 1;

            while (errorRetry < retryFor && !isClosed()){
                try {
                    if (redelivery || errorRetry > 0) theMessage.setJMSRedelivered(true);

                    // Changed my mind, always ack the message before processing it. This seems to
                    // be consistent with activemq testcases too ...
                    handleAutomaticMessageAcknowledgement(receivedMessage, sessionMessageListener);
                    sessionMessageListener.onMessage(theMessage);
                    success = true;
                    break ;
                } catch (RuntimeException rEx){
                    // Badly behaved client, retry ...
                    if (logger.isInfoEnabled())
                      logger.info("Unexpected runtime exception from client message listener.", rEx);
                }
                errorRetry ++;
            }

            if (isClosed()) return ;

            if (!success){
                // If failed, then reset transaction state - so that next txn will not get affected by this.
                rollbackTransactionState();
                // We gave up deliverying message ...
                if (retryFor > 1) {
                    if (logger.isInfoEnabled())
                      logger.info("Delivery of message to listener resulted in repeated failures, " +
                          " dropping message -  session recovery should be used to handle it.");
                }
                else {
                    if (logger.isInfoEnabled()) logger.info("Use session recovery to handle message");
                }
            }
        } finally {
            closeFromWithinListener.remove();
        }
    }

    public MessagingSessionFacade.DestinationType findDestinationType(String destination) throws JMSException {
        return sessionFacade.findDestinationType(destination);
    }

    public MessagingSessionFacade.DestinationType findDestinationType(Destination destination) throws JMSException {
        return sessionFacade.findDestinationType(destination);
    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        connection.initConnectionClientID();

        return createProducerImpl(findDestinationType(destination), destination);
    }

    private MessageProducer createProducerImpl(MessagingSessionFacade.DestinationType type,
                                               Destination destination) throws JMSException {
        switch (type){
            case QUEUE:
                return sessionFacade.createQueueSender(destination);
            case TOPIC:
                return sessionFacade.createTopicPublisher(destination);
            default:
                throw new JMSException("Unable to find destination type " + destination +
                    ", please use explicit queue/topic methods to create producer");
        }
    }

    // delegate to this IF this method can be invoked (specifically, if not QueueSession)
    protected TopicPublisher createPublisherImpl(Topic topic) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        if (null == topic) throw new InvalidDestinationException("Illegal destination");
        connection.initConnectionClientID();

        return (TopicPublisher) createProducerImpl(MessagingSessionFacade.DestinationType.TOPIC, topic);
    }


    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        if (null == destination) throw new InvalidDestinationException("Illegal destination");
        connection.initConnectionClientID();

        return createConsumerImpl(findDestinationType(destination), destination);
    }

    private MessageConsumer createConsumerImpl(MessagingSessionFacade.DestinationType type,
                                               Destination destination) throws JMSException {
        switch (type){
            case QUEUE:
                return sessionFacade.createQueueReceiver(destination);
            case TOPIC:
                return sessionFacade.createTopicSubscriber(destination);
            default:
                throw new JMSException("Unable to find destination type " + destination +
                    ", please use explicit queue/topic methods to create consumer");
        }
    }

    // delegate to this IF this method can be invoked (specifically, if not QueueSession)
    protected TopicSubscriber createSubscriberImpl(Topic topic) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        if (null == topic) throw new InvalidDestinationException("Illegal destination");
        connection.initConnectionClientID();

        return (TopicSubscriber) createConsumerImpl(MessagingSessionFacade.DestinationType.TOPIC, topic);
    }

    // delegate to this IF this method can be invoked (specifically, if not QueueSession)
    protected TopicSubscriber createSubscriberImpl(Topic topic, String messageSelector, boolean noLocal)
        throws JMSException {

        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        if (null == topic) throw new InvalidDestinationException("Illegal destination");
        connection.initConnectionClientID();

        return (TopicSubscriber) createConsumerImpl(MessagingSessionFacade.DestinationType.TOPIC,
            topic, messageSelector, noLocal);
    }


    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        if (null == destination) throw new InvalidDestinationException("Illegal destination");
        connection.initConnectionClientID();

        return createConsumer(destination, messageSelector, false);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector,
                                          boolean noLocal) throws JMSException {

        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        if (null == destination) throw new InvalidDestinationException("Illegal destination");
        connection.initConnectionClientID();

        return createConsumerImpl(findDestinationType(destination), destination, messageSelector, noLocal);
    }

    private MessageConsumer createConsumerImpl(MessagingSessionFacade.DestinationType type, Destination destination,
                                               String messageSelector, boolean noLocal) throws JMSException {
        switch (type){
            case QUEUE:
                return sessionFacade.createQueueReceiver(destination, messageSelector, noLocal);
            case TOPIC:
                return sessionFacade.createTopicSubscriber(destination, messageSelector, noLocal);
            default:
                throw new JMSException("Unable to find destination type " + destination +
                    ", please use explicit queue/topic methods to create consumer");
        }
    }

    // TODO: Check if it is actually a Queue !
    @Override
    public Queue createQueue(String queueName) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");

        return (Queue) getDestination(MessagingSessionFacade.DestinationType.QUEUE, queueName);
    }

    // delegate to this IF this method can be invoked (specifically, if not TopicSession)
    protected QueueReceiver createReceiverImpl(Queue queue) throws JMSException {
        return sessionFacade.createQueueReceiver(queue);
    }

    // delegate to this IF this method can be invoked (specifically, if not QueueSession)
    protected QueueReceiver createReceiverImpl(Queue queue, String messageSelector) throws JMSException {
        return sessionFacade.createQueueReceiver(queue, messageSelector);
    }

    // delegate to this IF this method can be invoked (specifically, if not QueueSession)
    protected QueueSender createSenderImpl(Queue queue) throws JMSException {
        return sessionFacade.createQueueSender(queue);
    }

    // TODO: Check if it is actually a Topic !
    @Override
    public Topic createTopic(String topicName) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        if (null == topicName) throw new InvalidDestinationException("Illegal destination");
        connection.initConnectionClientID();

        return (Topic) getDestination(MessagingSessionFacade.DestinationType.TOPIC, topicName);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String subscribedId) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");

        if (null == topic) throw new InvalidDestinationException("Illegal destination");
        if (null == subscribedId) throw new JMSException("Illegal subscribedId");
        connection.initConnectionClientID();

        subscriptions.registerSubscriberIdToTopic(subscribedId, topic.getTopicName());
        return sessionFacade.createDurableSubscriber(topic, createSubscriberId(subscribedId));
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String subscribedId, String messageSelector,
                                                   boolean noLocal) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");

        if (null == topic) throw new InvalidDestinationException("Illegal destination");
        if (null == subscribedId) throw new JMSException("Illegal subscribedId");
        connection.initConnectionClientID();

        subscriptions.registerSubscriberIdToTopic(subscribedId, topic.getTopicName());
        return sessionFacade.createDurableSubscriber(topic, createSubscriberId(subscribedId),
            messageSelector, noLocal);
    }

    @Override
    public void unsubscribe(String subscribedId) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");

        final String topicName = subscriptions.findTopicNameForSubscriberId(subscribedId);
        sessionFacade.unsubscribeFromTopic(topicName, createSubscriberId(subscribedId));
    }

    public String createSubscriberId(final String subscribedId) throws JMSException {
        final String clientId = connection.getClientID();
        StringBuilder sb = new StringBuilder();

        // Some arbitrary combination of client id and subscriber id.
        sb.append("CLIENT_ID:");
        sb.append(clientId);
        sb.append('|');
        sb.append("SUBSCRIBER_ID:");
        sb.append(subscribedId);

        return sb.toString();
    }

    /*
    public String createTemporaryTopicId() throws JMSException {
        final String clientId = connection.getClientID();
        StringBuilder sb = new StringBuilder();

        // Some arbitrary combination of client id and subscriber id.
        sb.append("CLIENT_ID:");
        sb.append(clientId);
        sb.append('|');
        sb.append("TOPIC_ID:");
        sb.append(generateRandomString());

        return sb.toString();
    }
    */

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");

        return sessionFacade.createBrowser(queue);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");

        return sessionFacade.createBrowser(queue, messageSelector);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");

        return sessionFacade.createTemporaryQueue();
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");

        return sessionFacade.createTemporaryTopic();
    }

    public void subscriberCreated() {
        // subscriberCreatedCount ++;
    }

    public void acknowledge(MessageImpl message) throws JMSException {
        if (sessionState.isInCloseMode()) throw new javax.jms.IllegalStateException("Already closed");
        // If NOT in explicit acknowledge mode, ignore request.
        if (Session.CLIENT_ACKNOWLEDGE != getAcknowledgeMode()) return;
        // If in transaction, ignore request.
        if (getTransacted()) return ;

        sessionFacade.acknowledge(message);
    }

    public String toName(Destination destination) throws JMSException {
        if (destination instanceof Topic) return ((Topic)destination).getTopicName();
        if (destination instanceof Queue) return ((Queue)destination).getQueueName();

        throw new javax.jms.IllegalStateException("Unknown/unsupported destination " + destination);
    }

    public static Topic asTopic(final String topicName){
        return new Topic() {
            @Override
            public String getTopicName() throws JMSException {
                return topicName;
            }

            @Override
            public String toString(){
                return topicName;
            }
        };
    }


    public static Queue asQueue(final String queueName){
        return new Queue() {
            @Override
            public String getQueueName() throws JMSException {
                return queueName;
            }

            @Override
            public String toString(){
                return queueName;
            }
        };
    }
    // TODO: Convert to JNDI lookup.
    public Destination getDestination(final MessagingSessionFacade.DestinationType type,
                                      final String destination) throws JMSException {
        switch (type){
            case TOPIC:
                return asTopic(destination);
            case QUEUE:
                return asQueue(destination);
            default:
                throw new JMSException("Unknown destination type " + type +
                    " for destination " + destination);
        }
    }

    public static String generateRandomString() {
        // UUID is expensive, but using it for now ...
        return UUID.randomUUID().toString();
    }

    public void registerTopicSubscriptionInfo(TopicSubscription topicSubscription, Node selectorAst) {
        subscriptions.registerTopicSubscriptionSelector(topicSubscription, selectorAst);
    }

    public void registerQueueSubscriptionInfo(QueueSubscription queueSubscription, Node selectorAst) {
        subscriptions.registerQueueSubscriptionSelector(queueSubscription, selectorAst);
    }

    // returns true IF we need to do an explicit subscribe to the topic (there was NO subscription to it earlier).
    public void registerTopicSubscriber(TopicSubscriber topicSubscriber) throws JMSException {
        registerSubscriber(topicSubscriber, MessagingSessionFacade.DestinationType.TOPIC,
            topicSubscriber.getTopic().getTopicName(), sessionFacade.getSubscriberId(topicSubscriber));
    }

    public void registerQueueSubscriber(QueueReceiver queueReceiver) throws JMSException {
        registerSubscriber(queueReceiver, MessagingSessionFacade.DestinationType.QUEUE,
            queueReceiver.getQueue().getQueueName(), sessionFacade.getSubscriberId(queueReceiver));
    }

    private void registerSubscriber(MessageConsumer subscriber, MessagingSessionFacade.DestinationType type,
                                   final String destination, final String subscriberId) throws JMSException {

        assert MessagingSessionFacade.DestinationType.QUEUE == type ||
            MessagingSessionFacade.DestinationType.TOPIC == type;

        boolean needSubscription = false;
        boolean needDelivery = false;
        if (logger.isTraceEnabled()) logger.trace("Registering ... " + subscriber + " for " + destination +
            ", sid " + subscriberId);

        synchronized (lockObject){
            if (sessionState.isInCloseMode()) throw new JMSException("Already closed");

            // already subscribed.
            if (! subscriptions.addToSubscriberSet(subscriber)) return ;

            if (subscriptions.addToSubscribers(subscriber, type, destination, subscriberId)) {
                // needSubscription = sessionState.isStarted();
                needSubscription = ! sessionState.isInCloseMode();
                needDelivery = sessionState.isStarted();
            }
        }

        // TODO: There is a potential race here between registering/starting subscription and
        // stopping/closing subscription(s) elsewhere.
        // We should resolve it by taking a per List lock (which is gauranteed to be non-null here)
        // and a per List subscription status.
        // For now, NOT handling crazy edge-cases like this - under most circumsances, this will
        // fail for other reasons anyway !

        // Session must be used by clients only in a thread safe manner, since it is ok to do this
        // outside the lock.
        if (needSubscription){
            if (logger.isTraceEnabled()) logger.trace("Subscribing ... " + subscriber + " for " +
                destination + ", sid " + subscriberId);

            if (MessagingSessionFacade.DestinationType.TOPIC == type){
                // Only for topic's, right ?
                try {
                    sessionFacade.subscribeToTopic(destination, subscriberId);
                } catch (JMSException e){
                    // It might be possible for this to fail ...
                    // Log and ignore
                    if (logger.isDebugEnabled()) {
                        logger.debug("Error subscribing from topic for entry : " + subscriberId);
                        DebugUtil.dumpJMSStacktrace(logger, e);
                    }
                }
            }
            if (logger.isTraceEnabled()) logger.trace("Subscribing ... " + subscriber + " for " +
                destination + ", sid " + subscriberId + " DONE");
        }

        if (needDelivery) {
            if (MessagingSessionFacade.DestinationType.TOPIC == type){
                if (logger.isTraceEnabled()) logger.trace("Topic delivery ... " + subscriber + " for " +
                    destination + ", sid " + subscriberId);
                sessionFacade.startTopicDelivery(destination, subscriberId);
                if (logger.isTraceEnabled()) logger.trace("Topic delivery ... " + subscriber + " for " +
                    destination + ", sid " + subscriberId + " DONE");
            }
            else {
                if (logger.isTraceEnabled()) logger.trace("Queue delivery ... " + subscriber + " for " +
                    destination + ", sid " + subscriberId);
                sessionFacade.startQueueDelivery(destination, subscriberId);
                if (logger.isTraceEnabled()) logger.trace("Queue delivery ... " + subscriber + " for " +
                    destination + ", sid " + subscriberId + " DONE");
            }
        }

        if (logger.isTraceEnabled()) logger.trace("registerSubscriber ... " + messageListenerThreadStarted);
        if (! messageListenerThreadStarted){
            try {
                this.messageListenerThread.start();
            } catch (IllegalThreadStateException  itse){
                // ignore
                // This should not happen, it will happen when Session is used in an MT-unsafe manner,
                // contrary to what is expected from JMS.
                if (logger.isDebugEnabled()) logger.debug("Unexpected", itse);
            }
            messageListenerThreadStarted = true;
        }
        if (logger.isTraceEnabled()) logger.trace("registerSubscriber ... DONE");
    }

    public void unregisterTopicSubscriber(TopicSubscriber topicSubscriber) throws JMSException {
        unregisterSubscriber(topicSubscriber, MessagingSessionFacade.DestinationType.TOPIC,
            topicSubscriber.getTopic().getTopicName(), sessionFacade.getSubscriberId(topicSubscriber));
    }

    public void unregisterQueueReceiver(QueueReceiver queueReceiver) throws JMSException {
        unregisterSubscriber(queueReceiver, MessagingSessionFacade.DestinationType.QUEUE,
            queueReceiver.getQueue().getQueueName(), sessionFacade.getSubscriberId(queueReceiver));
    }

    private void unregisterSubscriber(MessageConsumer subscriber, MessagingSessionFacade.DestinationType type,
                                      final String destination, final String subscriberId) throws JMSException {

        assert MessagingSessionFacade.DestinationType.QUEUE == type ||
            MessagingSessionFacade.DestinationType.TOPIC == type;
        final boolean stopDelivery;

        synchronized (lockObject){
            // if in closing, continue on anyway ...
            if (isClosed()) return ;

            stopDelivery = subscriptions.removeSubscriber(subscriber, type, destination, subscriberId);
            if (stopDelivery) {
                if (! subscriptions.getAllConsumersSet().remove(subscriber)) return ;
            }
        }

        // Session is expected to be used in a MT safe manner, since it is MT-unsafe.
        if (stopDelivery){
            if (MessagingSessionFacade.DestinationType.TOPIC == type){
                stopTopicDelivery(destination, subscriberId);
            }
            else {
                stopQueueDelivery(destination, subscriberId);
            }
        }
    }

    public void handleAutomaticMessageAcknowledgement(ReceivedMessage receivedMessage,
                                                      MessageListener sessionMessageListener) {
        doHandleAutomaticMessageAcknowledgement(new TransactedReceiveOperation(receivedMessage,
            sessionMessageListener));
    }

    public void handleAutomaticMessageAcknowledgement(ReceivedMessage receivedMessage, MessageConsumer subscriber) {
        doHandleAutomaticMessageAcknowledgement(new TransactedReceiveOperation(receivedMessage, subscriber));
    }

    // This is a provider internal method.
    private void doHandleAutomaticMessageAcknowledgement(TransactedReceiveOperation receiveOperation) {
        if (isClosed()) return ;
        // If in transaction, ignore.
        if (transacted) {
            enqueueReceiveWithinTransaction(receiveOperation);
            return ;
        }

        if (Session.AUTO_ACKNOWLEDGE == getAcknowledgeMode() ||
            Session.DUPS_OK_ACKNOWLEDGE == getAcknowledgeMode()){
            // Ignore (any) exceptions which might be thrown ...
            try {
                if (logger.isTraceEnabled()) logger.trace("acknowledging ... " + receiveOperation);
                receiveOperation.receivedMessage.originalMessage.getAckRunnable().run();
            } catch (Exception ex){
                if (logger.isDebugEnabled()) {
                    logger.debug("Ignoring exception while sending ack ... ", ex);
                }
            }
        }
    }


    public void unsubscribeFromTopic(String topicName, String subscribedId) throws JMSException {
        sessionFacade.unsubscribeFromTopic(topicName, subscribedId);
    }

    public void stopTopicDelivery(String topicName, String subscribedId) throws JMSException {
        sessionFacade.stopTopicDelivery(topicName, subscribedId);
    }

    public void stopQueueDelivery(String queueName, String subscribedId) throws JMSException {
        sessionFacade.stopQueueDelivery(queueName, subscribedId);
    }

    public void messageReceived(final MessageImpl msg, MessagingSessionFacade.DestinationType type)
        throws JMSException {

        String traceMsg = null;
        ReceivedMessage receivedMessage = new ReceivedMessage(msg, msg, type);
        synchronized (lockObject){
            // ignore if closed ... continue on if in closing state.
            if (isClosed()) return ;
            messageList.add(receivedMessage);

            if (!getTransacted() && CLIENT_ACKNOWLEDGE == getAcknowledgeMode()) {
                sessionFacade.registerUnAcknowledgedMessage(receivedMessage);
            }

            lockObject.notifyAll();
            if (logger.isTraceEnabled()) traceMsg = "messageReceived from " + msg.getSourceName() +
                ", for " + msg.getSubscriberId() + " = " + msg;
        }

        if (logger.isTraceEnabled()) logger.trace(traceMsg);
    }

    // A simple immutable datastructure to hold details about a message which has been recieved.
    public static class ReceivedMessage {
        // Ensure that the original message is NOT modified in any way !
        public final MessageImpl originalMessage;
        // This is the message returned to the client : to the listener and/or in the TopicSubscriberImpl -
        // created as a
        // clone of the originalMessage.
        public final MessageImpl msg;

        public final MessagingSessionFacade.DestinationType destinationType;

        private ReceivedMessage(MessageImpl originalMessage, MessageImpl msg,
                                MessagingSessionFacade.DestinationType destinationType) {
            this.originalMessage = originalMessage;
            this.msg = msg;
            this.destinationType = destinationType;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("ReceivedMessage");
            sb.append("{originalMessage=").append(originalMessage);
            sb.append(", msg=").append(msg);
            sb.append(", destinationType=").append(destinationType);
            sb.append('}');
            return sb.toString();
        }
    }

    // required to catch resource leaks ...
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!sessionState.isInCloseMode()) {
            if (logger.isErrorEnabled()) logger.error("Session was NOT closed before it went out of scope");
            close();
        }
    }

    public boolean isClosed() {
        return sessionState.isClosed();
    }


    // TODO: Introduce a daemon thread which periodically updates this - each call to
    // System.currentTimeMillis is a native call
    // And can be very expensive if there are a lot of concurrent invocations to it : learnings
    // from XMPP server circa 2006 !
    public static long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    public Subscription createSubscription(MessagingSessionFacade.DestinationType type, String name,
                                           String subscriberId){
        switch(type){
          case QUEUE:
              return new QueueSubscription(name, subscriberId);
          case TOPIC:
              return new TopicSubscription(name, subscriberId);
          default:
              throw new IllegalArgumentException("Unknown destination type " + type +
                  " for destination " + name + ", subscriberId " + subscriberId);
        }
    }
    public interface Subscription {
        public boolean isTopic();
        public boolean isQueue();
    }

    public static final class TopicSubscription implements Subscription {
        public final String topicName;
        public final String subscriberId;

        public TopicSubscription(String topicName, String subscriberId) {
            if (null == topicName || null == subscriberId) {
                throw new NullPointerException("Unexpected null as parameter topicName: " +
                    topicName + ", subscriberId: " + subscriberId);
            }
            this.topicName = topicName;
            this.subscriberId = subscriberId;
        }

        public boolean isTopic() { return true; }

        public boolean isQueue() { return false; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TopicSubscription that = (TopicSubscription) o;

            if (!subscriberId.equals(that.subscriberId)) return false;
            if (!topicName.equals(that.topicName)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = topicName.hashCode();
            result = 31 * result + subscriberId.hashCode();
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("TopicSubscription");
            sb.append("{topicName='").append(topicName).append('\'');
            sb.append(", subscriberId='").append(subscriberId).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    public static final class QueueSubscription implements Subscription {
        public final String queueName;
        public final String subscriberId;

        public QueueSubscription(String queueName, String subscriberId) {
            if (null == queueName || null == subscriberId) {
                throw new NullPointerException("Unexpected null as parameter queueName: " +
                    queueName + ", subscriberId: " + subscriberId);
            }
            this.queueName = queueName;
            this.subscriberId = subscriberId;
        }

        public boolean isTopic() { return false; }

        public boolean isQueue() { return true; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            QueueSubscription that = (QueueSubscription) o;

            if (!subscriberId.equals(that.subscriberId)) return false;
            if (!queueName.equals(that.queueName)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = queueName.hashCode();
            result = 31 * result + subscriberId.hashCode();
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("QueueSubscription");
            sb.append("{queueName='").append(queueName).append('\'');
            sb.append(", subscriberId='").append(subscriberId).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }


    private static interface TransactedOperation {
        public void rollback();
        public void commit() throws JMSException;
        public boolean requiresStartedSession();
    }

    private class TransactedSendOperation implements TransactedOperation {
        private final String destination;
        private final MessageImpl messageImpl;
        private final Message userMessage;

        private TransactedSendOperation(String destination, MessageImpl messageImpl, Message userMessage) {
            this.destination = destination;
            this.messageImpl = messageImpl;
            this.userMessage = userMessage;
        }


        @Override
        public void commit() throws JMSException {
            String msgId = SessionImpl.this.sessionFacade.publish(destination, messageImpl);
            if (userMessage instanceof MessageImpl) ((MessageImpl) userMessage).setJMSMessageIDInternal(msgId);
            else userMessage.setJMSMessageID(msgId);
        }

        public void rollback() {
            // noop ...
        }

        @Override
        public boolean requiresStartedSession() {
            return false;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("TransactedSendOperation");
            sb.append("{destination='").append(destination).append('\'');
            sb.append(", messageImpl=").append(messageImpl);
            sb.append(", userMessage=").append(userMessage);
            sb.append('}');
            return sb.toString();
        }
    }

    private class TransactedReceiveOperation implements TransactedOperation {
        private final ReceivedMessage receivedMessage;
        private final MessageListener sessionMessageListener;
        private final MessageConsumer subscriber;

        private TransactedReceiveOperation(ReceivedMessage receivedMessage, MessageListener sessionMessageListener) {
            this.receivedMessage = receivedMessage;
            this.sessionMessageListener = sessionMessageListener;
            this.subscriber = null;
        }

        private TransactedReceiveOperation(ReceivedMessage receivedMessage, MessageConsumer subscriber) {
            this.receivedMessage = receivedMessage;
            this.subscriber = subscriber;
            this.sessionMessageListener = null;
        }

        @Override
        public boolean requiresStartedSession() {
            return true;
        }

        @Override
        public void rollback() {

            // async dispatch ...
            if (null != sessionMessageListener) {
                synchronized (SessionImpl.this.lockObject){
                    rolledbackMessageList.add(this);
                    SessionImpl.this.lockObject.notifyAll();
                }
            }
            // If rollback in sync mode, do in same thread - else a rollback, receive WILL see messages
            // in different order !
            // This is also required since in async mode, session IS NOT MT-safe - and so is
            // expecting this behavior.
            else if (null != subscriber){
                try {
                    MessageImpl theMessage = MessageUtil.createCloneForDispatch(SessionImpl.this,
                            receivedMessage.originalMessage,
                            receivedMessage.originalMessage.getSourceName(),
                            receivedMessage.originalMessage.getSubscriberId());
                    theMessage.setJMSRedelivered(true);

                    sessionFacade.enqueueReceivedMessage(subscriber,
                            new ReceivedMessage(receivedMessage.originalMessage, theMessage,
                                receivedMessage.destinationType), true);
                } catch (JMSException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Unable to enqueue received message to");
                        DebugUtil.dumpJMSStacktrace(logger, e);
                    }
                }
            }
        }

        @Override
        public void commit() throws JMSException {
            try {
                receivedMessage.originalMessage.getAckRunnable().run();
            } catch (Exception ex){
                if (logger.isDebugEnabled()) {
                    logger.debug("Ignoring exception while sending ack ... ", ex);
                }
            }
        }

        /**
         * Recovery is slightly tricky - we have two cases here :
         * a) Messages which to be consumed via subscriber which support the sync mode - via receive(), variants.
         * b) Messages which are to be consumed via the subscriber or session's async mode - via listener.
         *
         *
         * We have to ensure that message recovery will result in the exact SAME order of message
         * delivery to client as it was done first time
         * (when rollback was triggered).
         * To ensure this, the recover method is called in REVERSE order in which operations were
         * enqueued in the txn (log) queue.
         *
         * To handle (a), sessionFacade.enqueueReceivedMessage pushes message to begining of pending
         * message queue in subscriber.
         * Taken along with reverse order of unwinding of txn log, this ensure the desired behavior for (a).
         *
         * To ensure desired behavior for (b), we pass a listenerDeliveryList as parameter - which is
         * used to maintain the
         * order of how to invoke onMessage to recover for async dispatch. Note: we keep adding to
         * begining of this list to ensure that
         * in the end, oldest message in txn log is the first message in listenerDeliveryList when
         * we attempt recovery.
         *
         */
        public void recover(LinkedList<TransactedReceiveOperation> listenerDeliveryList) {
            // Do the actual recovery ...
            if (null != subscriber){
                // already handled in rollback ...
                assert false : "unexpected ...";
            }
            else if (null != sessionMessageListener){
                listenerDeliveryList.addFirst(this);
            }
        }

        public void recoverForListener(){
            assert null == subscriber;
            assert null != sessionMessageListener;

            try {
                final MessageImpl message = MessageUtil.createCloneForDispatch(SessionImpl.this,
                        receivedMessage.originalMessage,
                        receivedMessage.originalMessage.getSourceName(),
                        receivedMessage.originalMessage.getSubscriberId());
                deliverToListener(sessionMessageListener, receivedMessage, message, true);
            } catch (JMSException e) {
                // Unexpected not to be able to clone ...
                if (logger.isDebugEnabled()) {
                    logger.debug("Unexpected exception trying to process message");
                    DebugUtil.dumpJMSStacktrace(logger, e);
                }
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("TransactedReceiveOperation");
            sb.append("{receivedMessage=").append(receivedMessage);
            sb.append(", sessionMessageListener=").append(sessionMessageListener);
            sb.append(", subscriber=").append(subscriber);
            sb.append('}');
            return sb.toString();
        }
    }


    // For txn support.
    private final Object transactionLock = new Object();
    private final List<TransactedOperation> transactedOperation = new LinkedList<TransactedOperation>();

    private void rollbackTransactionState(){
        final ArrayList<TransactedOperation> transactedOperationCopy;
        synchronized (transactionLock){
            transactedOperationCopy = new ArrayList<TransactedOperation>(transactedOperation);
            transactedOperation.clear();
        }
        rollbackTransactionState(transactedOperationCopy);
    }

    private void rollbackTransactionState(ArrayList<TransactedOperation> transactedOperationCopy){

        if (logger.isDebugEnabled()) logger.debug("Attempting to rollback " +
            transactedOperationCopy.size() + " operations");
        if (logger.isTraceEnabled()) logger.trace("Operations : " + transactedOperationCopy);

        // Rollback MUST be in reverse order !
        final int size = transactedOperationCopy.size();
        for (int i = size - 1;i >= 0; i --){
            TransactedOperation op = transactedOperationCopy.get(i);
            op.rollback();
        }
    }


    // Note: the messageImpl MUST be a copy of what the user sent - so that modifications by user
    // WILL NOT affect this.
    public void enqueuePublishWithinTransaction(String topicName, MessageImpl messageImpl, Message userMessage) {
        TransactedSendOperation sendOperation = new TransactedSendOperation(topicName, messageImpl, userMessage);
        synchronized (transactionLock){
            transactedOperation.add(sendOperation);
        }
    }

    private void enqueueReceiveWithinTransaction(TransactedReceiveOperation receiveOperation) {
        synchronized (transactionLock){
            transactedOperation.add(receiveOperation);
        }
    }

    private void commitTransactionState() throws JMSException {
        final ArrayList<TransactedOperation> transactedOperationCopy;
        synchronized (transactionLock){
            transactedOperationCopy = new ArrayList<TransactedOperation>(transactedOperation);
            transactedOperation.clear();
        }

        if (!sessionState.isStarted()){
            // Ensure that there are ONLY send op's - else throw TransactionRolledBackException : we
            // cannot ack message !
            for (TransactedOperation op : transactedOperationCopy){
                if (op.requiresStartedSession())
                  throw new TransactionRolledBackException("Commit failed : session is not open - cant ack message");
            }
        }

        for (TransactedOperation op : transactedOperationCopy){
            try {
                op.commit();
            } catch (JMSException jEx){
                if (logger.isDebugEnabled()) logger.debug("Commit failed for " + op, jEx);
                rollbackTransactionState(transactedOperationCopy);
                TransactionRolledBackException trbEx = new TransactionRolledBackException("Commit failed");
                trbEx.setLinkedException(jEx);
                throw trbEx;
            }
        }
    }

    public boolean isMessageExpired(MessageImpl message) {
        return 0 != message.getJMSExpiration() && SessionImpl.currentTimeMillis() > message.getJMSExpiration();
    }

    public boolean isLocallyPublished(String messageId) {
        return connection.isLocallyPublished(messageId);
    }

    public void addToLocallyPublishedMessageIds(String messageId) {
        connection.addToLocallyPublishedMessageIds(messageId);
    }

    public ConnectionImpl getConnection() {
        return connection;
    }

    private static final class Subscriptions {
        // Keeps track of number of subscribers created. This will prevent setMessageListener form
        // succeeding in case subscriberCreatedCount > 0
        // Their use is mutually exclusive.
        // private int subscriberCreatedCount = 0;
        private AtomicInteger numSubscribers = new AtomicInteger(0);

        private final ConcurrentHashMap<TopicSubscription, Node> topicSubscriptionToSelectorMap =
            new ConcurrentHashMap<TopicSubscription, Node>(32);
        private final ConcurrentHashMap<QueueSubscription, Node> queueSubscriptionToSelectorMap =
            new ConcurrentHashMap<QueueSubscription, Node>(32);

        // We make use of concurrent api for this map (and its list) since we will be reading
        // it heavily concurrently while modifying it rarely.
        private final Map<Subscription, CopyOnWriteArrayList<MessageConsumer>> topicSubscriptionToSubscriberMap =
            new ConcurrentHashMap<Subscription, CopyOnWriteArrayList<MessageConsumer>>();
        private final Map<Subscription, CopyOnWriteArrayList<MessageConsumer>> queueSubscriptionToSubscriberMap =
            new ConcurrentHashMap<Subscription, CopyOnWriteArrayList<MessageConsumer>>();

        // The value for the key is irrelevant - there is not
        private final Set<MessageConsumer> allConsumersSet = Collections.newSetFromMap(
            new IdentityHashMap<MessageConsumer, Boolean>());

        private static final int SUBSCRIBER_ID_TO_DESTINATION_CACHE_SIZE =
            Integer.getInteger("SUBSCRIBER_ID_TO_DESTINATION_CACHE_SIZE", 1024);
        // This is guarded by subscriberIdTo<Destination> lock - query/modify ONLY in that context !
        private final Map<String, String> topicSubscriberIdToTopicName =
            new LRUCacheMap<String, String>(SUBSCRIBER_ID_TO_DESTINATION_CACHE_SIZE, true);

        public Map<Subscription, CopyOnWriteArrayList<MessageConsumer>> createSubscriptionToSubscriberMapCopy() {
          Map<Subscription, CopyOnWriteArrayList<MessageConsumer>> retval
              = new HashMap<Subscription, CopyOnWriteArrayList<MessageConsumer>>();
          retval.putAll(topicSubscriptionToSubscriberMap);
          retval.putAll(queueSubscriptionToSubscriberMap);
          return retval;
        }

        public Set<MessageConsumer> getAllConsumersSet() {
          return allConsumersSet;
        }

        public CopyOnWriteArrayList<? extends MessageConsumer> getSubscribers(Subscription subscription) {
            if (subscription.isTopic()) {
                assert subscription instanceof TopicSubscription;
                return topicSubscriptionToSubscriberMap.get(subscription);
            }
            if (subscription.isQueue()) {
                assert subscription instanceof QueueSubscription;
                return queueSubscriptionToSubscriberMap.get(subscription);
            }
            throw new IllegalArgumentException("Unknown subscription type " + subscription);
        }

        public Node getSelectorExpression(Subscription subscription) {
            if (subscription.isTopic()) {
                assert subscription instanceof TopicSubscription;
                return topicSubscriptionToSelectorMap.get(subscription);
            }
            if (subscription.isQueue()) {
                assert subscription instanceof QueueSubscription;
                return queueSubscriptionToSelectorMap.get(subscription);
            }
            throw new IllegalArgumentException("Unknown subscription type " + subscription);
        }

        public void registerSubscriberIdToTopic(String subscribedId, String topicName) throws JMSException {
            synchronized (topicSubscriberIdToTopicName){
                String currentTopicName = topicSubscriberIdToTopicName.get(subscribedId);
                if (null != currentTopicName && !currentTopicName.equals(topicName)) {
                    throw new JMSException("There is already a subscription in this session for " +
                        "same subscriberId for topic " + currentTopicName);
                }
                topicSubscriberIdToTopicName.put(subscribedId, topicName);
            }
        }

        public String findTopicNameForSubscriberId(String subscribedId) throws JMSException {
            synchronized (topicSubscriberIdToTopicName){
                String topicName = topicSubscriberIdToTopicName.get(subscribedId);
                if (null == topicName){
                    throw new JMSException("Unable to find topicName for subscriberId " + subscribedId);
                }
                return topicName;
            }
        }

        public void registerTopicSubscriptionSelector(TopicSubscription topicSubscription, Node selectorAst) {
            topicSubscriptionToSelectorMap.put(topicSubscription, selectorAst);
        }

        public void registerQueueSubscriptionSelector(QueueSubscription queueSubscription, Node selectorAst) {
            queueSubscriptionToSelectorMap.put(queueSubscription, selectorAst);
        }

        public boolean addToSubscriberSet(MessageConsumer consumer) {
            return allConsumersSet.add(consumer);
        }

        public boolean addToSubscribers(MessageConsumer subscriber, MessagingSessionFacade.DestinationType type,
                                        String destination, String subscriberId) {
            switch (type){
              case QUEUE :
                return createIfMissingAndAdd(queueSubscriptionToSubscriberMap,
                    new QueueSubscription(destination, subscriberId),
                    subscriber);
              case TOPIC:
                return createIfMissingAndAdd(topicSubscriptionToSubscriberMap,
                    new TopicSubscription(destination, subscriberId),
                    subscriber);
              default:
                  throw new IllegalArgumentException("Unknown subscription type " + type);
            }
        }

        public boolean removeSubscriber(MessageConsumer subscriber, MessagingSessionFacade.DestinationType type,
                                        String destination, String subscriberId) {
            boolean retval = false;
            switch (type){
                case TOPIC:
                {
                    final CopyOnWriteArrayList<MessageConsumer> subscriberList =
                        topicSubscriptionToSubscriberMap.get(new TopicSubscription(destination, subscriberId));
                    if (null != subscriberList) {
                        if (subscriberList.remove(subscriber)){
                            numSubscribers.decrementAndGet();
                            if (subscriberList.isEmpty()){
                                // Unsubscribe
                                retval = true;
                            }
                        }
                    }
                    else retval = true;
                    break;
                }
                case QUEUE:
                {
                    final CopyOnWriteArrayList<MessageConsumer> subscriberList =
                        queueSubscriptionToSubscriberMap.get(new QueueSubscription(destination, subscriberId));
                    if (null != subscriberList) {
                        if (subscriberList.remove(subscriber)){
                            numSubscribers.decrementAndGet();
                            if (subscriberList.isEmpty()){
                                // Unsubscribe
                                retval = true;
                            }
                        }
                    }
                    else retval = true;
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unknown subscription type " + type);
            }
            return retval;
        }

        // returns true IF a new list is inserted.
        private boolean createIfMissingAndAdd(Map<Subscription, CopyOnWriteArrayList<MessageConsumer>> map,
                                              Subscription key, MessageConsumer value) {
            boolean retval = false;
            if (!map.containsKey(key)) {
                map.put(key, new CopyOnWriteArrayList<MessageConsumer>());
            }

            List<MessageConsumer>  list = map.get(key);
            if (list.isEmpty()) retval = true;
            if (!list.contains(value)) {
                list.add(value);
                numSubscribers.incrementAndGet();
            }
            return retval;
        }

        public int getNumSubscribers() {
            return numSubscribers.get();
        }
    }
}
