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
import org.apache.hedwig.jms.DebugUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Subscriber to a topic.
 *
 */
public class TopicSubscriberImpl extends MessageConsumerImpl implements TopicSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(TopicSubscriberImpl.class);

    private final SessionImpl session;
    private final Topic topic;
    private final String subscriberId;
    private final boolean noLocal;

    private final boolean forceUnsubscribe;
    private volatile boolean registered = false;
    private boolean closed = false;

    // Any publically exposed object MUST NOT rely on 'this' for its locking semantics unless it is
    // explicitly exposing this behavior.
    private final Object lockObject = new Object();
    private final LinkedList<SessionImpl.ReceivedMessage> pendingMessageList
        = new LinkedList<SessionImpl.ReceivedMessage>();

    public TopicSubscriberImpl(SessionImpl session, Topic topic, String subscriberId,
                               boolean forceUnsubscribe) throws JMSException {
        super(null);
        this.session = session;
        this.topic = topic;
        this.subscriberId = subscriberId;
        // default is false right ?
        this.noLocal = false;
        this.forceUnsubscribe = forceUnsubscribe;

        // I am not sure if we have to register with session immediately on create or not ...
        registerWithSession();
    }

    public TopicSubscriberImpl(SessionImpl session, Topic topic, String subscriberId,
                               String messageSelector, boolean noLocal, boolean forceUnsubscribe) throws JMSException {
        super(messageSelector);
        this.session = session;
        this.topic = topic;
        this.subscriberId = subscriberId;

        this.noLocal = noLocal;
        this.forceUnsubscribe = forceUnsubscribe;

        if (null == getSelectorAst()){
            // Only if NOT empty string - treat empty string as null selector spec.
            if (null != messageSelector && 0 != messageSelector.trim().length()){
                throw new InvalidSelectorException("Invalid selector specified '" + messageSelector + "'");
            }
        }
        else {
            session.registerTopicSubscriptionInfo(new SessionImpl.TopicSubscription(topic.getTopicName(),
                subscriberId), getSelectorAst());
        }

        // I am not sure if we have to register with session immediately on create or not ...
        registerWithSession();
    }

    @Override
    public Topic getTopic() {
        return topic;
    }

    @Override
    public boolean getNoLocal() {
        return noLocal;
    }

    public String getSubscriberId() {
        return subscriberId;
    }

    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSException {
        super.setMessageListener(messageListener);
        registerWithSession();
    }

    private void registerWithSession() throws JMSException {

        // Fail fast ... volatile perf hit is ok in comparison to rest.
        if (this.registered) return ;

        final boolean register;
        synchronized (lockObject){
            // if (closed) throw new JMSException("Already closed");
            if (closed) return ;

            if (!this.registered) {
                this.registered = true;
                register = true;
            }
            else register = false;
        }
        if (register) this.session.registerTopicSubscriber(this);
    }

    @Override
    public Message receive() throws JMSException {
        return receive(0);
    }


    @Override
    public Message receive(final long maxTimeout) throws JMSException {
        return receiveImpl(maxTimeout, true);
    }

    private Message receiveImpl(final long maxTimeout, boolean canWait) throws JMSException {
        final long waitTimeout;
        final long startTime;

        // periodically wake up !
        if (canWait){
            if (maxTimeout <= 0) waitTimeout = 1000;
            else {
                long duration = maxTimeout / 16;
                if (duration <= 0) duration = 1;
                waitTimeout = duration;
            }
            startTime = SessionImpl.currentTimeMillis();
        }
        else {
            waitTimeout = 0;
            startTime = 0;
        }

        registerWithSession();

        // check before lock ...
        if (null != getMessageListener()) {
          throw new javax.jms.IllegalStateException(
                  "There is a message listener already subscribed for this subscriber");
        }

        final SessionImpl.ReceivedMessage message;
        final List<SessionImpl.ReceivedMessage> ackList = new ArrayList<SessionImpl.ReceivedMessage>(4);

        synchronized (lockObject){

outer:
            while (true) {

                // Should we ignore cached messages instead of this ?
                // Once closed, wont help much anyway, right ?
                if (closed) {
                    message = null;
                    break outer;
                }

                // While we waited, it could have been set.
                if (null != getMessageListener()) {
                  throw new javax.jms.IllegalStateException(
                          "There is a message listener already subscribed for this subscriber");
                }

                while (canWait && pendingMessageList.isEmpty()){

                    // Should we ignore cached messages instead of this ?
                    // Once closed, wont help much anyway, right ?
                    if (closed) {
                        message = null;
                        break outer;
                    }

                    if (0 != maxTimeout && startTime + maxTimeout < SessionImpl.currentTimeMillis()) {
                        message = null;
                        break outer;
                    }

                    try {
                        lockObject.wait(waitTimeout);
                    } catch (InterruptedException iEx){
                        JMSException jEx = new JMSException("Interrupted .. " + iEx);
                        jEx.setLinkedException(iEx);
                        throw jEx;
                    }
                }


                if (pendingMessageList.isEmpty()) {
                    message = null;
                    break outer;
                }
                SessionImpl.ReceivedMessage tmessage = pendingMessageList.remove();
                ackList.add(tmessage);

                if (noLocal){
                    if (session.isLocallyPublished(tmessage.originalMessage.getJMSMessageID())){
                        // find next message.
                        continue;
                    }
                }
                if (session.isMessageExpired(tmessage.originalMessage)) continue;
                // use this message then.
                message = tmessage;
                break;
            }
        }

        if (logger.isTraceEnabled()) logger.trace("Acklist receive (" + ackList.size() + ") ... " + ackList);
        for (SessionImpl.ReceivedMessage ackMessage : ackList){
            session.handleAutomaticMessageAcknowledgement(ackMessage, this);
        }

        if (logger.isTraceEnabled()) logger.trace("receive response " + (null != message ? message.msg : null));
        return null != message ? message.msg : null;
   }

    @Override
    public Message receiveNoWait() throws JMSException {
        return receiveImpl(0, false);
    }

    @Override
    public void close() throws JMSException {

        final boolean unregister;
        final boolean unsubscribe;

        synchronized (lockObject){
            if (closed) return ;
            closed = true;

            // This means that we drop all pending messages ...
            // gc friendly.
            pendingMessageList.clear();

            unregister = registered;
            this.registered = false;

            unsubscribe = this.forceUnsubscribe;
        }

        if (unregister) this.session.unregisterTopicSubscriber(this);

        // this.session.stopTopicDelivery(topic.getTopicName(), subscriberId);
        if (unsubscribe) session.unsubscribeFromTopic(topic.getTopicName(), subscriberId);

        // nothing else to be done ...
    }

    boolean enqueueReceivedMessage(SessionImpl.ReceivedMessage receivedMessage, final boolean addFirst) {
        if (logger.isTraceEnabled())
          logger.trace("Enqueing message " + receivedMessage + " to subscriber " + subscriberId +
              " for topic " + topic.toString() + ", addFirst : " + addFirst);

        String infoMsg = null;
        String traceMsg = null;
        synchronized (lockObject){
            // ignore
            if (closed) return false;
            // If number of buffered messages > some max limit, evict them - else we run out of memory !
            if (pendingMessageList.size() > SessionImpl.MAX_SUBSCRIBER_BUFFERED_MESSAGES) {
                // simply discard it with an error logged.
                infoMsg = "Discarding " + pendingMessageList.size() + " messages since there are no consumers for them";
                pendingMessageList.clear();
            }

            // Note: Selector evaluation will happen in SessionImpl.
            // if (!selectorMatched(receivedMessage)) return false;

            if (addFirst) pendingMessageList.addFirst(receivedMessage);
            else pendingMessageList.add(receivedMessage);

            lockObject.notifyAll();
            if (logger.isTraceEnabled()) traceMsg = "pendingMessageList (" + pendingMessageList.size() +
                ") : \n" + pendingMessageList + "\n---\n next : " + pendingMessageList.getFirst();
        }

        if (null != infoMsg) logger.info(infoMsg);
        if (logger.isTraceEnabled() && null != traceMsg) logger.trace(traceMsg);

        return true;
    }

    public void start() {
        try {
            registerWithSession();
        } catch (JMSException jEx){
            // ignore.
            DebugUtil.dumpJMSStacktrace(logger, jEx);
        }
    }
}
