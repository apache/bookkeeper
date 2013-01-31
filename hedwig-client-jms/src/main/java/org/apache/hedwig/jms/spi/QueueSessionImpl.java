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

import org.apache.hedwig.jms.ConnectionImpl;
import org.apache.hedwig.jms.MessagingSessionFacade;
import org.apache.hedwig.jms.SessionImpl;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

/**
 * Queue specific impl
 */
public class QueueSessionImpl extends SessionImpl implements QueueSession {

    public QueueSessionImpl(ConnectionImpl connection, boolean transacted, int acknowledgeMode) throws JMSException {
        super(connection, transacted, acknowledgeMode);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return super.createReceiverImpl(queue);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        return super.createReceiverImpl(queue, messageSelector);
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        return super.createSenderImpl(queue);
    }

    // JMS requires these methods cant be called on QueueSession.
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String subscribedId) throws JMSException {
        throw new javax.jms.IllegalStateException("Cant call this method on QueueSession");
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String subscribedId, String messageSelector,
                                                   boolean noLocal) throws JMSException {
        throw new javax.jms.IllegalStateException("Cant call this method on QueueSession");
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new javax.jms.IllegalStateException("Cant call this method on QueueSession");
    }

    @Override
    public void unsubscribe(String subscribedId) throws JMSException {
        throw new javax.jms.IllegalStateException("Cant call this method on QueueSession");
    }

  @Override
    public Topic createTopic(String topicName) throws JMSException {
        throw new javax.jms.IllegalStateException("Cant call this method on QueueSession");
    }
}
