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
import org.apache.hedwig.jms.SessionImpl;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.TemporaryQueue;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

/**
 * Topic specific impl
 */
public class TopicSessionImpl extends SessionImpl implements TopicSession {

    public TopicSessionImpl(ConnectionImpl connection, boolean transacted, int acknowledgeMode) throws JMSException {
        super(connection, transacted, acknowledgeMode);
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return super.createSubscriberImpl(topic);
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        return super.createSubscriberImpl(topic, messageSelector, noLocal);
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return super.createPublisherImpl(topic);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new javax.jms.IllegalStateException("Cant call this method on TopicSession");
    }

    @Override
    public Queue createQueue(String queueName) throws JMSException {
        throw new javax.jms.IllegalStateException("Cant call this method on TopicSession");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        throw new javax.jms.IllegalStateException("Cant call this method on TopicSession");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        throw new javax.jms.IllegalStateException("Cant call this method on TopicSession");
    }
}
