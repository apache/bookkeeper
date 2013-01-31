/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;


import javax.jms.TextMessage;
import javax.jms.Topic;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.hedwig.JmsTestBase;
import org.apache.hedwig.jms.spi.HedwigConnectionImpl;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;

import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonBlockingConsumerRedeliveryTest extends JmsTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(NonBlockingConsumerRedeliveryTest.class);

    private final String destinationName = "Destination";
    // private final int MSG_COUNT = 100;

    private HedwigConnectionFactoryImpl connectionFactory;

    public void testMessageDeleiveredWhenNonBlockingEnabled() throws Exception {

        final List<String> received = new ArrayList<String>(16);
        final List<String> beforeRollback = new ArrayList<String>(16);
        final List<String> afterRollback = new ArrayList<String>(16);

        HedwigConnectionImpl connection = connectionFactory.createConnection();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(destinationName);
        MessageConsumer consumer = session.createConsumer(destination);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    received.add(((TextMessage) message).getText());
                } catch (JMSException e) {
                    // should not happen ...
                    e.printStackTrace();
                }
            }
        });

        final int MSG_COUNT = connection.getHedwigClientConfig().getMaximumOutstandingMessages() - 1;

        sendMessages(MSG_COUNT);

        session.commit();
        connection.start();


        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                LOG.info("Consumer has received " + received.size() + " messages.");
                return received.size() == MSG_COUNT;
            }
        });

        assertTrue("Pre-Rollback expects to receive: " + MSG_COUNT + " messages, got "
                   + received.size() + ".",MSG_COUNT == received.size());

        beforeRollback.addAll(received);
        received.clear();
        session.rollback();

        assertTrue("Post-Rollback expects to receive: " + MSG_COUNT + " messages.",
            Wait.waitFor(new Wait.Condition(){
                public boolean isSatisified() throws Exception {
                    LOG.info("Consumer has received " + received.size() + " messages since rollback.");
                    return received.size() == MSG_COUNT;
                }
            }
        ));

        afterRollback.addAll(received);
        received.clear();

        assertEquals(beforeRollback.size(), afterRollback.size());
        assertEquals(beforeRollback, afterRollback);
        session.commit();
    }

    public void testMessageDeleiveryDoesntStop() throws Exception {

        final List<String> received = Collections.synchronizedList(new ArrayList<String>(16));
        final List<String> beforeRollback = new ArrayList<String>(16);
        final List<String> afterRollback = new ArrayList<String>(16);

        HedwigConnectionImpl connection = connectionFactory.createConnection();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(destinationName);
        MessageConsumer consumer = session.createConsumer(destination);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    received.add(((TextMessage) message).getText());
                } catch (JMSException e) {
                    // should not happen
                    e.printStackTrace();
                }
            }
        });

        final int MSG_COUNT = connection.getHedwigClientConfig().getMaximumOutstandingMessages() / 2 - 1;

        sendMessages(MSG_COUNT);
        connection.start();

        assertTrue("Pre-Rollback expects to receive: " + MSG_COUNT + " messages.",
            Wait.waitFor(new Wait.Condition(){
                public boolean isSatisified() throws Exception {
                    LOG.info("Consumer has received " + received.size() + " messages.");
                    return received.size() == MSG_COUNT;
                }
            }
        ));

        beforeRollback.addAll(received);
        received.clear();
        session.rollback();

        sendMessages(MSG_COUNT);

        {
            boolean messagesReceived = Wait.waitFor(new Wait.Condition(){
                public boolean isSatisified() throws Exception {
                    LOG.info("Consumer has received " + received.size() + " messages since rollback.");
                    return received.size() == MSG_COUNT * 2;
                }
            });
            assertTrue("Post-Rollback expects to receive: " + MSG_COUNT + " messages, received "
                       + received.size() + " messages.", messagesReceived);
        }

        afterRollback.addAll(received);
        received.clear();

        assertEquals(beforeRollback.size() * 2, afterRollback.size());

        session.commit();
    }

    public void testNonBlockingMessageDeleiveryIsDelayed() throws Exception {
        final List<String> received = new ArrayList<String>(16);

        HedwigConnectionImpl connection = (HedwigConnectionImpl) connectionFactory.createConnection();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(destinationName);
        MessageConsumer consumer = session.createConsumer(destination);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    received.add(((TextMessage) message).getText());
                } catch (JMSException e) {
                    // should not happen
                    e.printStackTrace();
                }
            }
        });

        final int MSG_COUNT = connection.getHedwigClientConfig().getMaximumOutstandingMessages() - 1;

        sendMessages(MSG_COUNT);
        connection.start();

        assertTrue("Pre-Rollback expects to receive: " + MSG_COUNT + " messages.",
            Wait.waitFor(new Wait.Condition(){
                public boolean isSatisified() throws Exception {
                    LOG.info("Consumer has received " + received.size() + " messages.");
                    return received.size() == MSG_COUNT;
                }
            }
        ));

        received.clear();
        session.rollback();

        {
            boolean condition = Wait.waitFor(new Wait.Condition() {
                public boolean isSatisified() throws Exception {
                    return received.size() > 0;
                }
            }, TimeUnit.SECONDS.toMillis(4)
            );
            // We do not have any notion of delaying rederlivery - so we immediately get the message (unlike activemq's
            // connection.getRedeliveryPolicy().setInitialRedeliveryDelay(TimeUnit.SECONDS.toMillis(6));
            // assertFalse("Delayed redelivery test not expecting any messages yet. got "
            // + received.size() + " messages", condition);
            assertTrue("Rollback expects to receive: " + MSG_COUNT + " messages.",
                Wait.waitFor(new Wait.Condition(){
                    public boolean isSatisified() throws Exception {
                        LOG.info("Consumer has received " + received.size() + " messages.");
                        return received.size() == MSG_COUNT;
                    }
                }
            ));
        }

        session.commit();
        session.close();
    }

    public void testNonBlockingMessageDeleiveryWithRollbacks() throws Exception {
        final List<String> received = new ArrayList<String>(16);

        HedwigConnectionImpl connection = (HedwigConnectionImpl) connectionFactory.createConnection();
        final Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        final Destination destination = session.createTopic(destinationName);
        final MessageConsumer consumer = session.createConsumer(destination);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    received.add(((TextMessage) message).getText());
                } catch (JMSException e) {
                    // should not happen
                    e.printStackTrace();
                }
            }
        });

        final int MSG_COUNT = connection.getHedwigClientConfig().getMaximumOutstandingMessages() - 1;

        sendMessages(MSG_COUNT);
        connection.start();

        assertTrue("Pre-Rollback expects to receive: " + MSG_COUNT + " messages.",
            Wait.waitFor(new Wait.Condition(){
                public boolean isSatisified() throws Exception {
                    LOG.info("Consumer has received " + received.size() + " messages.");
                    return received.size() == MSG_COUNT;
                }
            }
        ));

        received.clear();

        consumer.setMessageListener(new MessageListener() {

            int count = 0;

            @Override
            public void onMessage(Message message) {

                if (++count > 10) {
                    try {
                        session.rollback();
                        LOG.info("Rolling back session.");
                        count = 0;
                    } catch (JMSException e) {
                        LOG.warn("Caught an unexcepted exception: " + e.getMessage());
                    }
                } else {
                    try {
                        received.add(((TextMessage) message).getText());
                    } catch (JMSException e) {
                        // should not happen
                        e.printStackTrace();
                    }
                    try {
                        session.commit();
                    } catch (JMSException e) {
                        LOG.warn("Caught an unexcepted exception: " + e.getMessage());
                    }
                }
            }
        });

        session.rollback();

        assertTrue("Post-Rollback expects to receive: " + MSG_COUNT + " messages.",
            Wait.waitFor(new Wait.Condition(){
                public boolean isSatisified() throws Exception {
                    LOG.info("Consumer has received " + received.size() + " messages since rollback.");
                    return received.size() == MSG_COUNT;
                }
            }
        ));

        assertEquals(MSG_COUNT, received.size());
        session.commit();
    }

    private void sendMessages(final int MSG_COUNT) throws Exception {
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(destinationName);
        MessageProducer producer = session.createProducer(destination);
        for(int i = 0; i < MSG_COUNT; ++i) {
            producer.send(session.createTextMessage("" + i));
        }
    }

    public void setUp() throws Exception {
        super.setUp();
        connectionFactory = new HedwigConnectionFactoryImpl();
    }

}
