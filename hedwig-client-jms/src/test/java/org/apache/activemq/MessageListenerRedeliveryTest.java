/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.apache.activemq;

import javax.jms.Topic;

import org.apache.hedwig.JmsTestBase;
import org.apache.hedwig.jms.SessionImpl;
import java.util.ArrayList;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import javax.jms.Destination;
import org.apache.hedwig.jms.message.MessageImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageListenerRedeliveryTest extends JmsTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MessageListenerRedeliveryTest.class);

    private Connection connection;

    protected void setUp() throws Exception {
        super.setUp();
        connection = createConnection();
    }

    /**
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    protected Connection createConnection() throws Exception {
        HedwigConnectionFactoryImpl factory = new HedwigConnectionFactoryImpl();
        return factory.createConnection();
    }

    private class TestMessageListener implements MessageListener {

        public int counter = 0;
        private Session session;
        static final int ROLLBACK_COUNT = 5;

        public TestMessageListener(Session session) {
            this.session = session;
        }

        public void onMessage(Message message) {
            try {
                LOG.info("Message Received: " + message);
                counter++;
                if (counter < ROLLBACK_COUNT) {
                    LOG.info("Message Rollback.");
                    session.rollback();
                } else {
                    LOG.info("Message Commit.");
                    message.acknowledge();
                    session.commit();
                }
            } catch (JMSException e) {
                LOG.error("Error when rolling back transaction");
            }
        }
    }

    public void testTopicRollbackConsumerListener() throws JMSException {
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Topic queue = session.createTopic("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        Message message = createTextMessage(session);
        MessageConsumer consumer = session.createConsumer(queue);
        TestMessageListener listener = new TestMessageListener(session);
        consumer.setMessageListener(listener);
        producer.send(message);
        session.commit();


        MessageConsumer mc = (MessageConsumer)consumer;


        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {

        }

        assertEquals(TestMessageListener.ROLLBACK_COUNT, listener.counter);

        session.close();
    }

    public void testTopicSessionListenerExceptionRetry() throws  Exception {
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic queue = session.createTopic("queue-" + getName());
        Message message = createTextMessage(session, "1");
        MessageConsumer consumer = session.createConsumer(queue);

        final int maxDeliveries = 2;
        final CountDownLatch gotMessage = new CountDownLatch(2);
        final AtomicInteger count  = new AtomicInteger(0);
        final ArrayList<String> received = new ArrayList<String>();
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                LOG.info("Message Received: " + message);
                try {
                    received.add(((TextMessage) message).getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                    fail(e.toString());
                }
                if (count.incrementAndGet() < maxDeliveries) {
                    throw new RuntimeException(getName() + " force a redelivery");
                }
                // new blood
                count.set(0);
                gotMessage.countDown();
            }
        });

        MessageProducer producer = createProducer(session, queue);
        producer.send(message);
        message = createTextMessage(session, "2");
        producer.send(message);

        assertTrue("got message before retry expiry", gotMessage.await(20, TimeUnit.SECONDS));

        for (int i=0; i<maxDeliveries; i++) {
            assertEquals("got first redelivered: " + i, "1", received.get(i));
        }
        for (int i=maxDeliveries; i<maxDeliveries*2; i++) {
            assertEquals("got first redelivered: " + i, "2", received.get(i));
        }
        session.close();
    }

    private TextMessage createTextMessage(Session session, String text) throws JMSException {
        return session.createTextMessage(text);
    }
    private TextMessage createTextMessage(Session session) throws JMSException {
        return session.createTextMessage("Hello");
    }

    private MessageProducer createProducer(Session session, Destination queue) throws JMSException {
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(getDeliveryMode());
        return producer;
    }

    protected int getDeliveryMode() {
        return DeliveryMode.PERSISTENT;
    }
}
