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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;


import org.apache.hedwig.jms.SessionImpl;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsRollbackRedeliveryTest extends AutoFailTestSupport {
    protected static final Logger LOG = LoggerFactory.getLogger(JmsRollbackRedeliveryTest.class);
    final int nbMessages = 10;
    final String destinationName = "Destination";
    boolean consumerClose = true;
    boolean rollback = true;

    public void setUp() throws Exception {
        setAutoFail(true);
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testRedelivery() throws Exception {
        doTestRedelivery(false);
    }

    public void testRedeliveryWithInterleavedProducer() throws Exception {
        doTestRedelivery(true);
    }

    public void doTestRedelivery(boolean interleaveProducer) throws Exception {

        ConnectionFactory connectionFactory = new HedwigConnectionFactoryImpl();
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(destinationName);
        MessageConsumer consumer = session.createConsumer(destination);
        connection.start();

        if (interleaveProducer) {
            populateDestinationWithInterleavedProducer(nbMessages, destinationName, connection);
        } else {
            populateDestination(nbMessages, destinationName, connection);
        }
        // Consume messages and rollback transactions
        {
            AtomicInteger received = new AtomicInteger();
            Map<String, Boolean> rolledback = new ConcurrentHashMap<String, Boolean>();
            while (received.get() < nbMessages) {
                TextMessage msg = (TextMessage) consumer.receive(6000000);
                if (msg != null) {
                    if (msg != null && rolledback.put(msg.getText(), Boolean.TRUE) != null) {
                        LOG.info("Received message " + msg.getText()
                                 + " (" + received.getAndIncrement() + ")" + msg.getJMSMessageID());
                        assertTrue(msg.getJMSRedelivered());
                        // assertEquals(2, msg.getLongProperty("JMSXDeliveryCount"));
                        session.commit();
                    } else {
                        LOG.info("Rollback message " + msg.getText() + " id: " +  msg.getJMSMessageID());
                        assertFalse("should not have redelivery flag set, id: "
                                    + msg.getJMSMessageID(), msg.getJMSRedelivered());
                        session.rollback();
                    }
                }
            }
            consumer.close();
            session.close();
        }
    }

    public void testRedeliveryOnSingleConsumer() throws Exception {

        ConnectionFactory connectionFactory =
            new HedwigConnectionFactoryImpl();
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(destinationName);
        MessageConsumer consumer = session.createConsumer(destination);

        populateDestinationWithInterleavedProducer(nbMessages, destinationName, connection);

        // Consume messages and rollback transactions
        {
            AtomicInteger received = new AtomicInteger();
            Map<String, Boolean> rolledback = new ConcurrentHashMap<String, Boolean>();
            while (received.get() < nbMessages) {
                TextMessage msg = (TextMessage) consumer.receive(6000000);
                if (msg != null) {
                    if (msg != null && rolledback.put(msg.getText(), Boolean.TRUE) != null) {
                        LOG.info("Received message " + msg.getText() + " ("
                                 + received.getAndIncrement() + ")" + msg.getJMSMessageID());
                        assertTrue(msg.getJMSRedelivered());
                        session.commit();
                    } else {
                        LOG.info("Rollback message " + msg.getText() + " id: " +  msg.getJMSMessageID());
                        session.rollback();
                    }
                }
            }
            consumer.close();
            session.close();
        }
    }

    public void testRedeliveryOnSingleSession() throws Exception {

        ConnectionFactory connectionFactory =
            new HedwigConnectionFactoryImpl();
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(destinationName);
        MessageConsumer consumer = session.createConsumer(destination);
        connection.start();

        populateDestination(nbMessages, destinationName, connection);

        // Consume messages and rollback transactions
        {
            AtomicInteger received = new AtomicInteger();
            Map<String, Boolean> rolledback = new ConcurrentHashMap<String, Boolean>();
            while (received.get() < nbMessages) {
                TextMessage msg = (TextMessage) consumer.receive(6000000);
                if (msg != null) {
                    if (msg != null && rolledback.put(msg.getText(), Boolean.TRUE) != null) {
                        LOG.info("Received message " + msg.getText() + " ("
                                 + received.getAndIncrement() + ")" + msg.getJMSMessageID());
                        assertTrue(msg.getJMSRedelivered());
                        session.commit();
                    } else {
                        LOG.info("Rollback message " + msg.getText() + " id: " +  msg.getJMSMessageID());
                        session.rollback();
                    }
                }
            }
            consumer.close();
            session.close();
        }
    }

    // AMQ-1593
    public void testValidateRedeliveryCountOnRollback() throws Exception {

        final int numMessages = 1;
        ConnectionFactory connectionFactory =
            new HedwigConnectionFactoryImpl();
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createTopic(destinationName);

        MessageConsumer consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id-1");
        connection.start();

        populateDestination(numMessages, destinationName, connection);

        {
            AtomicInteger received = new AtomicInteger();
            // hardcoded, we actually allow for infinite rollback/redelivery ...
            final int maxRetries = 5;
            while (received.get() < maxRetries) {
                TextMessage msg = (TextMessage) consumer.receive(1000);
                assert msg != null;
                if (msg != null) {
                    LOG.info("Received message " + msg.getText() + " ("
                             + received.getAndIncrement() + ")" + msg.getJMSMessageID());
                    session.rollback();
                }
            }
            session.close();
            consumeMessage(connection, "subscriber-id-1");
        }
    }

    // AMQ-1593
    public void testValidateRedeliveryCountOnRollbackWithPrefetch0() throws Exception {

       final int numMessages = 1;
       ConnectionFactory connectionFactory =
            new HedwigConnectionFactoryImpl();
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createTopic(destinationName);

        MessageConsumer consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id-2");
        connection.start();

        populateDestination(numMessages, destinationName, connection);

        {
            AtomicInteger received = new AtomicInteger();
            // hardcoded, we actually allow for infinite rollback/redelivery ...
            final int maxRetries = 5;
            while (received.get() < maxRetries) {
                TextMessage msg = (TextMessage) consumer.receive(1000);
                assert msg != null;
                if (msg != null) {
                    LOG.info("Received message " + msg.getText() + " ("
                             + received.getAndIncrement() + ")" + msg.getJMSMessageID());
                    session.rollback();
                }
            }

            session.close();
            consumeMessage(connection, "subscriber-id-2");
        }
    }


    private void consumeMessage(Connection connection, String subscriberId)
            throws JMSException {
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createTopic(destinationName);
        MessageConsumer consumer;
        if (null == subscriberId) consumer = session.createConsumer(destination);
        else consumer = session.createDurableSubscriber((Topic) destination, subscriberId);

        TextMessage msg = (TextMessage) consumer.receive(1000);
        assertNotNull(msg);
        session.commit();
        session.close();
    }

    public void testRedeliveryPropertyWithNoRollback() throws Exception {
        final int numMessages = 1;
        ConnectionFactory connectionFactory =
            new HedwigConnectionFactoryImpl();
        Connection connection = connectionFactory.createConnection();
        // ensure registration of durable subscription
        {
            connection.setClientID(getName() + "-client-id-1");
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination destination = session.createTopic(destinationName);

            MessageConsumer consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id-3");
        }
        connection.start();

        populateDestination(numMessages, destinationName, connection);
        connection.close();
        {
            AtomicInteger received = new AtomicInteger();
            // hardcoded, we actually allow for infinite rollback/redelivery ...
            final int maxRetries = 5;
            while (received.get() < maxRetries) {
                connection = connectionFactory.createConnection();
                connection.setClientID(getName() + "-client-id-1");
                connection.start();
                Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                Destination destination = session.createTopic(destinationName);

                MessageConsumer consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id-3");
                TextMessage msg = (TextMessage) consumer.receive(2000);
                assert msg != null;
                if (msg != null) {
                    LOG.info("Received message " + msg.getText() + " ("
                             + received.getAndIncrement() + ")" + msg.getJMSMessageID());
                }
                session.close();
                connection.close();
            }
            connection = connectionFactory.createConnection();
            connection.setClientID(getName() + "-client-id-1");
            connection.start();
            consumeMessage(connection, "subscriber-id-3");
        }
    }

    private void populateDestination(final int nbMessages,
            final String destinationName, Connection connection)
            throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(destinationName);
        MessageProducer producer = session.createProducer(destination);
        for (int i = 1; i <= nbMessages; i++) {
            producer.send(session.createTextMessage("<hello id='" + i + "'/>"));
        }
        producer.close();
        session.close();
    }


    private void populateDestinationWithInterleavedProducer(final int nbMessages,
            final String destinationName, Connection connection)
            throws JMSException {
        Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination1 = session1.createTopic(destinationName);
        MessageProducer producer1 = session1.createProducer(destination1);
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination2 = session2.createTopic(destinationName);
        MessageProducer producer2 = session2.createProducer(destination2);

        for (int i = 1; i <= nbMessages; i++) {
            if (i%2 == 0) {
                producer1.send(session1.createTextMessage("<hello id='" + i + "'/>"));
            } else {
                producer2.send(session2.createTextMessage("<hello id='" + i + "'/>"));
            }
        }
        producer1.close();
        session1.close();
        producer2.close();
        session2.close();
    }

}
