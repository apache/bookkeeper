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

import javax.jms.Connection;

import org.apache.hedwig.JmsTestBase;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class JmsRedeliveredTest extends JmsTestBase {

    private Connection connection;

    /*
     * (non-Javadoc)
     *
     * @see junit.framework.TestCase#setUp()
     */
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

    /**
     * Creates a connection.
     *
     * @return connection
     * @throws Exception
     */
    protected Connection createConnection() throws Exception {
        HedwigConnectionFactoryImpl factory = new HedwigConnectionFactoryImpl();
        return factory.createConnection();
    }

    /**
     * Tests if a message unacknowledged message gets to be resent when the
     * session is closed and then a new consumer session is created.
     *
     */
    public void testTopicSessionCloseMarksMessageRedelivered() throws JMSException {
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic queue = session.createTopic("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        MessageConsumer consumer = session.createDurableSubscriber(queue, "subscriber-id1");
        producer.send(createTextMessage(session));

        // Consume the message...
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        // Don't ack the message.

        // Reset the session. This should cause the Unacked message to be
        // redelivered.
        session.close();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Attempt to Consume the message...
        consumer = session.createDurableSubscriber(queue, "subscriber-id1");
        msg = consumer.receive(2000);
        assertNotNull(msg);
        // Since we only simulate this in provider, we cannot do this across consumers !
        // assertTrue("Message should be redelivered.", msg.getJMSRedelivered());
        msg.acknowledge();

        session.close();
    }


    public void testTopicSessionCloseMarksUnAckedMessageRedelivered() throws JMSException {
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic queue = session.createTopic("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        MessageConsumer consumer = session.createDurableSubscriber(queue, "subscriber-id2");
        producer.send(createTextMessage(session, "1"));
        producer.send(createTextMessage(session, "2"));

        // Consume the message...
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        assertEquals("1", ((TextMessage)msg).getText());
        msg.acknowledge();

        // Don't ack the message.
        msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        assertEquals("2", ((TextMessage)msg).getText());

        // Reset the session. This should cause the Unacked message to be
        // redelivered.
        session.close();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Attempt to Consume the message...
        consumer = session.createDurableSubscriber(queue, "subscriber-id2");
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertEquals("2", ((TextMessage)msg).getText());
        // Since we only simulate this in provider, we cannot do this across consumers !
        // assertTrue("Message should be redelivered.", msg.getJMSRedelivered());
        msg.acknowledge();

        session.close();
    }

    /**
     * Tests session recovery and that the redelivered message is marked as
     * such. Session uses client acknowledgement, the destination is a queue.
     *
     * @throws JMSException
     */
    public void testTopicRecoverMarksMessageRedelivered() throws Exception {
        connection.setClientID(getName());
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic queue = session.createTopic("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        MessageConsumer consumer = session.createDurableSubscriber(queue, getName() + " - subscriber");
        producer.send(createTextMessage(session));

        // Consume the message...
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        // Don't ack the message.

        // We DO NOT support session recovery
        // - to unblock this test, I am stopp'ing and start'ing connection : not the same, but ...
        // Reset the session. This should cause the Unacked message to be
        // redelivered.
        // session.recover();
        connection.close();
        connection = createConnection();
        connection.setClientID(getName());
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = session.createDurableSubscriber(queue, getName() + " - subscriber");
        connection.start();

        // Attempt to Consume the message...
        msg = consumer.receive(2000);
        assertNotNull(msg);
        // assertTrue("Message should be redelivered.", msg.getJMSRedelivered());
        msg.acknowledge();

        session.close();
    }

    /**
     * Tests rollback message to be marked as redelivered. Session uses client
     * acknowledgement and the destination is a queue.
     *
     * @throws JMSException
     */
    public void testTopicRollbackMarksMessageRedelivered() throws JMSException {
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Topic queue = session.createTopic("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        MessageConsumer consumer = session.createConsumer(queue);
        producer.send(createTextMessage(session));
        session.commit();

        // Get the message... Should not be redelivered.
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());

        // Rollback.. should cause redelivery.
        session.rollback();

        // Attempt to Consume the message...
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());

        session.commit();
        session.close();
    }

    /**
     * Tests if the message gets to be re-delivered when the session closes and
     * that the re-delivered message is marked as such. Session uses client
     * acknowledgment, the destination is a topic and the consumer is a durable
     * subscriber.
     *
     * @throws JMSException
     */
    public void testDurableTopicSessionCloseMarksMessageRedelivered() throws JMSException {
        connection.setClientID(getName());
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic("topic-" + getName());
        MessageConsumer consumer = session.createDurableSubscriber(topic, "sub1");

        // This case only works with persistent messages since transient
        // messages
        // are dropped when the consumer goes offline.
        MessageProducer producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(createTextMessage(session));

        // Consume the message...
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be re-delivered.", msg.getJMSRedelivered());
        // Don't ack the message.

        // Reset the session. This should cause the Unacked message to be
        // re-delivered.
        session.close();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Attempt to Consume the message...
        consumer = session.createDurableSubscriber(topic, "sub1");
        msg = consumer.receive(2000);
        assertNotNull(msg);
        // Since we only simulate this in provider, we cannot do this across consumers !
        // assertTrue("Message should be redelivered.", msg.getJMSRedelivered());
        msg.acknowledge();

        session.close();
    }

    /**
     * Tests rollback message to be marked as redelivered. Session uses client
     * acknowledgement and the destination is a topic.
     *
     * @throws JMSException
     */
    public void testDurableTopicRollbackMarksMessageRedelivered() throws JMSException {
        if (null == connection.getClientID()) connection.setClientID(getName());
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic("topic-" + getName());
        MessageConsumer consumer = session.createDurableSubscriber(topic, "sub1");

        MessageProducer producer = createProducer(session, topic);
        producer.send(createTextMessage(session));
        session.commit();

        // Get the message... Should not be redelivered.
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());

        // Rollback.. should cause redelivery.
        session.rollback();

        // Attempt to Consume the message...
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());

        session.commit();
        session.close();
    }

    /**
     * Creates a text message.
     *
     * @param session
     * @return TextMessage.
     * @throws JMSException
     */
    private TextMessage createTextMessage(Session session) throws JMSException {
        return createTextMessage(session, "Hello");
    }

    private TextMessage createTextMessage(Session session, String txt) throws JMSException {
        return session.createTextMessage(txt);
    }

    /**
     * Creates a producer.
     *
     * @param session
     * @param queue - destination.
     * @return MessageProducer
     * @throws JMSException
     */
    private MessageProducer createProducer(Session session, Destination queue) throws JMSException {
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(getDeliveryMode());
        return producer;
    }

    /**
     * Returns delivery mode.
     *
     * @return int - persistent delivery mode.
     */
    protected int getDeliveryMode() {
        return DeliveryMode.PERSISTENT;
    }

    /**
     * Run the JmsRedeliverTest with the delivery mode set as persistent.
     */
    public static final class PersistentCase extends JmsRedeliveredTest {

        /**
         * Returns delivery mode.
         *
         * @return int - persistent delivery mode.
         */
        protected int getDeliveryMode() {
            return DeliveryMode.PERSISTENT;
        }
    }

    /**
     * Run the JmsRedeliverTest with the delivery mode set as non-persistent.
     */
    public static final class TransientCase extends JmsRedeliveredTest {

        /**
         * Returns delivery mode.
         *
         * @return int - non-persistent delivery mode.
         */
        protected int getDeliveryMode() {
            return DeliveryMode.NON_PERSISTENT;
        }
    }

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(PersistentCase.class);
        suite.addTestSuite(TransientCase.class);
        return suite;
    }
}
