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
package org.apache.activemq.usecases;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.test.TestSupport;
import org.apache.activemq.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicRedeliverTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(TopicRedeliverTest.class);
    private static final int RECEIVE_TIMEOUT = 10000;

    protected int deliveryMode = DeliveryMode.PERSISTENT;
    private IdGenerator idGen = new IdGenerator();

    public TopicRedeliverTest() {
    }

    public TopicRedeliverTest(String n) {
        super(n);
    }

    protected void setUp() throws Exception {
        super.setUp();
        topic = true;
    }

    /**
     * test messages are acknowledged and recovered properly
     * @throws Exception
     */
    public void testClientAcknowledge() throws Exception {
        Destination destination = createDestination(getClass().getName());
        Connection connection = createConnection();
        final String clientId = idGen.generateId();
        connection.setClientID(clientId);
        connection.start();
        Session consumerSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createDurableSubscriber((Topic) destination, "subscriber-id1");
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        // send some messages

        TextMessage sent1 = producerSession.createTextMessage();
        sent1.setText("msg1");
        producer.send(sent1);

        TextMessage sent2 = producerSession.createTextMessage();
        sent1.setText("msg2");
        producer.send(sent2);

        TextMessage sent3 = producerSession.createTextMessage();
        sent1.setText("msg3");
        producer.send(sent3);

        consumer.receive(RECEIVE_TIMEOUT);
        Message rec2 = consumer.receive(RECEIVE_TIMEOUT);
        consumer.receive(RECEIVE_TIMEOUT);

        // ack rec2 - in hedwig, this implicitly ack's rec1 too ...
        rec2.acknowledge();

        TextMessage sent4 = producerSession.createTextMessage();
        sent4.setText("msg4");
        producer.send(sent4);

        Message rec4 = consumer.receive(RECEIVE_TIMEOUT);
        // assertTrue(rec4.equals(sent4));
        assert rec4 instanceof TextMessage;
        assertTrue(((TextMessage) rec4).getText().equals(sent4.getText()));
        // We DO NOT support session recovery - to unblock this test,
        // I am stopp'ing and start'ing connection : not the same, but ...
        // consumerSession.recover();
        connection.close();
        connection = createConnection();
        // same client id !
        connection.setClientID(clientId);
        consumerSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = consumerSession.createDurableSubscriber((Topic) destination, "subscriber-id1");
        connection.start();

        consumer.receive(RECEIVE_TIMEOUT);
        rec4 = consumer.receive(RECEIVE_TIMEOUT);
        // assertTrue(rec4.equals(sent4));
        assert rec4 instanceof TextMessage : "rec4 == " + rec4;
        assertTrue(((TextMessage) rec4).getText().equals(sent4.getText()));
        // assertTrue(rec4.getJMSRedelivered());
        rec4.acknowledge();
        connection.close();

    }

    /**
     * Test redelivered flag is set on rollbacked transactions
     *
     * @throws Exception
     */
    public void testRedilveredFlagSetOnRollback() throws Exception {
        Destination destination = createDestination(getClass().getName());
        Connection connection = createConnection();
        connection.setClientID(idGen.generateId());
        connection.start();
        Session consumerSession = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = null;
        if (topic) {
            consumer = consumerSession.createDurableSubscriber((Topic)destination, "TESTRED");
        } else {
            consumer = consumerSession.createConsumer(destination);
        }
        Session producerSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        TextMessage sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg1");
        producer.send(sentMsg);
        producerSession.commit();

        Message recMsg = consumer.receive(RECEIVE_TIMEOUT);
        assertFalse(recMsg.getJMSRedelivered());
        recMsg = consumer.receive(RECEIVE_TIMEOUT);
        consumerSession.rollback();
        recMsg = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue(recMsg.getJMSRedelivered());
        consumerSession.commit();
        // assertTrue(recMsg.equals(sentMsg));
        assert recMsg instanceof TextMessage;
        assertTrue(((TextMessage) recMsg).getText().equals(sentMsg.getText()));
        assertTrue(recMsg.getJMSRedelivered());
        connection.close();
    }

    public void testNoExceptionOnRedeliveryAckWithSimpleTopicConsumer() throws Exception {
        Destination destination = createDestination(getClass().getName());
        Connection connection = createConnection();
        final AtomicBoolean gotException = new AtomicBoolean();
        connection.setExceptionListener(new ExceptionListener() {
            public void onException(JMSException exception) {
                LOG.error("unexpected ex:" + exception);
                    gotException.set(true);
            }
        });
        connection.setClientID(idGen.generateId());
        connection.start();
        Session consumerSession = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = null;
        if (topic) {
            consumer = consumerSession.createConsumer((Topic)destination);
        } else {
            consumer = consumerSession.createConsumer(destination);
        }
        Session producerSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        TextMessage sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg1");
        producer.send(sentMsg);
        producerSession.commit();

        Message recMsg = consumer.receive(RECEIVE_TIMEOUT);
        assertFalse(recMsg.getJMSRedelivered());
        recMsg = consumer.receive(RECEIVE_TIMEOUT);
        consumerSession.rollback();
        recMsg = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue(recMsg.getJMSRedelivered());
        consumerSession.rollback();
        recMsg = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue(recMsg.getJMSRedelivered());
        consumerSession.commit();
        // assertTrue(recMsg.equals(sentMsg));
        assert recMsg instanceof TextMessage;
        assertTrue(((TextMessage) recMsg).getText().equals(sentMsg.getText()));
        assertTrue(recMsg.getJMSRedelivered());
        connection.close();

        assertFalse("no exception", gotException.get());
    }

    /**
     * Check a session is rollbacked on a Session close();
     *
     * @throws Exception
     */

    public void xtestTransactionRollbackOnSessionClose() throws Exception {
        Destination destination = createDestination(getClass().getName());
        Connection connection = createConnection();
        connection.setClientID(idGen.generateId());
        connection.start();
        Session consumerSession = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = null;
        if (topic) {
            consumer = consumerSession.createDurableSubscriber((Topic)destination, "TESTRED");
        } else {
            consumer = consumerSession.createConsumer(destination);
        }
        Session producerSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        TextMessage sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg1");
        producer.send(sentMsg);

        producerSession.commit();

        Message recMsg = consumer.receive(RECEIVE_TIMEOUT);
        assertFalse(recMsg.getJMSRedelivered());
        consumerSession.close();
        consumerSession = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        consumer = consumerSession.createConsumer(destination);

        recMsg = consumer.receive(RECEIVE_TIMEOUT);
        consumerSession.commit();
        // assertTrue(recMsg.equals(sentMsg));
        assert recMsg instanceof TextMessage;
        assertTrue(((TextMessage) recMsg).getText().equals(sentMsg.getText()));
        connection.close();
    }

    /**
     * check messages are actuallly sent on a tx rollback
     *
     * @throws Exception
     */

    public void testTransactionRollbackOnSend() throws Exception {
        Destination destination = createDestination(getClass().getName());
        Connection connection = createConnection();
        connection.setClientID(idGen.generateId());
        connection.start();
        Session consumerSession = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(destination);
        Session producerSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        TextMessage sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg1");
        producer.send(sentMsg);
        producerSession.commit();

        Message recMsg = consumer.receive(RECEIVE_TIMEOUT);
        consumerSession.commit();
        // assertTrue(recMsg.equals(sentMsg));
        assert recMsg instanceof TextMessage;
        assertTrue(((TextMessage) recMsg).getText().equals(sentMsg.getText()));

        sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg2");
        producer.send(sentMsg);
        producerSession.rollback();

        sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg3");
        producer.send(sentMsg);
        producerSession.commit();

        recMsg = consumer.receive(RECEIVE_TIMEOUT);
        // assertTrue(recMsg.equals(sentMsg));
        assert recMsg instanceof TextMessage;
        assertTrue(((TextMessage) recMsg).getText().equals(sentMsg.getText()));
        consumerSession.commit();

        connection.close();
    }

}
