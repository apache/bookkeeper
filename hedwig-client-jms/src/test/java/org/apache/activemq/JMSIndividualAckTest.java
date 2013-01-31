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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

/**
 *
 */
public class JMSIndividualAckTest extends TestSupport {

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

    /**
     * Tests if acknowledged messages are being consumed.
     *
     * @throws JMSException
     */
    public void testAckedMessageAreConsumed() throws JMSException {
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic queue = session.createTopic(getQueueName());
        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createDurableSubscriber(queue, "subscriber-id1");

        producer.send(session.createTextMessage("Hello"));

        // Consume the message...
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        msg.acknowledge();

        // Reset the session.
        session.close();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Attempt to Consume the message...
        consumer = session.createDurableSubscriber(queue, "subscriber-id1");
        msg = consumer.receive(1000);
        assertNull(msg);

        session.close();
    }

    /**
     * Tests if acknowledged messages are being consumed.
     *
     * @throws JMSException
     */
    // This test cant, unfortunately, pass
    //- in hedwig, acknowledge is a ACKNOWLEDGE UNTIL. So the last ack will ack all messages until then ...
    /*
    public void testLastMessageAcked() throws JMSException {
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic queue = session.createTopic(getQueueName());
        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createDurableSubscriber(queue, "subscriber-id2");
        TextMessage msg1 = session.createTextMessage("msg1");
        TextMessage msg2 = session.createTextMessage("msg2");
        TextMessage msg3 = session.createTextMessage("msg3");
        producer.send(msg1);
        producer.send(msg2);
        producer.send(msg3);

        // Consume the message...
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        msg = consumer.receive(1000);
        assertNotNull(msg);
        msg = consumer.receive(1000);
        assertNotNull(msg);
        msg.acknowledge();

        // Reset the session.
        session.close();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Attempt to Consume the message...
        consumer = session.createDurableSubscriber(queue, "subscriber-id2");
        msg = consumer.receive(1000);
        assertNotNull(msg);
        assertEquals(msg1,msg);
        msg = consumer.receive(1000);
        assertNotNull(msg);
        assertEquals(msg2,msg);
        msg = consumer.receive(1000);
        assertNull(msg);
        session.close();
    }
    */

    /**
     * Tests if unacknowledged messages are being re-delivered when the consumer connects again.
     *
     * @throws JMSException
     */
    public void testUnAckedMessageAreNotConsumedOnSessionClose() throws JMSException {
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic queue = session.createTopic(getQueueName());
        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createDurableSubscriber(queue, "subscriber-id3");
        producer.send(session.createTextMessage("Hello"));

        // Consume the message...
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        // Don't ack the message.

        // Reset the session.  This should cause the unacknowledged message to be re-delivered.
        session.close();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Attempt to Consume the message...
        consumer = session.createDurableSubscriber(queue, "subscriber-id3");
        msg = consumer.receive(2000);
        assertNotNull(msg);
        msg.acknowledge();

        session.close();
    }

    protected String getQueueName() {
        return getClass().getName() + "." + getName();
    }

}
