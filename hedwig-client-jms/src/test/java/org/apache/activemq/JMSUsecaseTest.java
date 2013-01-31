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

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import javax.jms.Destination;

import org.apache.hedwig.jms.MessagingSessionFacade;
import org.apache.hedwig.jms.SessionImpl;
import org.apache.hedwig.jms.message.MessageImpl;

public class JMSUsecaseTest extends JmsTestSupport {

    public Destination destination;
    public int deliveryMode;
    public int prefetch;
    public MessagingSessionFacade.DestinationType destinationType;
    public boolean durableConsumer;

    public static Test suite() {
        return suite(JMSUsecaseTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void initCombosForTestSendReceive() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testSendReceive() throws Exception {
        // Send a message to the broker.
        connection.start();
        SessionImpl session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageProducer producer = session.createProducer(destination);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageImpl message = new MessageImpl(session);
        producer.send(message);

        // Make sure only 1 message was delivered.
        assertNotNull(consumer.receive(1000));
        assertNull(consumer.receiveNoWait());
    }

    public void initCombosForTestSendReceiveTransacted() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testSendReceiveTransacted() throws Exception {
        // Send a message to the broker.
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        destination = createDestination(session, destinationType);
        MessageProducer producer = session.createProducer(destination);
        MessageConsumer consumer = session.createConsumer(destination);
        producer.send(session.createTextMessage("test"));

        // Message should not be delivered until commit.
        assertNull(consumer.receiveNoWait());
        session.commit();

        // Make sure only 1 message was delivered.
        Message message = consumer.receive(1000);
        assertNotNull(message);
        assertFalse(message.getJMSRedelivered());
        assertNull(consumer.receiveNoWait());

        // Message should be redelivered is rollback is used.
        session.rollback();

        // Make sure only 1 message was delivered.
        message = consumer.receive(2000);
        assertNotNull(message);
        assertTrue(message.getJMSRedelivered());
        assertNull(consumer.receiveNoWait());

        // If we commit now, the message should not be redelivered.
        session.commit();
        assertNull(consumer.receiveNoWait());
    }

}
