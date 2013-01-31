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
import javax.jms.Connection;

import org.apache.hedwig.JmsTestBase;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;


public class TimeStampTest extends JmsTestBase {
    public void test() throws Exception {
        // Create a ConnectionFactory
        HedwigConnectionFactoryImpl connectionFactory =
            new HedwigConnectionFactoryImpl();

        // Create a Connection
        Connection connection = connectionFactory.createConnection();
        connection.start();

        // Create a Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination Queue
        Destination destination = session.createTopic("TEST.FOO");

        // Create a MessageProducer from the Session to the Topic or Queue
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        // Create a MessageConsumer from the Session to the Topic or Queue
        MessageConsumer consumer = session.createConsumer(destination);
        // Create a messages
        Message sentMessage = session.createMessage();

        // Tell the producer to send the message
        long beforeSend = System.currentTimeMillis();
        producer.send(sentMessage);
        long afterSend = System.currentTimeMillis();

        // assert message timestamp is in window
        assertTrue(beforeSend <= sentMessage.getJMSTimestamp() && sentMessage.getJMSTimestamp() <= afterSend);


        // Wait for a message
        Message receivedMessage = consumer.receive(1000);

        // assert we got the same message ID we sent
        assertEquals(sentMessage.getJMSMessageID(), receivedMessage.getJMSMessageID());

        // assert message timestamp is in window
        assertTrue("JMS Message Timestamp should be set during the send method: \n" + "        beforeSend = "
                   + beforeSend + "\n" + "   getJMSTimestamp = "
                   + receivedMessage.getJMSTimestamp() + "\n" + "         afterSend = "
                   + afterSend + "\n", beforeSend <= receivedMessage.getJMSTimestamp()
                   && receivedMessage.getJMSTimestamp() <= afterSend);

        // assert message timestamp is unchanged
        assertEquals("JMS Message Timestamp of recieved message should be the same as the sent message\n        ",
                     sentMessage.getJMSTimestamp(), receivedMessage.getJMSTimestamp());

        // Clean up
        producer.close();
        consumer.close();
        session.close();
        connection.close();
    }
}
