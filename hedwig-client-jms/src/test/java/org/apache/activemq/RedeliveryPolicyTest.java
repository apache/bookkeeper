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
import org.apache.hedwig.jms.SessionImpl;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Test;



/**
 * Test cases used to test the JMS message exclusive consumers.
 *
 *
 */
public class RedeliveryPolicyTest extends JmsTestSupport {

    public static Test suite() {
        return suite(RedeliveryPolicyTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    /**
     * @throws Exception
     */
    public void testExponentialRedeliveryPolicyDelaysDeliveryOnRollback() throws Exception {

        connection.start();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Topic destination = SessionImpl.asTopic(getName());
        MessageProducer producer = session.createProducer(destination);

        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        producer.send(session.createTextMessage("1st"));
        producer.send(session.createTextMessage("2nd"));
        session.commit();

        TextMessage m;
        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());
        session.rollback();

        // No delay on first rollback..
        m = (TextMessage)consumer.receive(100);
        assertNotNull(m);
        session.rollback();

        m = (TextMessage)consumer.receive(700);
        assertNotNull(m);
        assertEquals("1st", m.getText());
        session.rollback();

        // Show re-delivery delay is incrementing exponentially
        m = (TextMessage)consumer.receive(100);
        assertNotNull(m);
        assertEquals("1st", m.getText());

        m = (TextMessage)consumer.receive(100);
        assertNotNull(m);
        assertEquals("2nd", m.getText());
    }


    /**
     * @throws Exception
     */
    public void testInfiniteMaximumNumberOfRedeliveries() throws Exception {

        connection.start();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Topic destination = SessionImpl.asTopic("TEST");
        MessageProducer producer = session.createProducer(destination);

        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        producer.send(session.createTextMessage("1st"));
        producer.send(session.createTextMessage("2nd"));
        session.commit();

        TextMessage m;

        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());
        session.rollback();

        //we should be able to get the 1st message redelivered until a session.commit is called
        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());
        session.rollback();

        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());
        session.rollback();

        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());
        session.rollback();

        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());
        session.rollback();

        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());
        session.commit();

        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("2nd", m.getText());
        session.commit();

    }
}
