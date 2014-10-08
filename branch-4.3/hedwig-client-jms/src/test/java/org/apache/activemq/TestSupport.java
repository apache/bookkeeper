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
import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;


import org.apache.hedwig.jms.SessionImpl;
import org.apache.hedwig.jms.message.MessageImpl;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;
import org.apache.hedwig.jms.spi.HedwigConnectionImpl;


/**
 * Useful base class for unit test cases
 */
public abstract class TestSupport extends CombinationTestSupport {

    protected HedwigConnectionFactoryImpl connectionFactory;
    protected boolean topic = true;

    protected MessageImpl createMessage() {
        return new MessageImpl(null);
    }

    protected Destination createDestination(String subject) {
        if (topic) {
            return SessionImpl.asTopic(subject);
        } else {
            throw new IllegalArgumentException("Queue NOT supported");
        }
    }

    protected Destination createDestination() {
        return createDestination(getDestinationString());
    }

    /**
     * Returns the name of the destination used in this test case
     */
    protected String getDestinationString() {
        return getClass().getName() + "." + getName(true);
    }

    /**
     * @param messsage
     * @param firstSet
     * @param secondSet
     */
    protected void assertTextMessagesEqual(String messsage, Message[] firstSet, Message[] secondSet)
        throws JMSException {
        assertEquals("Message count does not match: " + messsage, firstSet.length, secondSet.length);
        for (int i = 0; i < secondSet.length; i++) {
            TextMessage m1 = (TextMessage)firstSet[i];
            TextMessage m2 = (TextMessage)secondSet[i];
            assertFalse("Message " + (i + 1) + " did not match : " + messsage + ": expected {" + m1
                        + "}, but was {" + m2 + "}", m1 == null ^ m2 == null);
            assertEquals("Message " + (i + 1) + " did not match: " + messsage + ": expected {" + m1
                         + "}, but was {" + m2 + "}", m1.getText(), m2.getText());
        }
    }

    protected HedwigConnectionFactoryImpl createConnectionFactory() throws Exception {
        return new HedwigConnectionFactoryImpl();
    }

    /**
     * Factory method to create a new connection
     */
    protected Connection createConnection() throws Exception {
        return createConnection(true);
    }

    protected Connection createConnection(boolean setClientId) throws Exception {
        HedwigConnectionImpl connection = getConnectionFactory().createConnection();
        if (setClientId) connection.setClientID(getName());
        return connection;
    }

    public HedwigConnectionFactoryImpl getConnectionFactory() throws Exception {
        if (connectionFactory == null) {
            connectionFactory = createConnectionFactory();
            assertTrue("Should have created a connection factory!", connectionFactory != null);
        }
        return connectionFactory;
    }

    protected String getConsumerSubject() {
        return getSubject();
    }

    protected String getProducerSubject() {
        return getSubject();
    }

    protected String getSubject() {
        return getName();
    }

    public static void recursiveDelete(File f) {
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            for (int i = 0; i < files.length; i++) {
                recursiveDelete(files[i]);
            }
        }
        f.delete();
    }

    public static void removeMessageStore() {
        if (System.getProperty("activemq.store.dir") != null) {
            recursiveDelete(new File(System.getProperty("activemq.store.dir")));
        }
        if (System.getProperty("derby.system.home") != null) {
            recursiveDelete(new File(System.getProperty("derby.system.home")));
        }
    }

    /**
     * Test if base directory contains spaces
     */
    protected void assertBaseDirectoryContainsSpaces() {
        assertFalse("Base directory cannot contain spaces.",
                    new File(System.getProperty("basedir", ".")).getAbsoluteFile().toString().contains(" "));
    }

}
