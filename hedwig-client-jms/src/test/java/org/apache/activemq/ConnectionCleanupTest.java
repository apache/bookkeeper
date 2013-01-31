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

import javax.jms.JMSException;
import javax.jms.Session;

import junit.framework.TestCase;
import org.apache.hedwig.JmsTestBase;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;
import org.apache.hedwig.jms.spi.HedwigConnectionImpl;

public class ConnectionCleanupTest extends JmsTestBase {

    private HedwigConnectionImpl connection;
    private HedwigConnectionFactoryImpl factory;

    protected void setUp() throws Exception {
        super.setUp();
        this.factory = new HedwigConnectionFactoryImpl();
        connection = factory.createConnection();
    }

    /**
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        connection.close();
        super.tearDown();
    }

    /**
     * @throws JMSException
     */
    public void testChangeClientID() throws JMSException {

        connection.setClientID("test");
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        try {
            connection.setClientID("test");
            fail("Should have received JMSException");
        } catch (JMSException e) {
        }

        connection.close();
        connection = factory.createConnection();
        connection.setClientID("test");

        connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        try {
            connection.setClientID("test");
            fail("Should have received JMSException");
        } catch (JMSException e) {
        }
    }

}
