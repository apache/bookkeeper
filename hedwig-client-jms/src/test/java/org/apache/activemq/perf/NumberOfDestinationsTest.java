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
package org.apache.activemq.perf;

/**
 * A NumberOfDestinationsTest
 *
 */
import javax.jms.Topic;
import java.io.File;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.hedwig.JmsTestBase;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;


import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// For now, ignore it ...
@Ignore
public class NumberOfDestinationsTest extends JmsTestBase {
    protected static final int MESSAGE_COUNT = 1;
    protected static final int NUMBER_OF_DESTINATIONS = 100000;
    private static final Logger LOG = LoggerFactory.getLogger(NumberOfDestinationsTest.class);
    protected int destinationCount;

    public void testDestinations() throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer mp = session.createProducer(null);
        for (int j = 0; j < NUMBER_OF_DESTINATIONS; j++) {
            Destination dest = getDestination(session);

            for (int i = 0; i < MESSAGE_COUNT; i++) {
                Message msg = session.createTextMessage("test" + i);
                mp.send(dest, msg);
            }
            if (j % 500 == 0) {
                LOG.info("Iterator " + j);
            }
        }
        connection.close();
    }

    protected Destination getDestination(Session session) throws JMSException {
        String topicName = getClass().getName() + "." + destinationCount++;
        return session.createTopic(topicName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected HedwigConnectionFactoryImpl createConnectionFactory() throws Exception {
        HedwigConnectionFactoryImpl cf = new HedwigConnectionFactoryImpl();
        return cf;
    }

}
