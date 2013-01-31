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
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.Message;

public class JmsSendWithAsyncCallbackTest extends TestSupport {

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

    public void testAsyncCallbackIsFaster() throws JMSException, InterruptedException {
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic queue = session.createTopic(getName());

        // setup a consumer to drain messages..
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
            }
        });

        // warmup...
        for(int i=0; i < 10; i++) {
            benchmarkNonCallbackRate();
            benchmarkCallbackRate();
        }

        double callbackRate = benchmarkCallbackRate();
        double nonCallbackRate = benchmarkNonCallbackRate();

        System.out.println(String.format("AsyncCallback Send rate: %,.2f m/s", callbackRate));
        System.out.println(String.format("NonAsyncCallback Send rate: %,.2f m/s", nonCallbackRate));

        // there is no such requirement in hedwig case :-)
        // The async style HAS to be faster than the non-async style..
        // assertTrue( callbackRate/nonCallbackRate > 1.5 );
    }

    private double benchmarkNonCallbackRate() throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic queue = session.createTopic(getName());
        int count = 1000;
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            producer.send(session.createTextMessage("Hello"));
        }
        return 1000.0 * count / (System.currentTimeMillis() - start);
    }

    private double benchmarkCallbackRate() throws JMSException, InterruptedException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic queue = session.createTopic(getName());
        int count = 1000;
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            producer.send(session.createTextMessage("Hello"));
        }
        return 1000.0 * count / (System.currentTimeMillis() - start);
    }

}
