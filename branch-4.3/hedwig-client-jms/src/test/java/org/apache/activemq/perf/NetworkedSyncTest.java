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

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.apache.hedwig.JmsTestBase;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;


import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// For now, ignore it ...
@Ignore
public class NetworkedSyncTest extends JmsTestBase {

    // constants
    public static final int MESSAGE_COUNT = 10000; //100000;
    private static final Logger LOG = LoggerFactory.getLogger(NetworkedSyncTest.class);
    /**
     * @param name
     */
    public NetworkedSyncTest(String name) {
        super(name);
        LOG.info("Testcase started.");
    }

   public static void main(String args[]) {
       TestRunner.run(NetworkedSyncTest.class);
   }

    public void testMessageExchange() throws Exception {
        LOG.info("testMessageExchange() called.");

        long start = System.currentTimeMillis();

        // create producer and consumer threads
        Thread producer = new Thread(new Producer());
        Thread consumer = new Thread(new Consumer());
        // start threads
        consumer.start();
        Thread.sleep(2000);
        producer.start();

        // wait for threads to finish
        producer.join();
        consumer.join();
        long end = System.currentTimeMillis();

        System.out.println("Duration: "+(end-start));
    }
}

/**
 * Message producer running as a separate thread, connecting to broker1
 */
class Producer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    /**
     * connect to broker and constantly send messages
     */
    public void run() {

        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            HedwigConnectionFactoryImpl amq = new HedwigConnectionFactoryImpl();
            connection = amq.createConnection();

            connection.setExceptionListener(new javax.jms.ExceptionListener() {
                public void onException(javax.jms.JMSException e) {
                    e.printStackTrace();
                }
            });

            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic destination = session.createTopic("TEST.FOO");

            producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            long counter = 0;

            // Create and send message
            for (int i = 0; i < NetworkedSyncTest.MESSAGE_COUNT; i++) {

                String text = "Hello world! From: "
                        + Thread.currentThread().getName() + " : "
                        + this.hashCode() + ":" + counter;
                TextMessage message = session.createTextMessage(text);
                producer.send(message);
                counter++;

                if ((counter % 1000) == 0)
                    LOG.info("sent " + counter + " messages");

            }
        } catch (Exception ex) {
            LOG.error(ex.toString());
            return;
        } finally {
            try {
                if (producer != null)
                    producer.close();
                if (session != null)
                    session.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                LOG.error("Problem closing down JMS objects: " + e);
            }
        }
    }
}

/*
 * Message consumer running as a separate thread, connecting to broker2
 */
class Consumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);;

    /**
     * connect to broker and receive messages
     */
    public void run() {
        Connection connection = null;
        Session session = null;
        MessageConsumer consumer = null;

        try {
            HedwigConnectionFactoryImpl amq = new HedwigConnectionFactoryImpl();
            connection = amq.createConnection();
            // need to set clientID when using durable subscription.
            if (null == connection.getClientID()) connection.setClientID("tmielke");

            connection.setExceptionListener(new javax.jms.ExceptionListener() {
                public void onException(javax.jms.JMSException e) {
                    e.printStackTrace();
                }
            });

            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createTopic("TEST.FOO");
            consumer = session.createDurableSubscriber((Topic) destination,"tmielke");

            long counter = 0;
            // Wait for a message
            for (int i = 0; i < NetworkedSyncTest.MESSAGE_COUNT; i++) {
                Message message2 = consumer.receive();
                if (message2 instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message2;
                    String text = textMessage.getText();
                    // logger.info("Received: " + text);
                } else {
                    LOG.error("Received message of unsupported type. Expecting TextMessage. "+ message2);
                }
                counter++;
                if ((counter % 1000) == 0)
                    LOG.info("received " + counter + " messages");
            }
        } catch (Exception e) {
            LOG.error("Error in Consumer: " + e);
            return;
        } finally {
            try {
                if (consumer != null)
                    consumer.close();
                if (session != null)
                    session.close();
                if (connection != null)
                    connection.close();
            } catch (Exception ex) {
                LOG.error("Error closing down JMS objects: " + ex);
            }
        }
    }
}
