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

import javax.jms.Topic;
import java.util.concurrent.CountDownLatch;

import org.apache.hedwig.JmsTestBase;
import org.apache.hedwig.jms.SessionImpl;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchMultipleConsumersTest extends JmsTestBase {
    private final static Logger logger = LoggerFactory.getLogger(DispatchMultipleConsumersTest.class);
    Destination dest;
    String destinationName = "TEST.Q";
    String msgStr = "Test text message";
    int messagesPerThread = 20;
    int producerThreads = 50;
    int consumerCount = 2;
    AtomicInteger sentCount;
    AtomicInteger consumedCount;
    CountDownLatch producerLatch;
    CountDownLatch consumerLatch;
    String userName = "";
    String password = "";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        dest = SessionImpl.asTopic(destinationName);
    }

    private void resetCounters() {
        sentCount = new AtomicInteger(0);
        consumedCount = new AtomicInteger(0);
        producerLatch = new CountDownLatch(producerThreads);
        consumerLatch = new CountDownLatch(consumerCount);
    }

    public void testDispatch1() {
        for (int i = 1; i <= 5; i++) {
            resetCounters();
            dispatch();
            assertEquals("Incorrect messages in Iteration " + i, sentCount.get() * consumerCount, consumedCount.get());
        }
    }

    private void dispatch() {
        startConsumers();
        startProducers();
        try {
            producerLatch.await();
            consumerLatch.await();
        } catch (InterruptedException e) {
            fail("test interrupted!");
        }
    }

    private void startConsumers() {
        HedwigConnectionFactoryImpl connFactory = new HedwigConnectionFactoryImpl();
        Connection conn;
        try {
            conn = connFactory.createConnection(userName, password);
            conn.start();
            for (int i = 0; i < consumerCount; i++) {
                ConsumerThread th = new ConsumerThread(conn, "ConsumerThread"+i);
                th.start();
            }
        } catch (JMSException e) {
            logger.error("Failed to start consumers", e);
        }
    }

    private void startProducers() {
        HedwigConnectionFactoryImpl connFactory = new HedwigConnectionFactoryImpl();
        for (int i = 0; i < producerThreads; i++) {
            Thread th = new ProducerThread(connFactory, messagesPerThread, "ProducerThread"+i);
            th.start();
        }
    }

    private class ConsumerThread extends Thread {
        private Session session;
        private MessageConsumer consumer;

        public ConsumerThread(Connection conn, String name) {
            super();
            this.setName(name);
            logger.trace("Created new consumer thread:" + name);
            try {
                session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                consumer = session.createConsumer(dest);
            } catch (JMSException e) {
                logger.error("Failed to start consumer thread:" + name, e);
            }
        }

        @Override
        public void run() {
            int msgCount = 0;
            int nullCount = 0;
            while (true) {
                try {
                    Message msg = consumer.receive(200);
                    if (msg == null) {
                        if (producerLatch.getCount() > 0) {
                            continue;
                        }
                        nullCount++;
                        if (nullCount > 10) {
                            //assume that we are not getting any more messages
                            break;
                        } else {
                            continue;
                        }
                    } else {
                        nullCount = 0;
                    }
                    // Thread.sleep(100);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Message received:" + msg.getJMSMessageID());
                    }
                    msgCount++;
                } catch (JMSException e) {
                    logger.error("Failed to consume:", e);
                    /*
                } catch (InterruptedException e) {
                    logger.error("Interrupted!", e);
                    */
                }
            }
            try {
                consumer.close();
            } catch (JMSException e) {
                logger.error("Failed to close consumer " + getName(), e);
            }
            consumedCount.addAndGet(msgCount);
            consumerLatch.countDown();
            logger.trace("Consumed " + msgCount + " messages using thread " + getName());
        }
    }

    private class ProducerThread extends Thread {
        private int count;
        private Connection conn;
        private Session session;
        private MessageProducer producer;

        public ProducerThread(HedwigConnectionFactoryImpl connFactory, int count, String name) {
            super();
            this.count = count;
            this.setName(name);
            logger.trace("Created new producer thread:" + name);
            try {
                conn = connFactory.createConnection();
                conn.start();
                session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                producer = session.createProducer(dest);
            } catch (JMSException e) {
                logger.error("Failed to start producer thread:" + name, e);
            }
        }

        @Override
        public void run() {
            int i = 0;
            try {
                for (; i < count; i++) {
                    producer.send(session.createTextMessage(msgStr));
                    // Thread.sleep(500);
                }
                conn.close();
            } catch (JMSException e) {
                logger.error(e.getMessage(), e);
                /*
            } catch (InterruptedException e) {
                logger.error("Interrupted!", e);
                */
            }
            sentCount.addAndGet(i);
            producerLatch.countDown();
            if (logger.isTraceEnabled()) {
                logger.trace("Sent " + i + " messages from thread " + getName());
            }
        }
    }
}
