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
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.hedwig.jms.SessionImpl;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;
import org.apache.activemq.CombinationTestSupport;

import javax.jms.Destination;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ObjectMessageNotSerializableTest extends CombinationTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectMessageNotSerializableTest.class);

    AtomicInteger numReceived = new AtomicInteger(0);
    final Vector<Throwable> exceptions = new Vector<Throwable>();

    public static Test suite() {
        return suite(ObjectMessageNotSerializableTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected void setUp() throws Exception {
        super.setUp();
        exceptions.clear();
    }

    public void testSendNotSerializeableObjectMessage() throws Exception {

        final  Destination destination = SessionImpl.asTopic("testT");
        final MyObject obj = new MyObject("A message");

        final CountDownLatch consumerStarted = new CountDownLatch(1);

        Thread vmConsumerThread = new Thread("Consumer Thread") {
                public void run() {
                    try {
                    HedwigConnectionFactoryImpl factory = new HedwigConnectionFactoryImpl();

                    Connection connection = factory.createConnection();
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageConsumer consumer = session.createConsumer(destination);
                    connection.start();
                    consumerStarted.countDown();
                    ObjectMessage message = (ObjectMessage) consumer.receive(30000);
                    if ( message != null ) {
                        MyObject object = (MyObject)message.getObject();
                        LOG.info("Got message " + object.getMessage());
                        numReceived.incrementAndGet();
                    }
                    consumer.close();
                    } catch (Throwable ex) {
                        exceptions.add(ex);
                    }
                }
            };
        vmConsumerThread.start();

        Thread producingThread = new Thread("Producing Thread") {
                public void run() {
                    try {
                        HedwigConnectionFactoryImpl factory = new HedwigConnectionFactoryImpl();

                        Connection connection = factory.createConnection();
                        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        MessageProducer producer = session.createProducer(destination);
                        ObjectMessage message = session.createObjectMessage();
                        message.setObject(obj);
                        producer.send(message);
                        producer.close();
                    } catch (Throwable ex) {
                        exceptions.add(ex);
                    }
                }
            };

        assertTrue("consumers started", consumerStarted.await(10, TimeUnit.SECONDS));
        producingThread.start();

        vmConsumerThread.join();
        producingThread.join();

        assert obj.getWriteObjectCalled() > 0 : "writeObject not called";
        assert 0 == obj.getReadObjectCalled() : "readObject called";
        assert 0 == obj.getReadObjectNoDataCalled() : "readObjectNoData called ?";

        assertEquals("Got expected messages", 1, numReceived.get());
        assertTrue("no unexpected exceptions: " + exceptions, exceptions.isEmpty());
    }

    public void testSendNotSerializeableObjectMessageOverTcp() throws Exception {
        final  Destination destination = SessionImpl.asTopic("testTopic");
        final MyObject obj = new MyObject("A message");

        final CountDownLatch consumerStarted = new CountDownLatch(3);
        final Vector<Throwable> exceptions = new Vector<Throwable>();
        Thread vmConsumerThread = new Thread("Consumer Thread") {
                public void run() {
                    try {
                        HedwigConnectionFactoryImpl factory = new HedwigConnectionFactoryImpl();

                        Connection connection = factory.createConnection();
                        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        MessageConsumer consumer = session.createConsumer(destination);
                        connection.start();
                        consumerStarted.countDown();
                        ObjectMessage message = (ObjectMessage)consumer.receive(30000);
                        if ( message != null ) {
                        MyObject object = (MyObject)message.getObject();
                        LOG.info("Got message " + object.getMessage());
                        numReceived.incrementAndGet();
                    }
                        consumer.close();
                    } catch (Throwable ex) {
                        exceptions.add(ex);
                    }
                }
            };
        vmConsumerThread.start();

        Thread tcpConsumerThread = new Thread("Consumer Thread") {
                public void run() {
                    try {

                    HedwigConnectionFactoryImpl factory =
                        new HedwigConnectionFactoryImpl();

                    Connection connection = factory.createConnection();
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageConsumer consumer = session.createConsumer(destination);
                    connection.start();
                    consumerStarted.countDown();
                    ObjectMessage message = (ObjectMessage)consumer.receive(30000);
                    if ( message != null ) {
                        MyObject object = (MyObject)message.getObject();
                        LOG.info("Got message " + object.getMessage());
                        numReceived.incrementAndGet();
                        assert object.getReadObjectCalled() > 0 : "readObject called";
                    }
                    consumer.close();
                    } catch (Throwable ex) {
                        exceptions.add(ex);
                    }
                }
            };
        tcpConsumerThread.start();


        Thread notherVmConsumerThread = new Thread("Consumer Thread") {
            public void run() {
                try {
                    HedwigConnectionFactoryImpl factory = new HedwigConnectionFactoryImpl();

                    Connection connection = factory.createConnection();
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageConsumer consumer = session.createConsumer(destination);
                    connection.start();
                    consumerStarted.countDown();
                    ObjectMessage message = (ObjectMessage)consumer.receive(30000);
                    if ( message != null ) {
                        MyObject object = (MyObject)message.getObject();
                        LOG.info("Got message " + object.getMessage());
                        numReceived.incrementAndGet();
                    }
                    consumer.close();
                } catch (Throwable ex) {
                    exceptions.add(ex);
                }
            }
        };
        notherVmConsumerThread.start();

        Thread producingThread = new Thread("Producing Thread") {
            public void run() {
                try {
                    HedwigConnectionFactoryImpl factory = new HedwigConnectionFactoryImpl();

                    Connection connection = factory.createConnection();
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(destination);
                    ObjectMessage message = (ObjectMessage)session.createObjectMessage();
                    message.setObject(obj);
                    producer.send(message);
                    producer.close();
                } catch (Throwable ex) {
                    exceptions.add(ex);
                }
            }
            };

        assertTrue("consumers started", consumerStarted.await(10, TimeUnit.SECONDS));
        producingThread.start();

        vmConsumerThread.join();
        tcpConsumerThread.join();
        notherVmConsumerThread.join();
        producingThread.join();

        assertEquals("writeObject called", 1, obj.getWriteObjectCalled());
        assertEquals("readObject called", 0, obj.getReadObjectCalled());
        assertEquals("readObjectNoData called", 0, obj.getReadObjectNoDataCalled());

        assertEquals("Got expected messages", 3, numReceived.get());
        assertTrue("no unexpected exceptions: " + exceptions, exceptions.isEmpty());
    }
}
