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
 * WITHOUT WARRANTIES OR ONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.hedwig.JmsTestBase;
import org.apache.hedwig.jms.SessionImpl;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DurableSubSelectorDelayTest extends JmsTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubSelectorDelayTest.class);

    public static final long RUNTIME = 3 * 60 * 1000;

    private Topic topic;

    public void testProcess() throws Exception {

        DurableSubscriber subscribers[] = new DurableSubscriber[10];

        for (int i = 0; i < subscribers.length; i++) {
            subscribers[i] = new DurableSubscriber(i);
        }

        MsgProducer msgProducer = new MsgProducer();
        msgProducer.start();

        for (int i = 0; i < subscribers.length; i++) {
            subscribers[i].process();
        }


        // wait for server to finish
        msgProducer.join();

        for (int j = 0; j < subscribers.length; j++) {
            LOG.info("Unsubscribing subscriber " + subscribers[j]);
            subscribers[j].unsubscribe();
        }

        // allow the clean up thread time to run
        TimeUnit.MINUTES.sleep(2);
        LOG.info("DONE.");
    }

    /**
     * Message Producer
     */
    final class MsgProducer extends Thread {

        final ConnectionFactory cf = new HedwigConnectionFactoryImpl();

        int transRover = 0;
        int messageRover = 0;

        public MsgProducer() {
            super("MsgProducer");
            setDaemon(true);
        }

        @Override
        public void run() {
            long endTime = RUNTIME + System.currentTimeMillis();

            try {
                while (endTime > System.currentTimeMillis()) {
                    Thread.sleep(400);
                    send();
                }
            } catch (Throwable e) {
               e.printStackTrace(System.out);
               throw new RuntimeException(e);
            }
        }

        public void send() throws JMSException {

            int trans = ++transRover;
            boolean relevantTrans = true;
            int count = 40;

            LOG.info("Sending Trans[id=" + trans + ", count="
                    + count + "]");

            Connection con = cf.createConnection();

            Session sess = con.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer prod = sess.createProducer(null);

            for (int i = 0; i < count; i++) {
                Message message = sess.createMessage();
                message.setIntProperty("ID", ++messageRover);
                message.setIntProperty("TRANS", trans);
                message.setBooleanProperty("RELEVANT", false);
                prod.send(topic, message);
            }

            Message message = sess.createMessage();
            message.setIntProperty("ID", ++messageRover);
            message.setIntProperty("TRANS", trans);
            message.setBooleanProperty("COMMIT", true);
            message.setBooleanProperty("RELEVANT", relevantTrans);
            prod.send(topic, message);

            LOG.info("Committed Trans[id=" + trans + ", count="
                    + count + "], ID=" + messageRover);

            sess.close();
            con.close();
        }
    }

    /**
     * Consumes massages from a durable subscription. Goes online/offline
     * periodically. Checks the incoming messages against the sent messages of
     * the server.
     */
    private final class DurableSubscriber {

        final ConnectionFactory cf = new HedwigConnectionFactoryImpl();

        private final String subName ;

        private final int id;
        private final String conClientId;
        private final String selector;

        private final Session sess;
        private final MessageConsumer consumer;
        private final Connection con;

        public DurableSubscriber(int id) throws JMSException {
            this.id = id;
            conClientId = "cli" + id;
            subName = "subscription"+ id;
            selector ="RELEVANT = true";
            con = openConnection();
            sess = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            consumer = sess.createDurableSubscriber(topic, subName, selector, false);

        }

        private void process() throws JMSException {
            long end = System.currentTimeMillis() + 20000;
            int transCount = 0;

            LOG.info(toString() + " ONLINE.");

            try {

                do {
                    long max = end - System.currentTimeMillis();

                    if (max <= 0) {
                            break;
                    }

                    Message message = consumer.receive(max);
                    if (message == null) {
                        continue;
                    }

                    LOG.info("Received Trans[id="
                            + message.getIntProperty("TRANS") + ", count="
                            + transCount + "] in " + this + ".");

                } while (true);

            } finally {
                sess.close();
                con.close();

                LOG.info(toString() + " OFFLINE.");
            }
        }

        private Connection openConnection() throws JMSException {
            Connection con = cf.createConnection();
            con.setClientID(conClientId);
            con.start();
            return con;
        }

        private void unsubscribe() throws JMSException {
            Connection con = openConnection();
            Session session = con
                    .createSession(false, Session.AUTO_ACKNOWLEDGE);
            // Call a dummp createDurableSubscriber (o.a.h.jms.package-info.html for more on why).
            session.createDurableSubscriber(topic, subName, selector, false);
            session.unsubscribe(subName);
            session.close();
            con.close();
        }

        @Override
        public String toString() {
            return "DurableSubscriber[id=" + id + "]";
        }
    }

    public void setUp() throws Exception {
        super.setUp();
        topic = SessionImpl.asTopic("TopicT");
    }

    public String getName() {
        return "DurableSubSelectorDelayTest";
    }

    private static boolean delete(File path) {
        if (path == null)
            return true;

        if (path.isDirectory()) {
            for (File file : path.listFiles()) {
                delete(file);
            }
        }
        return path.delete();
    }
}
