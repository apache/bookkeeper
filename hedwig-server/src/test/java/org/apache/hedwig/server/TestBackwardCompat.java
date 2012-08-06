/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.server;

import java.net.InetAddress;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

import junit.framework.TestCase;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Backward Compatability between different versions
 */
public class TestBackwardCompat extends TestCase {

    private static Logger logger = LoggerFactory.getLogger(TestBackwardCompat.class);

    static ZooKeeperUtil zkUtil = new ZooKeeperUtil();

    static class BookKeeperCluster400 {

        int numBookies;
        List<org.apache.hw_v4_0_0.bookkeeper.conf.ServerConfiguration> bkConfs;
        List<org.apache.hw_v4_0_0.bookkeeper.proto.BookieServer> bks;

        private int initialPort = 5000;
        private int nextPort = initialPort;

        BookKeeperCluster400(int numBookies) {
            this.numBookies = numBookies;
        }

        public void start() throws Exception {
            zkUtil.startServer();

            bks = new LinkedList<org.apache.hw_v4_0_0.bookkeeper.proto.BookieServer>();
            bkConfs = new LinkedList<org.apache.hw_v4_0_0.bookkeeper.conf.ServerConfiguration>();

            for (int i=0; i<numBookies; i++) {
                startBookieServer();
            }
        }

        public void stop() throws Exception {
            for (org.apache.hw_v4_0_0.bookkeeper.proto.BookieServer bs : bks) {
                bs.shutdown();
            }
            bks.clear();

            zkUtil.killServer();
        }

        protected void startBookieServer() throws Exception {
            File tmpDir = org.apache.hw_v4_0_0.hedwig.util.FileUtils.createTempDirectory(
                getClass().getName() + (nextPort - initialPort), "test");
            org.apache.hw_v4_0_0.bookkeeper.conf.ServerConfiguration conf = newServerConfiguration(
                nextPort++, zkUtil.getZooKeeperConnectString(), tmpDir, new File[] { tmpDir });
            bks.add(startBookie(conf));
            bkConfs.add(conf);
        }

        protected org.apache.hw_v4_0_0.bookkeeper.conf.ServerConfiguration newServerConfiguration(
            int port, String zkServers, File journalDir, File[] ledgerDirs) {
            org.apache.hw_v4_0_0.bookkeeper.conf.ServerConfiguration conf =
                new org.apache.hw_v4_0_0.bookkeeper.conf.ServerConfiguration();
            conf.setBookiePort(port);
            conf.setZkServers(zkServers);
            conf.setJournalDirName(journalDir.getPath());
            String[] ledgerDirNames = new String[ledgerDirs.length];
            for (int i=0; i<ledgerDirs.length; i++) {
                ledgerDirNames[i] = ledgerDirs[i].getPath();
            }
            conf.setLedgerDirNames(ledgerDirNames);
            return conf;
        }

        protected org.apache.hw_v4_0_0.bookkeeper.proto.BookieServer startBookie(
            org.apache.hw_v4_0_0.bookkeeper.conf.ServerConfiguration conf) throws Exception {
            org.apache.hw_v4_0_0.bookkeeper.proto.BookieServer server
                = new org.apache.hw_v4_0_0.bookkeeper.proto.BookieServer(conf);
            server.start();

            int port = conf.getBookiePort();
            while (zkUtil.getZooKeeperClient().exists(
                    "/ledgers/available/" + InetAddress.getLocalHost().getHostAddress() + ":" + port,
                    false) == null) {
                Thread.sleep(500);
            }
            return server;
        }
    }

    /**
     * Version 4.0.0 classes
     */
    static class Server400 {
        org.apache.hw_v4_0_0.hedwig.server.common.ServerConfiguration conf;
        org.apache.hw_v4_0_0.hedwig.server.netty.PubSubServer server;

        Server400(final String zkHosts) {
            conf = new org.apache.hw_v4_0_0.hedwig.server.common.ServerConfiguration() {
                @Override
                public String getZkHost() {
                    return zkHosts;
                }
            };
        }

        void start() throws Exception {
            server = new org.apache.hw_v4_0_0.hedwig.server.netty.PubSubServer(conf);
        }

        void stop() throws Exception {
            if (null != server) {
                server.shutdown();
            }
        }
    }

    static class Client400 {
        org.apache.hw_v4_0_0.hedwig.client.conf.ClientConfiguration conf;
        org.apache.hw_v4_0_0.hedwig.client.api.Client client;
        org.apache.hw_v4_0_0.hedwig.client.api.Publisher publisher;
        org.apache.hw_v4_0_0.hedwig.client.api.Subscriber subscriber;

        Client400() {
            conf = new org.apache.hw_v4_0_0.hedwig.client.conf.ClientConfiguration();
            client = new org.apache.hw_v4_0_0.hedwig.client.HedwigClient(conf);
            publisher = client.getPublisher();
            subscriber = client.getSubscriber();
        }

        void close() throws Exception {
            if (null != client) {
                client.close();
            }
        }

        org.apache.hw_v4_0_0.hedwig.protocol.PubSubProtocol.MessageSeqId publish(
            ByteString topic, ByteString data) throws Exception {
            org.apache.hw_v4_0_0.hedwig.protocol.PubSubProtocol.Message message =
                org.apache.hw_v4_0_0.hedwig.protocol.PubSubProtocol.Message.newBuilder()
                    .setBody(data).build();
            publisher.publish(topic, message);
            return null;
        }
    }

    static class BookKeeperCluster410 {

        int numBookies;
        List<org.apache.hw_v4_1_0.bookkeeper.conf.ServerConfiguration> bkConfs;
        List<org.apache.hw_v4_1_0.bookkeeper.proto.BookieServer> bks;

        private int initialPort = 5000;
        private int nextPort = initialPort;

        BookKeeperCluster410(int numBookies) {
            this.numBookies = numBookies;
        }

        public void start() throws Exception {
            zkUtil.startServer();

            bks = new LinkedList<org.apache.hw_v4_1_0.bookkeeper.proto.BookieServer>();
            bkConfs = new LinkedList<org.apache.hw_v4_1_0.bookkeeper.conf.ServerConfiguration>();

            for (int i=0; i<numBookies; i++) {
                startBookieServer();
            }
        }

        public void stop() throws Exception {
            for (org.apache.hw_v4_1_0.bookkeeper.proto.BookieServer bs : bks) {
                bs.shutdown();
            }
            bks.clear();

            zkUtil.killServer();
        }

        protected void startBookieServer() throws Exception {
            File tmpDir = org.apache.hw_v4_1_0.hedwig.util.FileUtils.createTempDirectory(
                getClass().getName() + (nextPort - initialPort), "test");
            org.apache.hw_v4_1_0.bookkeeper.conf.ServerConfiguration conf = newServerConfiguration(
                nextPort++, zkUtil.getZooKeeperConnectString(), tmpDir, new File[] { tmpDir });
            bks.add(startBookie(conf));
            bkConfs.add(conf);
        }

        protected org.apache.hw_v4_1_0.bookkeeper.conf.ServerConfiguration newServerConfiguration(
            int port, String zkServers, File journalDir, File[] ledgerDirs) {
            org.apache.hw_v4_1_0.bookkeeper.conf.ServerConfiguration conf =
                new org.apache.hw_v4_1_0.bookkeeper.conf.ServerConfiguration();
            conf.setBookiePort(port);
            conf.setZkServers(zkServers);
            conf.setJournalDirName(journalDir.getPath());
            String[] ledgerDirNames = new String[ledgerDirs.length];
            for (int i=0; i<ledgerDirs.length; i++) {
                ledgerDirNames[i] = ledgerDirs[i].getPath();
            }
            conf.setLedgerDirNames(ledgerDirNames);
            return conf;
        }

        protected org.apache.hw_v4_1_0.bookkeeper.proto.BookieServer startBookie(
            org.apache.hw_v4_1_0.bookkeeper.conf.ServerConfiguration conf) throws Exception {
            org.apache.hw_v4_1_0.bookkeeper.proto.BookieServer server
                = new org.apache.hw_v4_1_0.bookkeeper.proto.BookieServer(conf);
            server.start();

            int port = conf.getBookiePort();
            while (zkUtil.getZooKeeperClient().exists(
                    "/ledgers/available/" + InetAddress.getLocalHost().getHostAddress() + ":" + port,
                    false) == null) {
                Thread.sleep(500);
            }
            return server;
        }
    }

    /**
     * Version 4.1.0 classes
     */
    static class Server410 {
        org.apache.hw_v4_1_0.hedwig.server.common.ServerConfiguration conf;
        org.apache.hw_v4_1_0.hedwig.server.netty.PubSubServer server;

        Server410(final String zkHosts) {
            conf = new org.apache.hw_v4_1_0.hedwig.server.common.ServerConfiguration() {
                @Override
                public String getZkHost() {
                    return zkHosts;
                }
            };
        }

        void start() throws Exception {
            server = new org.apache.hw_v4_1_0.hedwig.server.netty.PubSubServer(conf);
            server.start();
        }

        void stop() throws Exception {
            if (null != server) {
                server.shutdown();
            }
        }
    }

    static class Client410 {
        org.apache.hw_v4_1_0.hedwig.client.conf.ClientConfiguration conf;
        org.apache.hw_v4_1_0.hedwig.client.api.Client client;
        org.apache.hw_v4_1_0.hedwig.client.api.Publisher publisher;
        org.apache.hw_v4_1_0.hedwig.client.api.Subscriber subscriber;

        Client410() {
            conf = new org.apache.hw_v4_1_0.hedwig.client.conf.ClientConfiguration();
            client = new org.apache.hw_v4_1_0.hedwig.client.HedwigClient(conf);
            publisher = client.getPublisher();
            subscriber = client.getSubscriber();
        }

        void close() throws Exception {
            if (null != client) {
                client.close();
            }
        }

        org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.MessageSeqId publish(
            ByteString topic, ByteString data) throws Exception {
            org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.Message message =
                org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.Message.newBuilder()
                    .setBody(data).build();
            publisher.publish(topic, message);
            return null;
        }
    }

    /**
     * Current Version
     */
    static class BookKeeperClusterCurrent {

        int numBookies;
        List<org.apache.bookkeeper.conf.ServerConfiguration> bkConfs;
        List<org.apache.bookkeeper.proto.BookieServer> bks;

        private int initialPort = 5000;
        private int nextPort = initialPort;

        BookKeeperClusterCurrent(int numBookies) {
            this.numBookies = numBookies;
        }

        public void start() throws Exception {
            zkUtil.startServer();

            bks = new LinkedList<org.apache.bookkeeper.proto.BookieServer>();
            bkConfs = new LinkedList<org.apache.bookkeeper.conf.ServerConfiguration>();

            for (int i=0; i<numBookies; i++) {
                startBookieServer();
            }
        }

        public void stop() throws Exception {
            for (org.apache.bookkeeper.proto.BookieServer bs : bks) {
                bs.shutdown();
            }
            bks.clear();

            zkUtil.killServer();
        }

        protected void startBookieServer() throws Exception {
            File tmpDir = org.apache.hedwig.util.FileUtils.createTempDirectory(
                getClass().getName() + (nextPort - initialPort), "test");
            org.apache.bookkeeper.conf.ServerConfiguration conf = newServerConfiguration(
                nextPort++, zkUtil.getZooKeeperConnectString(), tmpDir, new File[] { tmpDir });
            bks.add(startBookie(conf));
            bkConfs.add(conf);
        }

        protected org.apache.bookkeeper.conf.ServerConfiguration newServerConfiguration(
            int port, String zkServers, File journalDir, File[] ledgerDirs) {
            org.apache.bookkeeper.conf.ServerConfiguration conf =
                new org.apache.bookkeeper.conf.ServerConfiguration();
            conf.setBookiePort(port);
            conf.setZkServers(zkServers);
            conf.setJournalDirName(journalDir.getPath());
            String[] ledgerDirNames = new String[ledgerDirs.length];
            for (int i=0; i<ledgerDirs.length; i++) {
                ledgerDirNames[i] = ledgerDirs[i].getPath();
            }
            conf.setLedgerDirNames(ledgerDirNames);
            return conf;
        }

        protected org.apache.bookkeeper.proto.BookieServer startBookie(
            org.apache.bookkeeper.conf.ServerConfiguration conf) throws Exception {
            org.apache.bookkeeper.proto.BookieServer server
                = new org.apache.bookkeeper.proto.BookieServer(conf);
            server.start();

            int port = conf.getBookiePort();
            while (zkUtil.getZooKeeperClient().exists(
                    "/ledgers/available/" + InetAddress.getLocalHost().getHostAddress() + ":" + port,
                    false) == null) {
                Thread.sleep(500);
            }
            return server;
        }
    }

    static class ServerCurrent {
        org.apache.hedwig.server.common.ServerConfiguration conf;
        org.apache.hedwig.server.netty.PubSubServer server;

        ServerCurrent(final String zkHosts) {
            conf = new org.apache.hedwig.server.common.ServerConfiguration() {
                @Override
                public String getZkHost() {
                    return zkHosts;
                }
            };
        }

        void start() throws Exception {
            server = new org.apache.hedwig.server.netty.PubSubServer(conf);
            server.start();
        }

        void stop() throws Exception {
            if (null != server) {
                server.shutdown();
            }
        }
    }

    static class ClientCurrent {
        org.apache.hedwig.client.conf.ClientConfiguration conf;
        org.apache.hedwig.client.api.Client client;
        org.apache.hedwig.client.api.Publisher publisher;
        org.apache.hedwig.client.api.Subscriber subscriber;

        class IntMessageHandler implements org.apache.hedwig.client.api.MessageHandler {
            ByteString topic;
            ByteString subId;
            int next;

            CountDownLatch latch;

            IntMessageHandler(ByteString t, ByteString s, int start, int num) {
                this.topic = t;
                this.subId = s;
                this.next = start;
                this.latch = new CountDownLatch(num);
            }

            @Override
            public void deliver(ByteString t, ByteString s,
                                org.apache.hedwig.protocol.PubSubProtocol.Message msg,
                                org.apache.hedwig.util.Callback<Void> callback, Object context) {
                if (!t.equals(topic) || !s.equals(subId)) {
                    return;
                }
                int num = Integer.parseInt(msg.getBody().toStringUtf8());
                if (num == next) {
                    latch.countDown();
                    ++next;
                }
                callback.operationFinished(context, null);
            }

            public boolean await(long timeout, TimeUnit unit)
            throws InterruptedException {
                return latch.await(timeout, unit);
            }
        }

        ClientCurrent() {
            conf = new org.apache.hedwig.client.conf.ClientConfiguration() {
                @Override
                public boolean isAutoSendConsumeMessageEnabled() {
                    return true;
                }
                @Override
                public int getConsumedMessagesBufferSize() {
                    return 1;
                }
            };
            client = new org.apache.hedwig.client.HedwigClient(conf);
            publisher = client.getPublisher();
            subscriber = client.getSubscriber();
        }

        void close() throws Exception {
            if (null != client) {
                client.close();
            }
        }

        org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId publish(
            ByteString topic, ByteString data) throws Exception {
            org.apache.hedwig.protocol.PubSubProtocol.Message message =
                org.apache.hedwig.protocol.PubSubProtocol.Message.newBuilder()
                    .setBody(data).build();
            org.apache.hedwig.protocol.PubSubProtocol.PublishResponse resp =
                publisher.publish(topic, message);
            if (null == resp) {
                return null;
            }
            return resp.getPublishedMsgId();
        }

        void publishInts(ByteString topic, int start, int num) throws Exception {
            for (int i=0; i<num; i++) {
                org.apache.hedwig.protocol.PubSubProtocol.Message msg =
                    org.apache.hedwig.protocol.PubSubProtocol.Message.newBuilder().setBody(ByteString.copyFromUtf8("" + (start+i))).build();
                publisher.publish(topic, msg);
            }
        }

        void sendXExpectLastY(ByteString topic, ByteString subid, final int x, final int y)
        throws Exception {
            for (int i=0; i<x; i++) {
                publisher.publish(topic, org.apache.hedwig.protocol.PubSubProtocol.Message.newBuilder().setBody(
                                         ByteString.copyFromUtf8(String.valueOf(i))).build());
            }
            subscriber.subscribe(topic, subid, org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach.ATTACH);

            final AtomicInteger expected = new AtomicInteger(x - y);
            final CountDownLatch latch = new CountDownLatch(1);
            subscriber.startDelivery(topic, subid, new org.apache.hedwig.client.api.MessageHandler() {
                @Override
                synchronized public void deliver(ByteString topic, ByteString subscriberId,
                                                 org.apache.hedwig.protocol.PubSubProtocol.Message msg,
                                                 org.apache.hedwig.util.Callback<Void> callback, Object context) {
                    try {
                        int value = Integer.valueOf(msg.getBody().toStringUtf8());
                        if (value == expected.get()) {
                            expected.incrementAndGet();
                        } else {
                            logger.error("Did not receive expected value, expected {}, got {}",
                                         expected.get(), value);
                            expected.set(0);
                            latch.countDown();
                        }
                        if (expected.get() == x) {
                            latch.countDown();
                        }
                        callback.operationFinished(context, null);
                    } catch (Exception e) {
                        logger.error("Received bad message", e);
                        latch.countDown();
                    }
                }
            });
            assertTrue("Timed out waiting for messages Y is " + y + " expected is currently "
                       + expected.get(), latch.await(10, TimeUnit.SECONDS));
            assertEquals("Should be expected message with " + x, x, expected.get());
            subscriber.stopDelivery(topic, subid);
            subscriber.closeSubscription(topic, subid);
        }

        void subscribe(ByteString topic, ByteString subscriberId) throws Exception {
            org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions options =
                org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions.newBuilder()
                .setCreateOrAttach(org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach.CREATE_OR_ATTACH).build();
            subscribe(topic, subscriberId, options);
        }

        void subscribe(ByteString topic, ByteString subscriberId,
                       org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions options) throws Exception {
            subscriber.subscribe(topic, subscriberId, options);
        }

        void closeSubscription(ByteString topic, ByteString subscriberId) throws Exception {
            subscriber.closeSubscription(topic, subscriberId);
        }

        void receiveInts(ByteString topic, ByteString subscriberId, int start, int num) throws Exception {
            IntMessageHandler msgHandler = new IntMessageHandler(topic, subscriberId, start, num);
            subscriber.startDelivery(topic, subscriberId, msgHandler);
            msgHandler.await(num, TimeUnit.SECONDS);
            subscriber.stopDelivery(topic, subscriberId);
        }

    }

    /**
     * Test compatability of message bound between version 4.0.0 and
     * current version.
     *
     * 1) message bound doesn't take effects on 4.0.0 server.
     * 2) message bound take effects on both 4.1.0 and current server
     */
    @Test
    public void testMessageBoundCompat() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("testMessageBoundCompat");
        ByteString subid = ByteString.copyFromUtf8("mysub");

        // start bookkeeper 400
        BookKeeperCluster400 bkc400 = new BookKeeperCluster400(3);
        bkc400.start();

        // start 400 server
        Server400 s400 = new Server400(zkUtil.getZooKeeperConnectString());
        s400.start();

        org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions options5cur =
            org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions.newBuilder()
            .setCreateOrAttach(org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach.CREATE_OR_ATTACH)
            .setMessageBound(5).build();

        ClientCurrent ccur = new ClientCurrent();
        ccur.subscribe(topic, subid, options5cur);
        ccur.closeSubscription(topic, subid);
        ccur.sendXExpectLastY(topic, subid, 50, 50);

        // stop 400 servers
        s400.stop();
        bkc400.stop();

        // start bookkeeper 410
        BookKeeperCluster410 bkc410 = new BookKeeperCluster410(3);
        bkc410.start();

        // start 410 server
        Server410 s410 = new Server410(zkUtil.getZooKeeperConnectString());
        s410.start();

        ccur.subscribe(topic, subid, options5cur);
        ccur.closeSubscription(topic, subid);
        ccur.sendXExpectLastY(topic, subid, 50, 5);

        // stop 410 servers
        s410.stop();
        bkc410.stop();

        // start bookkeeper current
        BookKeeperClusterCurrent bkccur = new BookKeeperClusterCurrent(3);
        bkccur.start();

        // start current server
        ServerCurrent scur = new ServerCurrent(zkUtil.getZooKeeperConnectString());
        scur.start();

        ccur.subscribe(topic, subid, options5cur);
        ccur.closeSubscription(topic, subid);
        ccur.sendXExpectLastY(topic, subid, 50, 5);

        // stop current servers
        scur.stop();
        bkccur.stop();

        ccur.close();
    }

    /**
     * Test compatability of publish interface between version 4.1.0
     * and current verison.
     *
     * 1) 4.1.0 client could talk with current server.
     * 2) current client could talk with 4.1.0 server,
     *    but no message seq id would be returned
     */
    @Test
    public void testPublishCompat410() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("TestPublishCompat410");
        ByteString data = ByteString.copyFromUtf8("testdata");

        // start bookkeeper 410
        BookKeeperCluster410 bkc410 = new BookKeeperCluster410(3);
        bkc410.start();

        // start 410 server
        Server410 s410 = new Server410(zkUtil.getZooKeeperConnectString());
        s410.start();

        ClientCurrent ccur = new ClientCurrent();
        Client410 c410 = new Client410();

        // client c410 could publish message to 410 server
        assertNull(c410.publish(topic, data));
        // client ccur could publish message to 410 server
        // but no message seq id would be returned
        assertNull(ccur.publish(topic, data));

        // stop 410 server
        s410.stop();

        // start current server
        ServerCurrent scur = new ServerCurrent(zkUtil.getZooKeeperConnectString());
        scur.start();

        // client c410 could publish message to 410 server
        // but no message seq id would be returned
        assertNull(c410.publish(topic, data));
        // client ccur could publish message to current server
        assertNotNull(ccur.publish(topic, data));

        ccur.close();
        c410.close();

        // stop current server
        scur.stop();
        bkc410.stop();
    }

}
