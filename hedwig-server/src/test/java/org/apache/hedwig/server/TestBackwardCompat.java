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
import java.io.IOException;
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
import org.apache.bookkeeper.test.PortManager;

import org.apache.hedwig.util.HedwigSocketAddress;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Backward Compatability between different versions
 */
public class TestBackwardCompat extends TestCase {

    private static Logger logger = LoggerFactory.getLogger(TestBackwardCompat.class);

    static final int CONSUMEINTERVAL = 5;
    static ZooKeeperUtil zkUtil = new ZooKeeperUtil();

    static class BookKeeperCluster400 {

        int numBookies;
        List<org.apache.hw_v4_0_0.bookkeeper.conf.ServerConfiguration> bkConfs;
        List<org.apache.hw_v4_0_0.bookkeeper.proto.BookieServer> bks;

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
            int port = PortManager.nextFreePort();
            File tmpDir = org.apache.hw_v4_0_0.hedwig.util.FileUtils.createTempDirectory(
                getClass().getName() + port, "test");
            org.apache.hw_v4_0_0.bookkeeper.conf.ServerConfiguration conf = newServerConfiguration(
                port, zkUtil.getZooKeeperConnectString(), tmpDir, new File[] { tmpDir });
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

        Server400(final String zkHosts, final int port, final int sslPort) {
            conf = new org.apache.hw_v4_0_0.hedwig.server.common.ServerConfiguration() {
                @Override
                public String getZkHost() {
                    return zkHosts;
                }

                @Override
                public int getServerPort() {
                    return port;
                }

                @Override
                public int getSSLServerPort() {
                    return sslPort;
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

        Client400(final String connectString) {
            conf = new org.apache.hw_v4_0_0.hedwig.client.conf.ClientConfiguration() {
                    @Override
                    protected org.apache.hw_v4_0_0.hedwig.util.HedwigSocketAddress
                        getDefaultServerHedwigSocketAddress() {
                        return new org.apache.hw_v4_0_0.hedwig.util.HedwigSocketAddress(connectString);
                    }
                };
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
            int port = PortManager.nextFreePort();
            File tmpDir = org.apache.hw_v4_1_0.hedwig.util.FileUtils.createTempDirectory(
                getClass().getName() + port, "test");
            org.apache.hw_v4_1_0.bookkeeper.conf.ServerConfiguration conf = newServerConfiguration(
                    port, zkUtil.getZooKeeperConnectString(), tmpDir, new File[] { tmpDir });
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

        Server410(final String zkHosts, final int port, final int sslPort) {
            conf = new org.apache.hw_v4_1_0.hedwig.server.common.ServerConfiguration() {
                @Override
                public int getConsumeInterval() {
                    return CONSUMEINTERVAL;
                }
                @Override
                public String getZkHost() {
                    return zkHosts;
                }

                @Override
                public int getServerPort() {
                    return port;
                }

                @Override
                public int getSSLServerPort() {
                    return sslPort;
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

        class IntMessageHandler implements org.apache.hw_v4_1_0.hedwig.client.api.MessageHandler {
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
                                org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.Message msg,
                                org.apache.hw_v4_1_0.hedwig.util.Callback<Void> callback, Object context) {
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

        Client410(final String connectString) {
            conf = new org.apache.hw_v4_1_0.hedwig.client.conf.ClientConfiguration() {
                @Override
                public boolean isAutoSendConsumeMessageEnabled() {
                    return true;
                }
                @Override
                public int getConsumedMessagesBufferSize() {
                    return 1;
                }
                @Override
                protected org.apache.hw_v4_1_0.hedwig.util.HedwigSocketAddress
                    getDefaultServerHedwigSocketAddress() {
                    return new org.apache.hw_v4_1_0.hedwig.util.HedwigSocketAddress(connectString);
                }
            };
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

        void publishInts(ByteString topic, int start, int num) throws Exception {
            for (int i=0; i<num; i++) {
                org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.Message msg =
                    org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.Message.newBuilder().setBody(ByteString.copyFromUtf8("" + (start+i))).build();
                publisher.publish(topic, msg);
            }
        }

        void sendXExpectLastY(ByteString topic, ByteString subid, final int x, final int y)
        throws Exception {
            for (int i=0; i<x; i++) {
                publisher.publish(topic, org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.Message.newBuilder().setBody(
                                         ByteString.copyFromUtf8(String.valueOf(i))).build());
            }
            subscriber.subscribe(topic, subid, org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach.ATTACH);

            final AtomicInteger expected = new AtomicInteger(x - y);
            final CountDownLatch latch = new CountDownLatch(1);
            subscriber.startDelivery(topic, subid, new org.apache.hw_v4_1_0.hedwig.client.api.MessageHandler() {
                @Override
                synchronized public void deliver(ByteString topic, ByteString subscriberId,
                                                 org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.Message msg,
                                                 org.apache.hw_v4_1_0.hedwig.util.Callback<Void> callback, Object context) {
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
            org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.SubscriptionOptions options =
                org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.SubscriptionOptions.newBuilder()
                .setCreateOrAttach(org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach.CREATE_OR_ATTACH).build();
            subscribe(topic, subscriberId, options);
        }

        void subscribe(ByteString topic, ByteString subscriberId,
                       org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.SubscriptionOptions options) throws Exception {
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
     * Current Version
     */
    static class BookKeeperClusterCurrent {

        int numBookies;
        List<org.apache.bookkeeper.conf.ServerConfiguration> bkConfs;
        List<org.apache.bookkeeper.proto.BookieServer> bks;


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
            int port = PortManager.nextFreePort();
            File tmpDir = org.apache.hedwig.util.FileUtils.createTempDirectory(
                getClass().getName() + port, "test");
            org.apache.bookkeeper.conf.ServerConfiguration conf = newServerConfiguration(
                port, zkUtil.getZooKeeperConnectString(), tmpDir, new File[] { tmpDir });
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

        ServerCurrent(final String zkHosts, final int port, final int sslPort) {
            conf = new org.apache.hedwig.server.common.ServerConfiguration() {
                @Override
                public int getConsumeInterval() {
                    return CONSUMEINTERVAL;
                }

                @Override
                public String getZkHost() {
                    return zkHosts;
                }

                @Override
                public int getServerPort() {
                    return port;
                }

                @Override
                public int getSSLServerPort() {
                    return sslPort;
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

        ClientCurrent(final String connectString) {
            this(true, connectString);
        }

        ClientCurrent(final boolean autoConsumeEnabled, final String connectString) {
            conf = new org.apache.hedwig.client.conf.ClientConfiguration() {
                @Override
                public boolean isAutoSendConsumeMessageEnabled() {
                    return autoConsumeEnabled;
                }
                @Override
                public int getConsumedMessagesBufferSize() {
                    return 1;
                }
                @Override
                protected HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
                    return new HedwigSocketAddress(connectString);
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

        void receiveNumModM(final ByteString topic, final ByteString subid,
                            final int start, final int num, final int M) throws Exception {
            org.apache.hedwig.filter.ServerMessageFilter filter =
                new org.apache.hedwig.filter.ServerMessageFilter() {

                @Override
                public org.apache.hedwig.filter.ServerMessageFilter
                    initialize(Configuration conf) {
                    // do nothing
                    return this;
                }

                @Override
                public void uninitialize() {
                    // do nothing;
                }

                @Override
                public org.apache.hedwig.filter.MessageFilterBase
                    setSubscriptionPreferences(ByteString topic, ByteString subscriberId,
                    org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences preferences) {
                    // do nothing;
                    return this;
                }

                @Override
                public boolean testMessage(org.apache.hedwig.protocol.PubSubProtocol.Message msg) {
                    int value = Integer.valueOf(msg.getBody().toStringUtf8());
                    return 0 == value % M;
                }
            };
            filter.initialize(conf.getConf());

            subscriber.subscribe(topic, subid, org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach.ATTACH);
            final int base = start + M - start % M;
            final AtomicInteger expected = new AtomicInteger(base);
            final CountDownLatch latch = new CountDownLatch(1);
            subscriber.startDeliveryWithFilter(topic, subid, new org.apache.hedwig.client.api.MessageHandler() {
                synchronized public void deliver(ByteString topic, ByteString subscriberId,
                                                 org.apache.hedwig.protocol.PubSubProtocol.Message msg,
                                                 org.apache.hedwig.util.Callback<Void> callback, Object context) {
                    try {
                        int value = Integer.valueOf(msg.getBody().toStringUtf8());
                        // duplicated messages received, ignore them
                        if (value > start) {
                            if (value == expected.get()) {
                                expected.addAndGet(M);
                            } else {
                                logger.error("Did not receive expected value, expected {}, got {}",
                                             expected.get(), value);
                                expected.set(0);
                                latch.countDown();
                            }
                            if (expected.get() == (base + num * M)) {
                                latch.countDown();
                            }
                        }
                        callback.operationFinished(context, null);
                    } catch (Exception e) {
                        logger.error("Received bad message", e);
                        latch.countDown();
                    }
                }
            }, (org.apache.hedwig.filter.ClientMessageFilter) filter);
            assertTrue("Timed out waiting for messages mod " + M + " expected is " + expected.get(),
                       latch.await(10, TimeUnit.SECONDS));
            assertEquals("Should be expected message with " + (base + num * M), (base + num*M), expected.get());
            subscriber.stopDelivery(topic, subid);
            filter.uninitialize();
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

        // throttle doesn't work talking with 41 server
        void throttleX41(ByteString topic, ByteString subid, final int X)
        throws Exception {
            org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions options =
                org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions.newBuilder()
                .setCreateOrAttach(org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach.CREATE_OR_ATTACH)
                .setMessageWindowSize(X) .build();
            subscribe(topic, subid, options);
            closeSubscription(topic, subid);
            publishInts(topic, 1, 3*X);
            subscribe(topic, subid);

            final AtomicInteger expected = new AtomicInteger(1);
            final CountDownLatch throttleLatch = new CountDownLatch(1);
            final CountDownLatch nonThrottleLatch = new CountDownLatch(1);
            subscriber.startDelivery(topic, subid, new org.apache.hedwig.client.api.MessageHandler() {
                @Override
                public synchronized void deliver(ByteString topic, ByteString subscriberId,
                                                 org.apache.hedwig.protocol.PubSubProtocol.Message msg,
                                                 org.apache.hedwig.util.Callback<Void> callback, Object context) {
                    try {
                        int value = Integer.valueOf(msg.getBody().toStringUtf8());
                        logger.debug("Received message {},", value);

                        if (value == expected.get()) {
                            expected.incrementAndGet();
                        } else {
                            // error condition
                            logger.error("Did not receive expected value, expected {}, got {}",
                                         expected.get(), value);
                            expected.set(0);
                            throttleLatch.countDown();
                            nonThrottleLatch.countDown();
                        }
                        if (expected.get() > X+1) {
                            throttleLatch.countDown();
                        }
                        if (expected.get() == (3 * X + 1)) {
                            nonThrottleLatch.countDown();
                        }
                        callback.operationFinished(context, null);
                    } catch (Exception e) {
                        logger.error("Received bad message", e);
                        throttleLatch.countDown();
                        nonThrottleLatch.countDown();
                    }
                }
            });
            assertTrue("Should Receive more messages than throttle value " + X,
                        throttleLatch.await(10, TimeUnit.SECONDS));

            assertTrue("Timed out waiting for messages " + (3*X + 1),
                       nonThrottleLatch.await(10, TimeUnit.SECONDS));
            assertEquals("Should be expected message with " + (3*X + 1),
                         3*X + 1, expected.get());

            subscriber.stopDelivery(topic, subid);
            closeSubscription(topic, subid);
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

        int port = PortManager.nextFreePort();
        int sslPort = PortManager.nextFreePort();

        // start bookkeeper 400
        BookKeeperCluster400 bkc400 = new BookKeeperCluster400(3);
        bkc400.start();

        // start 400 server
        Server400 s400 = new Server400(zkUtil.getZooKeeperConnectString(), port, sslPort);
        s400.start();

        org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions options5cur =
            org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions.newBuilder()
            .setCreateOrAttach(org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach.CREATE_OR_ATTACH)
            .setMessageBound(5).build();

        ClientCurrent ccur = new ClientCurrent("localhost:" + port + ":" + sslPort);
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
        Server410 s410 = new Server410(zkUtil.getZooKeeperConnectString(), port, sslPort);
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
        ServerCurrent scur = new ServerCurrent(zkUtil.getZooKeeperConnectString(), port, sslPort);
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

        int port = PortManager.nextFreePort();
        int sslPort = PortManager.nextFreePort();

        // start 410 server
        Server410 s410 = new Server410(zkUtil.getZooKeeperConnectString(), port, sslPort);
        s410.start();

        ClientCurrent ccur = new ClientCurrent("localhost:"+port+":"+sslPort);
        Client410 c410 = new Client410("localhost:"+port+":"+sslPort);

        // client c410 could publish message to 410 server
        assertNull(c410.publish(topic, data));
        // client ccur could publish message to 410 server
        // but no message seq id would be returned
        assertNull(ccur.publish(topic, data));

        // stop 410 server
        s410.stop();

        // start current server
        ServerCurrent scur = new ServerCurrent(zkUtil.getZooKeeperConnectString(), port, sslPort);
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

    /**
     * Test compatability between version 4.1.0 and the current version.
     *
     * A current server could read subscription data recorded by 4.1.0 server.
     */
    @Test
    public void testSubscriptionDataCompat410() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("TestCompat410");
        ByteString sub410 = ByteString.copyFromUtf8("sub410");
        ByteString subcur = ByteString.copyFromUtf8("subcur");

        // start bookkeeper 410
        BookKeeperCluster410 bkc410 = new BookKeeperCluster410(3);
        bkc410.start();

        int port = PortManager.nextFreePort();
        int sslPort = PortManager.nextFreePort();

        // start 410 server
        Server410 s410 = new Server410(zkUtil.getZooKeeperConnectString(), port, sslPort);
        s410.start();

        Client410 c410 = new Client410("localhost:"+port+":"+sslPort);
        c410.subscribe(topic, sub410);
        c410.closeSubscription(topic, sub410);

        ClientCurrent ccur = new ClientCurrent("localhost:"+port+":"+sslPort);
        ccur.subscribe(topic, subcur);
        ccur.closeSubscription(topic, subcur);

        // publish messages using old client
        c410.publishInts(topic, 0, 10);
        // stop 410 server
        s410.stop();

        // start current server
        ServerCurrent scur = new ServerCurrent(zkUtil.getZooKeeperConnectString(),
                                               port, sslPort);
        scur.start();

        c410.subscribe(topic, sub410);
        c410.receiveInts(topic, sub410, 0, 10);

        ccur.subscribe(topic, subcur);
        ccur.receiveInts(topic, subcur, 0, 10);

        // publish messages using current client
        ccur.publishInts(topic, 10, 10);

        c410.receiveInts(topic, sub410, 10, 10);
        ccur.receiveInts(topic, subcur, 10, 10);

        // stop current server
        scur.stop();

        c410.close();
        ccur.close();

        // stop bookkeeper cluster
        bkc410.stop();
    }

    /**
     * Test compatability between version 4.1.0 and the current version.
     *
     * A 4.1.0 client could not update message bound, while current could do it.
     */
    @Test
    public void testUpdateMessageBoundCompat410() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("TestUpdateMessageBoundCompat410");
        ByteString subid = ByteString.copyFromUtf8("mysub");

        // start bookkeeper
        BookKeeperClusterCurrent bkccur= new BookKeeperClusterCurrent(3);
        bkccur.start();

        int port = PortManager.nextFreePort();
        int sslPort = PortManager.nextFreePort();

        // start hub server
        ServerCurrent scur = new ServerCurrent(zkUtil.getZooKeeperConnectString(),
                                               port, sslPort);
        scur.start();

        org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions options5cur =
            org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions.newBuilder()
            .setCreateOrAttach(org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach.CREATE_OR_ATTACH)
            .setMessageBound(5).build();
        org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.SubscriptionOptions options5v410 =
            org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.SubscriptionOptions.newBuilder()
            .setCreateOrAttach(org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach.CREATE_OR_ATTACH)
            .setMessageBound(5).build();
        org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.SubscriptionOptions options20v410 =
            org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.SubscriptionOptions.newBuilder()
            .setCreateOrAttach(org.apache.hw_v4_1_0.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach.CREATE_OR_ATTACH)
            .setMessageBound(20).build();

        Client410 c410 = new Client410("localhost:"+port+":"+sslPort);
        c410.subscribe(topic, subid, options20v410);
        c410.closeSubscription(topic, subid);
        c410.sendXExpectLastY(topic, subid, 50, 20);

        c410.subscribe(topic, subid, options5v410);
        c410.closeSubscription(topic, subid);
        // the message bound isn't updated.
        c410.sendXExpectLastY(topic, subid, 50, 20);

        ClientCurrent ccur = new ClientCurrent("localhost:"+port+":"+sslPort);
        ccur.subscribe(topic, subid, options5cur);
        ccur.closeSubscription(topic, subid);
        // the message bound should be updated.
        c410.sendXExpectLastY(topic, subid, 50, 5);

        // stop current server
        scur.stop();

        c410.close();
        ccur.close();

        // stop bookkeeper cluster
        bkccur.stop();
    }

    /**
     * Test compatability between version 4.1.0 and the current version.
     *
     * A current client running message filter would fail on 4.1.0 hub servers.
     */
    @Test
    public void testClientMessageFilterCompat410() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("TestUpdateMessageBoundCompat410");
        ByteString subid = ByteString.copyFromUtf8("mysub");

        // start bookkeeper
        BookKeeperCluster410 bkc410 = new BookKeeperCluster410(3);
        bkc410.start();

        int port = PortManager.nextFreePort();
        int sslPort = PortManager.nextFreePort();

        // start hub server 410
        Server410 s410 = new Server410(zkUtil.getZooKeeperConnectString(), port, sslPort);
        s410.start();

        ClientCurrent ccur = new ClientCurrent("localhost:"+port+":"+sslPort);
        ccur.subscribe(topic, subid);
        ccur.closeSubscription(topic, subid);

        ccur.publishInts(topic, 0, 100);
        try {
            ccur.receiveNumModM(topic, subid, 0, 50, 2);
            fail("client-side filter could not run on 4.1.0 hub server");
        } catch (Exception e) {
            logger.info("Should fail to run client-side message filter on 4.1.0 hub server.", e);
            ccur.closeSubscription(topic, subid);
        }

        // stop 410 server
        s410.stop();
        // stop bookkeeper cluster
        bkc410.stop();
    }

    /**
     * Test compatability between version 4.1.0 and the current version.
     *
     * Server side throttling does't work when current client connects to old version
     * server.
     */
    @Test
    public void testServerSideThrottleCompat410() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("TestServerSideThrottleCompat410");
        ByteString subid = ByteString.copyFromUtf8("mysub");

        // start bookkeeper
        BookKeeperCluster410 bkc410 = new BookKeeperCluster410(3);
        bkc410.start();

        int port = PortManager.nextFreePort();
        int sslPort = PortManager.nextFreePort();

        // start hub server 410
        Server410 s410 = new Server410(zkUtil.getZooKeeperConnectString(), port, sslPort);
        s410.start();

        ClientCurrent ccur = new ClientCurrent(false, "localhost:"+port+":"+sslPort);
        ccur.throttleX41(topic, subid, 10);

        ccur.close();

        // stop 410 server
        s410.stop();
        // stop bookkeeper cluster
        bkc410.stop();
    }
}
