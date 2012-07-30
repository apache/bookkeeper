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
package org.apache.hedwig.server.persistence;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.hedwig.HelperMethods;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.TrivialOwnAllTopicManager;
import org.apache.hedwig.util.Callback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class TestBookKeeperPersistenceManager extends TestCase {
    static Logger logger = LoggerFactory.getLogger(TestPersistenceManagerBlackBox.class);

    BookKeeperTestBase bktb;
    private final int numBookies = 3;
    private final long readDelay = 2000L;

    ServerConfiguration conf;
    ScheduledExecutorService scheduler;

    TopicManager tm;
    BookkeeperPersistenceManager manager;
    PubSubException failureException = null;
    MetadataManagerFactory metadataManagerFactory;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        // delay read response for 2s.
        bktb = new BookKeeperTestBase(numBookies, readDelay);
        bktb.setUp();

        conf = new ServerConfiguration();
        org.apache.bookkeeper.conf.ClientConfiguration bkClientConf =
                new org.apache.bookkeeper.conf.ClientConfiguration();
        bkClientConf.setNumWorkerThreads(1).setReadTimeout(9999)
        .setThrottleValue(3);
        conf.addConf(bkClientConf);

        metadataManagerFactory =
            MetadataManagerFactory.newMetadataManagerFactory(conf, bktb.getZooKeeperClient());

        scheduler = Executors.newScheduledThreadPool(1);
        tm = new TrivialOwnAllTopicManager(conf, scheduler);
        manager = new BookkeeperPersistenceManager(bktb.bk, metadataManagerFactory,
                                                   tm, conf, scheduler);
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        tm.stop();
        manager.stop();
        metadataManagerFactory.shutdown();
        scheduler.shutdown();
        bktb.tearDown();
        super.tearDown();
    }

    class RangeScanVerifier implements ScanCallback {
        LinkedList<Message> pubMsgs;
        boolean runNextScan = false;
        RangeScanRequest nextScan = null;

        public RangeScanVerifier(LinkedList<Message> pubMsgs, RangeScanRequest nextScan) {
            this.pubMsgs = pubMsgs;
            this.nextScan = nextScan;
        }

        @Override
        public void messageScanned(Object ctx, Message recvMessage) {
            if (null != nextScan && !runNextScan) {
                runNextScan = true;
                manager.scanMessages(nextScan);
            }

            if (pubMsgs.size() == 0) {
                return;
            }

            Message pubMsg = pubMsgs.removeFirst();
            if (!HelperMethods.areEqual(recvMessage, pubMsg)) {
                fail("Scanned message not equal to expected");
            }
        }

        @Override
        public void scanFailed(Object ctx, Exception exception) {
            fail("Failed to scan messages.");
        }

        @Override
        @SuppressWarnings("unchecked")
        public void scanFinished(Object ctx, ReasonForFinish reason) {
            LinkedBlockingQueue<Boolean> statusQueue = (LinkedBlockingQueue<Boolean>) ctx;
            try {
                statusQueue.put(true);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private LinkedList<Message> subMessages(List<Message> msgs, int start, int end) {
        LinkedList<Message> result = new LinkedList<Message>();
        for (int i=start; i<=end; i++) {
            result.add(msgs.get(i));
        }
        return result;
    }

    @Test
    public void testScanMessagesOnTwoLedgers() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("TestScanMessagesOnTwoLedgers");

        List<Message> msgs = new ArrayList<Message>();

        acquireTopic(topic);
        msgs.addAll(publishMessages(topic, 1));
        releaseTopic(topic);

        // acquire topic again to force a new ledger
        acquireTopic(topic);
        msgs.addAll(publishMessages(topic, 3));

        // scan messages
        LinkedBlockingQueue<Boolean> statusQueue = new LinkedBlockingQueue<Boolean>();
        RangeScanRequest nextScan = new RangeScanRequest(topic, 3, 2, Long.MAX_VALUE,
                new RangeScanVerifier(subMessages(msgs, 2, 3), null), statusQueue);
        manager.scanMessages(new RangeScanRequest(topic, 1, 2, Long.MAX_VALUE,
                new RangeScanVerifier(subMessages(msgs, 0, 1), nextScan), statusQueue));
        Boolean b = statusQueue.poll(10 * readDelay, TimeUnit.MILLISECONDS);
        if (b == null) {
            fail("One scan request doesn't finish");
        }
        b = statusQueue.poll(10 * readDelay, TimeUnit.MILLISECONDS);
        if (b == null) {
            fail("One scan request doesn't finish");
        }
    }

    class TestCallback implements Callback<PubSubProtocol.MessageSeqId> {

        @Override
        @SuppressWarnings("unchecked")
        public void operationFailed(Object ctx, PubSubException exception) {
            LinkedBlockingQueue<Boolean> statusQueue = (LinkedBlockingQueue<Boolean>) ctx;
            try {
                statusQueue.put(false);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void operationFinished(Object ctx, PubSubProtocol.MessageSeqId resultOfOperation) {
            LinkedBlockingQueue<Boolean> statusQueue = (LinkedBlockingQueue<Boolean>) ctx;
            try {
                statusQueue.put(true);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected List<Message> publishMessages(ByteString topic, int numMsgs) throws Exception {
        List<Message> msgs = HelperMethods.getRandomPublishedMessages(numMsgs, 1024);
        LinkedBlockingQueue<Boolean> statusQueue = new LinkedBlockingQueue<Boolean>();
        for (Message msg : msgs) {

            try {
                manager.persistMessage(new PersistRequest(topic, msg, new TestCallback(), statusQueue));
                // wait a maximum of a minute
                Boolean b = statusQueue.poll(60, TimeUnit.SECONDS);
                if (b == null) {
                    throw new RuntimeException("Publish timed out");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return msgs;
    }

    protected void acquireTopic(ByteString topic) throws Exception {
        Semaphore latch = new Semaphore(1);
        latch.acquire();
        manager.acquiredTopic(topic, new Callback<Void>() {
            @Override
            public void operationFinished(Object ctx, Void resultOfOperation) {
                failureException = null;
                ((Semaphore)ctx).release();
            }
            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                failureException = exception;
                ((Semaphore)ctx).release();
            }
        }, latch);
        latch.acquire();
        latch.release();
        if (null != failureException) {
            throw failureException;
        }
    }

    protected void releaseTopic(ByteString topic) throws Exception {
        manager.lostTopic(topic);
    }

}
