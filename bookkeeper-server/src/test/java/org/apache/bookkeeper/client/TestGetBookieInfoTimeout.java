package org.apache.bookkeeper.client;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import static org.junit.Assert.assertTrue;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperProtocol;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test tests timeout of GetBookieInfo request.
 *
 */
public class TestGetBookieInfoTimeout extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestGetBookieInfoTimeout.class);
    DigestType digestType;
    public EventLoopGroup eventLoopGroup;
    public OrderedSafeExecutor executor;
    private ScheduledExecutorService scheduler;

    public TestGetBookieInfoTimeout() {
        super(10);
        this.digestType = DigestType.CRC32;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        eventLoopGroup = new NioEventLoopGroup();

        executor = OrderedSafeExecutor.newBuilder()
                .name("BKClientOrderedSafeExecutor")
                .numThreads(2)
                .build();
        scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler"));
    }

    @After
    public void tearDown() throws Exception {
        scheduler.shutdown();
        eventLoopGroup.shutdownGracefully();
        executor.shutdown();
    }

    @Test
    public void testGetBookieInfoTimeout() throws Exception {

        // connect to the bookies and create a ledger
        LedgerHandle writelh = bkc.createLedger(3, 3, digestType, "testPasswd".getBytes());
        String tmp = "Foobar";
        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        // set timeout for getBookieInfo to be 2 secs and cause one of the bookies to go to sleep for 3X that time
        ClientConfiguration cConf = new ClientConfiguration();
        cConf.setGetBookieInfoTimeout(2);

        final BookieSocketAddress bookieToSleep = writelh.getLedgerMetadata().getEnsemble(0).get(0);
        int sleeptime = cConf.getBookieInfoTimeout() * 3;
        CountDownLatch latch = sleepBookie(bookieToSleep, sleeptime);
        latch.await();

        // try to get bookie info from the sleeping bookie. It should fail with timeout error
        BookieSocketAddress addr = new BookieSocketAddress(bookieToSleep.getSocketAddress().getHostString(),
                bookieToSleep.getPort());
        BookieClient bc = new BookieClient(cConf, eventLoopGroup, executor, scheduler, NullStatsLogger.INSTANCE);
        long flags = BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE
                | BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE;

        class CallbackObj {
            int rc;
            long requested;
            @SuppressWarnings("unused")
            long freeDiskSpace, totalDiskCapacity;
            CountDownLatch latch = new CountDownLatch(1);
            CallbackObj(long requested) {
                this.requested = requested;
                this.rc = 0;
                this.freeDiskSpace = 0L;
                this.totalDiskCapacity = 0L;
            }
        }
        CallbackObj obj = new CallbackObj(flags);
        bc.getBookieInfo(addr, flags, (rc, bInfo, ctx) -> {
            CallbackObj obj1 = (CallbackObj) ctx;
            obj1.rc = rc;
            if (rc == Code.OK) {
                if ((obj1.requested & BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE) != 0) {
                    obj1.freeDiskSpace = bInfo.getFreeDiskSpace();
                }
                if ((obj1.requested & BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE)
                        != 0) {
                    obj1.totalDiskCapacity = bInfo.getTotalDiskSpace();
                }
            }
            obj1.latch.countDown();
        }, obj);
        obj.latch.await();
        LOG.debug("Return code: " + obj.rc);
        assertTrue("GetBookieInfo failed with unexpected error code: " + obj.rc, obj.rc == Code.TimeoutException);
    }
}
