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
package org.apache.bookkeeper.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the bookie client.
 */
public class BookieClientTest {
    BookieServer bs;
    File tmpDir;
    public int port = 13645;

    public EventLoopGroup eventLoopGroup;
    public OrderedExecutor executor;
    private ScheduledExecutorService scheduler;

    @Before
    public void setUp() throws Exception {
        tmpDir = IOUtils.createTempDir("bookieClient", "test");
        // Since this test does not rely on the BookKeeper client needing to
        // know via ZooKeeper which Bookies are available, okay, so pass in null
        // for the zkServers input parameter when constructing the BookieServer.
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setBookiePort(port)
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() })
            .setMetadataServiceUri(null);
        bs = new BookieServer(conf);
        bs.start();
        eventLoopGroup = new NioEventLoopGroup();
        executor = OrderedExecutor.newBuilder()
                .name("BKClientOrderedSafeExecutor")
                .numThreads(2)
                .build();
        scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler"));
    }

    @After
    public void tearDown() throws Exception {
        scheduler.shutdown();
        bs.shutdown();
        recursiveDelete(tmpDir);
        eventLoopGroup.shutdownGracefully();
        executor.shutdown();
    }

    private static void recursiveDelete(File dir) {
        File children[] = dir.listFiles();
        if (children != null) {
            for (File child : children) {
                recursiveDelete(child);
            }
        }
        dir.delete();
    }

    static class ResultStruct {
        int rc = -123456;
        ByteBuffer entry;
    }

    ReadEntryCallback recb = new ReadEntryCallback() {

        public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf bb, Object ctx) {
            ResultStruct rs = (ResultStruct) ctx;
            synchronized (rs) {
                rs.rc = rc;
                if (BKException.Code.OK == rc && bb != null) {
                    bb.readerIndex(24);
                    rs.entry = bb.nioBuffer();
                }
                rs.notifyAll();
            }
        }

    };

    WriteCallback wrcb = new WriteCallback() {
        public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
            if (ctx != null) {
                synchronized (ctx) {
                    if (ctx instanceof ResultStruct) {
                        ResultStruct rs = (ResultStruct) ctx;
                        rs.rc = rc;
                    }
                    ctx.notifyAll();
                }
            }
        }
    };

    @Test
    public void testWriteGaps() throws Exception {
        final Object notifyObject = new Object();
        byte[] passwd = new byte[20];
        Arrays.fill(passwd, (byte) 'a');
        BookieSocketAddress addr = bs.getLocalAddress();
        ResultStruct arc = new ResultStruct();

        BookieClient bc = new BookieClient(new ClientConfiguration(), eventLoopGroup, executor,
                                           scheduler, NullStatsLogger.INSTANCE);
        ByteBufList bb = createByteBuffer(1, 1, 1);
        bc.addEntry(addr, 1, passwd, 1, bb, wrcb, arc, BookieProtocol.FLAG_NONE);
        synchronized (arc) {
            arc.wait(1000);
            assertEquals(0, arc.rc);
            bc.readEntry(addr, 1, 1, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(1, arc.entry.getInt());
        }
        bb = createByteBuffer(2, 1, 2);
        bc.addEntry(addr, 1, passwd, 2, bb, wrcb, null, BookieProtocol.FLAG_NONE);
        bb = createByteBuffer(3, 1, 3);
        bc.addEntry(addr, 1, passwd, 3, bb, wrcb, null, BookieProtocol.FLAG_NONE);
        bb = createByteBuffer(5, 1, 5);
        bc.addEntry(addr, 1, passwd, 5, bb, wrcb, null, BookieProtocol.FLAG_NONE);
        bb = createByteBuffer(7, 1, 7);
        bc.addEntry(addr, 1, passwd, 7, bb, wrcb, null, BookieProtocol.FLAG_NONE);
        synchronized (notifyObject) {
            bb = createByteBuffer(11, 1, 11);
            bc.addEntry(addr, 1, passwd, 11, bb, wrcb, notifyObject, BookieProtocol.FLAG_NONE);
            notifyObject.wait();
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 6, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 7, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(7, arc.entry.getInt(), BookieProtocol.FLAG_NONE);
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 1, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(1, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 2, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(2, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 3, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(3, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 4, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 11, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(11, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 5, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(5, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 10, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 12, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 13, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
    }

    private ByteBufList createByteBuffer(int i, long lid, long eid) {
        ByteBuf bb = Unpooled.buffer(4 + 24);
        bb.writeLong(lid);
        bb.writeLong(eid);
        bb.writeLong(eid - 1);
        bb.writeInt(i);
        return ByteBufList.get(bb);
    }

    @Test
    public void testNoLedger() throws Exception {
        ResultStruct arc = new ResultStruct();
        BookieSocketAddress addr = bs.getLocalAddress();
        BookieClient bc = new BookieClient(new ClientConfiguration(), eventLoopGroup, executor,
                                           scheduler, NullStatsLogger.INSTANCE);
        synchronized (arc) {
            bc.readEntry(addr, 2, 13, recb, arc, BookieProtocol.FLAG_NONE);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchLedgerExistsException, arc.rc);
        }
    }

    @Test
    public void testGetBookieInfo() throws IOException, InterruptedException {
        BookieSocketAddress addr = bs.getLocalAddress();
        BookieClient bc = new BookieClient(new ClientConfiguration(), new NioEventLoopGroup(), executor,
                                           scheduler, NullStatsLogger.INSTANCE);
        long flags = BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE
                | BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE;

        class CallbackObj {
            int rc;
            long requested;
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
        bc.getBookieInfo(addr, flags, new GetBookieInfoCallback() {
            @Override
            public void getBookieInfoComplete(int rc, BookieInfo bInfo, Object ctx) {
                CallbackObj obj = (CallbackObj) ctx;
                obj.rc = rc;
                if (rc == Code.OK) {
                    if ((obj.requested & BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE) != 0) {
                        obj.freeDiskSpace = bInfo.getFreeDiskSpace();
                    }
                    if ((obj.requested
                                & BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE) != 0) {
                        obj.totalDiskCapacity = bInfo.getTotalDiskSpace();
                    }
                }
                obj.latch.countDown();
            }

        }, obj);
        obj.latch.await();
        System.out.println("Return code: " + obj.rc + "FreeDiskSpace: " + obj.freeDiskSpace + " TotalCapacity: "
                + obj.totalDiskCapacity);
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.rc == Code.OK);
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.freeDiskSpace <= obj.totalDiskCapacity);
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.totalDiskCapacity > 0);
    }
}
