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

package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.jboss.netty.buffer.ChannelBuffer;

import org.apache.bookkeeper.client.BKException;

import org.apache.bookkeeper.test.BaseTestCase;
import org.apache.bookkeeper.test.BookieClientTest;
import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.net.InetSocketAddress;
import java.net.InetAddress;

public class TestProtoVersions {
    private BookieClientTest base;

    @Before
    public void setup() throws Exception {
        base = new BookieClientTest();
        base.setUp();
    }

    @After
    public void teardown() throws Exception {
        base.tearDown();
    }

    private void testVersion(int version, int expectedresult) throws Exception {
        PerChannelBookieClient bc = new PerChannelBookieClient(base.executor, base.channelFactory, 
                new InetSocketAddress(InetAddress.getLocalHost(), base.port), new AtomicLong(0));
        final AtomicInteger outerrc = new AtomicInteger(-1);
        final CountDownLatch connectLatch = new CountDownLatch(1);
        bc.connectIfNeededAndDoOp(new GenericCallback<Void>() {
                public void operationComplete(int rc, Void result) {
                    outerrc.set(rc);
                    connectLatch.countDown();
                }
            });
        connectLatch.await(5, TimeUnit.SECONDS);
        
        assertEquals("client not connected", BKException.Code.OK, outerrc.get());
        outerrc.set(-1000);
        final CountDownLatch readLatch = new CountDownLatch(1);
        ReadEntryCallback cb = new ReadEntryCallback() {
                public void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer buffer, Object ctx) {
                    outerrc.set(rc);
                    readLatch.countDown();
                }
            };
        bc.readCompletions.put(bc.newCompletionKey(1, 1),
                               new PerChannelBookieClient.ReadCompletion(cb, this));
        
        int totalHeaderSize = 4 // for the length of the packet
            + 4 // for request type
            + 8 // for ledgerId
            + 8; // for entryId

        // This will need to updated if the protocol for read changes
        ChannelBuffer tmpEntry = bc.channel.getConfig().getBufferFactory().getBuffer(totalHeaderSize);
        tmpEntry.writeInt(totalHeaderSize - 4);
        tmpEntry.writeInt(new BookieProtocol.PacketHeader((byte)version, BookieProtocol.READENTRY, (short)0).toInt());
        tmpEntry.writeLong(1);
        tmpEntry.writeLong(1);
        
        
        bc.channel.write(tmpEntry).awaitUninterruptibly();
        readLatch.await(5, TimeUnit.SECONDS);
        assertEquals("Expected result differs", expectedresult, outerrc.get());
        
        bc.close();
    }

    @Test(timeout=60000)
    public void testVersions() throws Exception {
        testVersion(BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION-1, BKException.Code.ProtocolVersionException);
        testVersion(BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION, BKException.Code.NoSuchEntryException);
        testVersion(BookieProtocol.CURRENT_PROTOCOL_VERSION, BKException.Code.NoSuchEntryException);
        testVersion(BookieProtocol.CURRENT_PROTOCOL_VERSION+1, BKException.Code.ProtocolVersionException);
    }
}