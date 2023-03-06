/*
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
 */
package org.apache.bookkeeper.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import java.net.SocketAddress;
import org.junit.Test;

/**
 * Unit tests for {@link ByteBufList}.
 */
public class ByteBufListTest {
    @Test
    public void testSingle() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBufList buf = ByteBufList.get(b1);

        assertEquals(1, buf.size());
        assertEquals(128, buf.readableBytes());
        assertEquals(b1, buf.getBuffer(0));

        assertEquals(buf.refCnt(), 1);
        assertEquals(b1.refCnt(), 1);

        buf.release();

        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
    }

    @Test
    public void testDouble() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b2.capacity());
        ByteBufList buf = ByteBufList.get(b1, b2);

        assertEquals(2, buf.size());
        assertEquals(256, buf.readableBytes());
        assertEquals(b1, buf.getBuffer(0));
        assertEquals(b2, buf.getBuffer(1));

        assertEquals(buf.refCnt(), 1);
        assertEquals(b1.refCnt(), 1);
        assertEquals(b2.refCnt(), 1);

        buf.release();

        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
    }

    @Test
    public void testComposite() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b2.capacity());

        CompositeByteBuf composite = PooledByteBufAllocator.DEFAULT.compositeBuffer();
        composite.addComponent(b1);
        composite.addComponent(b2);

        ByteBufList buf = ByteBufList.get(composite);

        // composite is unwrapped into two parts
        assertEquals(2, buf.size());
        // and released
        assertEquals(composite.refCnt(), 0);

        assertEquals(256, buf.readableBytes());
        assertEquals(b1, buf.getBuffer(0));
        assertEquals(b2, buf.getBuffer(1));

        assertEquals(buf.refCnt(), 1);
        assertEquals(b1.refCnt(), 1);
        assertEquals(b2.refCnt(), 1);

        buf.release();

        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
    }

    @Test
    public void testClone() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b2.capacity());
        ByteBufList buf = ByteBufList.get(b1, b2);

        ByteBufList clone = ByteBufList.clone(buf);

        assertEquals(2, buf.size());
        assertEquals(256, buf.readableBytes());
        assertEquals(b1, buf.getBuffer(0));
        assertEquals(b2, buf.getBuffer(1));

        assertEquals(2, clone.size());
        assertEquals(256, clone.readableBytes());
        assertEquals(b1, clone.getBuffer(0));
        assertEquals(b2, clone.getBuffer(1));

        assertEquals(buf.refCnt(), 1);
        assertEquals(clone.refCnt(), 1);
        assertEquals(b1.refCnt(), 2);
        assertEquals(b2.refCnt(), 2);

        buf.release();

        assertEquals(buf.refCnt(), 0);
        assertEquals(clone.refCnt(), 1);
        assertEquals(b1.refCnt(), 1);
        assertEquals(b2.refCnt(), 1);

        clone.release();

        assertEquals(buf.refCnt(), 0);
        assertEquals(clone.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
    }

    @Test
    public void testGetBytes() throws Exception {
        ByteBufList buf = ByteBufList.get(Unpooled.wrappedBuffer("hello".getBytes()),
                Unpooled.wrappedBuffer("world".getBytes()));

        assertArrayEquals("helloworld".getBytes(), buf.toArray());

        buf.prepend(Unpooled.wrappedBuffer("prefix-".getBytes()));
        assertArrayEquals("prefix-helloworld".getBytes(), buf.toArray());

        // Bigger buffer
        byte[] buf100 = new byte[100];
        int res = buf.getBytes(buf100);

        assertEquals("prefix-helloworld".length(), res);

        // Smaller buffer
        byte[] buf4 = new byte[4];
        res = buf.getBytes(buf4);

        assertEquals(4, res);
        assertEquals("pref", new String(buf4));
    }

    @Test
    public void testCoalesce() throws Exception {
        ByteBufList buf = ByteBufList.get(Unpooled.wrappedBuffer("hello".getBytes()),
                Unpooled.wrappedBuffer("world".getBytes()));

        assertEquals(Unpooled.wrappedBuffer("helloworld".getBytes()), ByteBufList.coalesce(buf));
    }

    @Test
    public void testRetain() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBufList buf = ByteBufList.get(b1);

        assertEquals(1, buf.size());
        assertEquals(128, buf.readableBytes());
        assertEquals(b1, buf.getBuffer(0));

        assertEquals(buf.refCnt(), 1);
        assertEquals(b1.refCnt(), 1);

        buf.retain();

        assertEquals(buf.refCnt(), 2);
        assertEquals(b1.refCnt(), 1);

        buf.release();

        assertEquals(buf.refCnt(), 1);
        assertEquals(b1.refCnt(), 1);

        buf.release();

        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
    }

    @Test
    public void testEncoder() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b2.capacity());
        ByteBufList buf = ByteBufList.get(b1, b2);

        ChannelHandlerContext ctx = new MockChannelHandlerContext();

        ByteBufList.ENCODER.write(ctx, buf, null);

        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
    }

    class MockChannelHandlerContext implements ChannelHandlerContext {
        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture disconnect() {
            return null;
        }

        @Override
        public ChannelFuture close() {
            return null;
        }

        @Override
        public ChannelFuture deregister() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture close(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture deregister(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture write(Object msg) {
            ReferenceCountUtil.release(msg);
            return null;
        }

        @Override
        public ChannelFuture write(Object msg, ChannelPromise promise) {
            ReferenceCountUtil.release(msg);
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            ReferenceCountUtil.release(msg);
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            ReferenceCountUtil.release(msg);
            return null;
        }

        @Override
        public ChannelPromise newPromise() {
            return null;
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise() {
            return null;
        }

        @Override
        public ChannelFuture newSucceededFuture() {
            return null;
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable cause) {
            return null;
        }

        @Override
        public ChannelPromise voidPromise() {
            return null;
        }

        @Override
        public Channel channel() {
            return null;
        }

        @Override
        public EventExecutor executor() {
            return null;
        }

        @Override
        public String name() {
            return null;
        }

        @Override
        public ChannelHandler handler() {
            return null;
        }

        @Override
        public boolean isRemoved() {
            return false;
        }

        @Override
        public ChannelHandlerContext fireChannelRegistered() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelUnregistered() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelActive() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelInactive() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireUserEventTriggered(Object evt) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelRead(Object msg) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelReadComplete() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelWritabilityChanged() {
            return null;
        }

        @Override
        public ChannelHandlerContext read() {
            return null;
        }

        @Override
        public ChannelHandlerContext flush() {
            return null;
        }

        @Override
        public ChannelPipeline pipeline() {
            return null;
        }

        @Override
        public ByteBufAllocator alloc() {
            return null;
        }

        @Override
        @Deprecated
        public <T> Attribute<T> attr(AttributeKey<T> key) {
            return null;
        }

        @Override
        @Deprecated
        public <T> boolean hasAttr(AttributeKey<T> key) {
            return false;
        }

    }
}
