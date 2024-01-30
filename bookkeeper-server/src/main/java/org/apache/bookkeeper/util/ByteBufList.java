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
package org.apache.bookkeeper.util;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.util.ArrayList;

/**
 * ByteBufList is a holder of a sequence of {@link ByteBuf} objects.
 *
 * <p>This class doesn't trying to mimic the {@link ByteBuf}, but rather exposes itself just like a regular object which
 * will need to be encoded on the channel. There are 2 utility encoders:
 * <ul>
 * <li>{@link #ENCODER}: regular encode that will write all the buffers in the {@link ByteBufList} on the channel</li>
 * </ul>
 *
 * <p>Example:
 *
 * <pre>
 * bootstrap.handler(new ChannelInitializer&lt;SocketChannel&gt;() {
 *     public void initChannel(SocketChannel ch) throws Exception {
 *         ChannelPipeline pipeline = ch.pipeline();
 *         pipeline.addLast("bytebufList", ByteBufList.ENCODER);
 *         pipeline.addLast("mainhandler", MyHandler.class);
 *     }
 * });
 * </pre>
 *
 * <p>ByteBufList is pooling the instances and uses ref-counting to release them.
 */
public class ByteBufList extends AbstractReferenceCounted {
    private final ArrayList<ByteBuf> buffers;
    private final Handle<ByteBufList> recyclerHandle;

    private static final int INITIAL_LIST_SIZE = 4;

    private static final Recycler<ByteBufList> RECYCLER = new Recycler<ByteBufList>() {
        @Override
        protected ByteBufList newObject(Recycler.Handle<ByteBufList> handle) {
            return new ByteBufList(handle);
        }
    };

    private ByteBufList(Handle<ByteBufList> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
        this.buffers = new ArrayList<>(INITIAL_LIST_SIZE);
    }

    /**
     * Get a new {@link ByteBufList} from the pool and assign 2 buffers to it.
     *
     * <p>The buffers b1 and b2 lifecycles are now managed by the ByteBufList: when the {@link ByteBufList} is
     * deallocated, b1 and b2 will be released as well.
     *
     * @param b1
     *            first buffer
     * @param b2
     *            second buffer
     * @return a {@link ByteBufList} instance from the pool
     */
    public static ByteBufList get(ByteBuf b1, ByteBuf b2) {
        ByteBufList buf = get();
        buf.add(b1);
        buf.add(b2);
        return buf;
    }

    /**
     * Get a new {@link ByteBufList} from the pool and assign 1 buffer to it.
     *
     * <p>The buffer b1 lifecycle is now managed by the ByteBufList: when the {@link ByteBufList} is
     * deallocated, b1 will be released as well.
     *
     * @param b1
     *            first buffer
     * @return a {@link ByteBufList} instance from the pool
     */
    public static ByteBufList get(ByteBuf b1) {
        ByteBufList buf = get();
        buf.add(b1);
        return buf;
    }

    /**
     * Get a new {@link ByteBufList} instance from the pool that is the clone of an already existing instance.
     */
    public static ByteBufList clone(ByteBufList other) {
        ByteBufList buf = get();
        for (int i = 0; i < other.buffers.size(); i++) {
            // Create a duplicate of the buffer so that there is no interference from other threads
            buf.add(other.buffers.get(i).retainedDuplicate());
        }
        return buf;
    }

    public static ByteBufList get() {
        ByteBufList buf = RECYCLER.get();
        buf.setRefCnt(1);
        return buf;
    }

    /**
     * Append a {@link ByteBuf} at the end of this {@link ByteBufList}.
     */
    public void add(ByteBuf buf) {
        final ByteBuf unwrapped = buf.unwrap() != null && buf.unwrap() instanceof CompositeByteBuf
                ? buf.unwrap() : buf;
        ReferenceCountUtil.retain(unwrapped);
        ReferenceCountUtil.release(buf);

        if (unwrapped instanceof CompositeByteBuf) {
            ((CompositeByteBuf) unwrapped).forEach(b -> {
                ReferenceCountUtil.retain(b);
                buffers.add(b);
            });
            ReferenceCountUtil.release(unwrapped);
        } else {
            buffers.add(unwrapped);
        }
    }

    /**
     * Prepend a {@link ByteBuf} at the beginning of this {@link ByteBufList}.
     */
    public void prepend(ByteBuf buf) {
        // don't unwrap slices
        final ByteBuf unwrapped = buf.unwrap() != null && buf.unwrap() instanceof CompositeByteBuf
                ? buf.unwrap() : buf;
        ReferenceCountUtil.retain(unwrapped);
        ReferenceCountUtil.release(buf);

        if (unwrapped instanceof CompositeByteBuf) {
            CompositeByteBuf composite = (CompositeByteBuf) unwrapped;
            for (int i = composite.numComponents() - 1; i >= 0; i--) {
                ByteBuf b = composite.component(i);
                ReferenceCountUtil.retain(b);
                buffers.add(0, b);
            }
            ReferenceCountUtil.release(unwrapped);
        } else {
            buffers.add(0, unwrapped);
        }
    }

    /**
     * @return the total amount of readable bytes across all the {@link ByteBuf} included in the list
     */
    public int readableBytes() {
        int readableBytes = 0;
        for (int i = 0; i < buffers.size(); i++) {
            readableBytes += buffers.get(i).readableBytes();
        }
        return readableBytes;
    }

    /**
     * Get access to a particular buffer in the list.
     *
     * @param index
     *            the index of the buffer
     * @return the buffer
     */
    public ByteBuf getBuffer(int index) {
        return buffers.get(index);
    }

    /**
     * @return the number of buffers included in the {@link ByteBufList}
     */
    public int size() {
        return buffers.size();
    }

    /**
     * Write bytes from the current {@link ByteBufList} into a byte array.
     *
     * <p>This won't modify the reader index of the internal buffers.
     *
     * @param dst
     *            the destination byte array
     * @return the number of copied bytes
     */
    public int getBytes(byte[] dst) {
        int copied = 0;
        for (int idx = 0; idx < buffers.size() && copied < dst.length; idx++) {
            ByteBuf b = buffers.get(idx);
            int len = Math.min(b.readableBytes(), dst.length - copied);
            b.getBytes(b.readerIndex(), dst, copied, len);

            copied += len;
        }

        return copied;
    }

    /**
     * Creates a copy of the readable content of the internal buffers and returns the copy.
     * @return an array containing all the internal buffers content
     */
    public byte[] toArray() {
        byte[] a = new byte[readableBytes()];
        getBytes(a);
        return a;
    }

    /**
     * Returns {@code true} if this buffer has a single backing byte array.
     * If this method returns true, you can safely call {@link #array()} and
     * {@link #arrayOffset()}.
     * @return true, if this {@link ByteBufList} is backed by a single array
     */
    public boolean hasArray() {
        return buffers.size() == 1 && buffers.get(0).hasArray();
    }

    /**
     * Returns a reference to the array backing this {@link ByteBufList}.
     * This method must only be called if {@link #hasArray()} returns {@code true}.
     * @return the array backing this {@link ByteBufList}
     */
    public byte[] array() {
        return buffers.get(0).array();
    }

    /**
     * Returns the offset of the first byte within the backing byte array of
     * this buffer.
     * This method must only be called if {@link #hasArray()} returns {@code true}.
     * @return the offset of the first byte within the backing byte array.
     */
    public int arrayOffset() {
        return buffers.get(0).arrayOffset();
    }

    /**
     * @return a single buffer with the content of both individual buffers
     */
    @VisibleForTesting
    public static ByteBuf coalesce(ByteBufList list) {
        ByteBuf res = Unpooled.buffer(list.readableBytes());
        for (int i = 0; i < list.buffers.size(); i++) {
            ByteBuf b = list.buffers.get(i);
            res.writeBytes(b, b.readerIndex(), b.readableBytes());
        }

        return res;
    }

    @Override
    public ByteBufList retain() {
        super.retain();
        return this;
    }

    @Override
    protected void deallocate() {
        for (int i = 0; i < buffers.size(); i++) {
            ReferenceCountUtil.release(buffers.get(i));
        }

        buffers.clear();
        recyclerHandle.recycle(this);
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        for (int i = 0; i < buffers.size(); i++) {
            buffers.get(i).touch(hint);
        }
        return this;
    }

    /**
     * Encoder for the {@link ByteBufList} that doesn't prepend any size header.
     */
    public static final Encoder ENCODER = new Encoder();

    /**
     * {@link ByteBufList} encoder.
     */
    @Sharable
    public static class Encoder extends ChannelOutboundHandlerAdapter {

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof ByteBufList) {
                ByteBufList b = (ByteBufList) msg;

                try {
                    // Write each buffer individually on the socket. The retain() here is needed to preserve the fact
                    // that ByteBuf are automatically released after a write. If the ByteBufPair ref count is increased
                    // and it gets written multiple times, the individual buffers refcount should be reflected as well.
                    int buffersCount = b.buffers.size();
                    for (int i = 0; i < buffersCount; i++) {
                        ByteBuf bx = b.buffers.get(i);
                        // Last buffer will carry on the final promise to notify when everything was written on the
                        // socket
                        ctx.write(bx.retainedDuplicate(), i == (buffersCount - 1) ? promise : ctx.voidPromise());
                    }
                } finally {
                    ReferenceCountUtil.release(b);
                }
            } else {
                ctx.write(msg, promise);
            }
        }
    }

}
