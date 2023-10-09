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
package org.apache.bookkeeper.bookie.storage.directentrylogger;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.common.util.ExceptionMessageHelper.exMsg;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;

/**
 * A utility buffer class to be used with native calls.
 * <p/>
 * Buffers are page aligned (4k pages).
 * <p/>
 * The wrapper mostly handles writes between ByteBuffers and
 * ByteBufs. It also provides a method for padding the buffer to the next
 * alignment, so writes can have an aligned size also (as required by
 * direct I/O). The padding is done with 0xF0, so that if it is read as
 * an integer, or long, the value will be negative (assuming the read is
 * a java read, and thus a signed int).
 */
class Buffer {
    /* Padding byte must have MSB set, so if read at the start
     * of an integer or long, the returned value is negative. */
    public static final byte PADDING_BYTE = (byte) 0xF0;

    /* Some machines can live with 512 alignment, but others
     * appear to require 4096, so go with 4096, which is page
     * alignment */
    public static final int ALIGNMENT = 4096;
    private static final int MAX_ALIGNMENT = Integer.MAX_VALUE & ~(ALIGNMENT - 1);
    static final byte[] PADDING = generatePadding();

    final NativeIO nativeIO;
    final int bufferSize;
    ByteBuf buffer;
    ByteBuffer byteBuffer;
    ByteBufAllocator allocator;
    long pointer = 0;

    Buffer(NativeIO nativeIO, ByteBufAllocator allocator, int bufferSize) throws IOException {
        checkArgument(isAligned(bufferSize),
                      "Buffer size not aligned %d", bufferSize);

        this.allocator = allocator;
        this.buffer = allocateAligned(ALIGNMENT, bufferSize);
        this.nativeIO = nativeIO;
        this.bufferSize = bufferSize;
        byteBuffer = buffer.nioBuffer(0, bufferSize);
        byteBuffer.order(ByteOrder.BIG_ENDIAN);
    }

    private ByteBuf allocateAligned(int alignment, int bufferSize) {
        ByteBuf buf = allocator.directBuffer(bufferSize + alignment);
        long addr = buf.memoryAddress();
        if ((addr & (alignment - 1)) == 0) {
            // The address is already aligned
            pointer = addr;
            return buf.slice(0, bufferSize);
        } else {
            int alignOffset = (int) (alignment - (addr & (alignment - 1)));
            pointer = addr + alignOffset;
            return buf.slice(alignOffset, bufferSize);
        }
    }

    /**
     * @return whether there is space in the buffer for size bytes.
     */
    boolean hasSpace(int size) throws IOException {
        if (size > bufferSize) {
            throw new IOException(exMsg("Write too large").kv("writeSize", size)
                                  .kv("maxSize", bufferSize).toString());
        }
        return byteBuffer.remaining() >= size;
    }

    /**
     * @return whether the buffer can honour a read of size at offset.
     */
    boolean hasData(int offset, int size) {
        return offset + size <= bufferSize;
    }

    /**
     * Write an integer to buffer. Progresses the position of the buffer by 4 bytes.
     */
    void writeInt(int value) throws IOException {
        byteBuffer.putInt(value);
    }

    /**
     * Write a btebuf to this buffer. Progresses the position of the buffer by the
     * number of readable bytes of the bytebuf. Progresses the readerIndex of the passed
     * bytebuf by the number of bytes read (i.e. to the end).
     */
    void writeByteBuf(ByteBuf bytebuf) throws IOException {
        int bytesWritten = bytebuf.readableBytes();
        ByteBuffer bytesToPut = bytebuf.nioBuffer();
        byteBuffer.put(bytesToPut);
        bytebuf.skipBytes(bytesWritten);
    }

    /**
     * Read an integer from the buffer at the given offset. The offset is in bytes.
     */
    int readInt(int offset) throws IOException {
        if (!hasData(offset, Integer.BYTES)) {
            throw new IOException(exMsg("Buffer cannot satify int read")
                                  .kv("offset", offset)
                                  .kv("bufferSize", bufferSize).toString());
        }
        try {
            return byteBuffer.getInt(offset);
        } catch (Exception e) {
            throw new IOException(exMsg("Error reading int")
                                  .kv("byteBuffer", byteBuffer.toString())
                                  .kv("offset", offset)
                                  .kv("bufferSize", bufferSize).toString(), e);
        }
    }

    /**
     * Read a long from the buffer at the given offset. The offset is in bytes.
     */
    long readLong(int offset) throws IOException {
        if (!hasData(offset, Long.BYTES)) {
            throw new IOException(exMsg("Buffer cannot satify long read")
                                  .kv("offset", offset)
                                  .kv("bufferSize", bufferSize).toString());
        }
        try {
            return byteBuffer.getLong(offset);
        } catch (Exception e) {
            throw new IOException(exMsg("Error reading long")
                                  .kv("byteBuffer", byteBuffer.toString())
                                  .kv("offset", offset)
                                  .kv("bufferSize", bufferSize).toString(), e);
        }
    }

    /**
     * Read a bytebuf of size from the buffer at the given offset.
     * If there are not enough bytes in the buffer to satify the read, some of the bytes are read
     * into the byte buffer and the number of bytes read is returned.
     */
    int readByteBuf(ByteBuf buffer, int offset, int size) throws IOException {
        int originalLimit = byteBuffer.limit();
        byteBuffer.position(offset);
        int bytesToRead = Math.min(size, byteBuffer.capacity() - offset);
        byteBuffer.limit(offset + bytesToRead);
        try {
            buffer.writeBytes(byteBuffer);
        } catch (Exception e) {
            throw new IOException(exMsg("Error reading buffer")
                                  .kv("byteBuffer", byteBuffer.toString())
                                  .kv("offset", offset).kv("size", size)
                                  .kv("bufferSize", bufferSize).toString(), e);
        } finally {
            byteBuffer.limit(originalLimit);
        }
        return bytesToRead;
    }

    /**
     * The data pointer object for the native buffer. This can be used
     * by JNI method which take a char* or void*.
     */
    long pointer() {
        return pointer;
    }

    long pointer(long offset, long expectedWrite) {
        if (offset == 0) {
            return pointer;
        } else {
            if (offset + expectedWrite > byteBuffer.capacity()) {
                throw new IllegalArgumentException(
                        exMsg("Buffer overflow").kv("offset", offset).kv("expectedWrite", expectedWrite)
                        .kv("capacity", byteBuffer.capacity()).toString());
            }

            return pointer + offset;
        }
    }
    /**
     * @return the number of bytes which have been written to this buffer.
     */
    int position() {
        return byteBuffer.position();
    }

    /**
     * @return the size of the buffer (i.e. the max number of bytes writable, or the max offset readable)
     */
    int size() {
        return bufferSize;
    }

    /**
     * Pad the buffer to the next alignment position.
     * @return the position of the next alignment. This should be used as the size argument to make aligned writes.
     */
    int padToAlignment() {
        int bufferPos = byteBuffer.position();
        int nextAlignment = nextAlignment(bufferPos);
        byteBuffer.put(PADDING, 0, nextAlignment - bufferPos);
        return nextAlignment;
    }

    /**
     * Clear the bytes written. This doesn't actually destroy the data, but moves the position back to the start of
     * the buffer.
     */
    void reset() {
        byteBuffer.clear();
    }

    /**
     * Free the memory that backs this buffer.
     */
    void free() {
        ReferenceCountUtil.release(buffer);
        buffer = null;
        byteBuffer = null;
    }
    private static byte[] generatePadding() {
        byte[] padding = new byte[ALIGNMENT];
        Arrays.fill(padding, (byte) PADDING_BYTE);
        return padding;
    }

    static boolean isAligned(long size) {
        return size >= 0 && ((ALIGNMENT - 1) & size) == 0;
    }

    static int nextAlignment(int pos) {
        checkArgument(pos <= MAX_ALIGNMENT,
                      "position (0x%x) must be lower or equal to max alignment (0x%x)",
                       pos, MAX_ALIGNMENT);
        checkArgument(pos >= 0, "position (0x%x) must be positive", pos);
        return (pos + (ALIGNMENT - 1)) & ~(ALIGNMENT - 1);
    }
}
