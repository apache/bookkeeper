/*
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
package org.apache.bookkeeper.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import org.apache.bookkeeper.util.ByteBufList;

public class ByteStringUtil {

    /**
     * Wrap the internal buffers of a ByteBufList into a single ByteString.
     * The lifecycle of the wrapped ByteString is tied to the ByteBufList.
     *
     * @param bufList ByteBufList to wrap
     * @return ByteString wrapping the internal buffers of the ByteBufList
     */
    public static ByteString byteBufListToByteString(ByteBufList bufList) {
        ByteString aggregated = null;
        for (int i = 0; i < bufList.size(); i++) {
            ByteBuf buffer = bufList.getBuffer(i);
            if (buffer.readableBytes() > 0) {
                aggregated = byteBufToByteString(aggregated, buffer);
            }
        }
        return aggregated != null ? aggregated : ByteString.EMPTY;
    }

    /**
     * Wrap the internal buffers of a ByteBuf into a single ByteString.
     * The lifecycle of the wrapped ByteString is tied to the ByteBuf.
     *
     * @param byteBuf ByteBuf to wrap
     * @return ByteString wrapping the internal buffers of the ByteBuf
     */
    public static ByteString byteBufToByteString(ByteBuf byteBuf) {
        return byteBufToByteString(null, byteBuf);
    }

    // internal method to aggregate a ByteBuf into a single aggregated ByteString
    private static ByteString byteBufToByteString(ByteString aggregated, ByteBuf byteBuf) {
        if (byteBuf.readableBytes() == 0) {
            return ByteString.EMPTY;
        }
        if (byteBuf.nioBufferCount() > 1) {
            for (ByteBuffer nioBuffer : byteBuf.nioBuffers()) {
                ByteString piece = UnsafeByteOperations.unsafeWrap(nioBuffer);
                aggregated = (aggregated == null) ? piece : aggregated.concat(piece);
            }
        } else {
            ByteString piece;
            if (byteBuf.hasArray()) {
                piece = UnsafeByteOperations.unsafeWrap(byteBuf.array(), byteBuf.arrayOffset() + byteBuf.readerIndex(),
                        byteBuf.readableBytes());
            } else {
                piece = UnsafeByteOperations.unsafeWrap(byteBuf.nioBuffer());
            }
            aggregated = (aggregated == null) ? piece : aggregated.concat(piece);
        }
        return aggregated;
    }
}
