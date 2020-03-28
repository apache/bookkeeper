/**
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

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.bookkeeper.proto.BookieProtocol.PacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.checksum.MacDigestManager;
import org.apache.bookkeeper.util.ByteBufList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for encoding and decoding the Bookkeeper protocol.
 */
public class BookieProtoEncoding {
    private static final Logger LOG = LoggerFactory.getLogger(BookieProtoEncoding.class);

    /**
     * An encoder/decoder interface for the Bookkeeper protocol.
     */
    public interface EnDecoder {
        /**
         * Encode a <i>object</i> into channel buffer.
         *
         * @param object
         *          object.
         * @return encode buffer.
         * @throws Exception
         */
        Object encode(Object object, ByteBufAllocator allocator) throws Exception;

        /**
         * Decode a <i>packet</i> into an object.
         *
         * @param packet
         *          received packet.
         * @return parsed object.
         * @throws Exception
         */
        Object decode(ByteBuf packet) throws Exception;

    }

    /**
     * An encoder/decoder for the Bookkeeper protocol before version 3.
     */
    public static class RequestEnDeCoderPreV3 implements EnDecoder {
        final ExtensionRegistry extensionRegistry;

        //This empty master key is used when an empty password is provided which is the hash of an empty string
        private static final byte[] emptyPasswordMasterKey;
        static {
            try {
                emptyPasswordMasterKey = MacDigestManager.genDigest("ledger", new byte[0]);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        public RequestEnDeCoderPreV3(ExtensionRegistry extensionRegistry) {
            this.extensionRegistry = extensionRegistry;
        }

        @Override
        public Object encode(Object msg, ByteBufAllocator allocator)
                throws Exception {
            if (!(msg instanceof BookieProtocol.Request)) {
                return msg;
            }
            BookieProtocol.Request r = (BookieProtocol.Request) msg;
            if (r instanceof BookieProtocol.AddRequest) {
                BookieProtocol.AddRequest ar = (BookieProtocol.AddRequest) r;
                int totalHeaderSize = 4 // for the header
                    + BookieProtocol.MASTER_KEY_LENGTH; // for the master key
                ByteBuf buf = allocator.buffer(totalHeaderSize);
                buf.writeInt(PacketHeader.toInt(r.getProtocolVersion(), r.getOpCode(), r.getFlags()));
                buf.writeBytes(r.getMasterKey(), 0, BookieProtocol.MASTER_KEY_LENGTH);
                ByteBufList data = ar.getData();
                ar.recycle();
                data.prepend(buf);
                return data;
            } else if (r instanceof BookieProtocol.ReadRequest) {
                int totalHeaderSize = 4 // for request type
                    + 8 // for ledgerId
                    + 8; // for entryId
                if (r.hasMasterKey()) {
                    totalHeaderSize += BookieProtocol.MASTER_KEY_LENGTH;
                }

                ByteBuf buf = allocator.buffer(totalHeaderSize);
                buf.writeInt(PacketHeader.toInt(r.getProtocolVersion(), r.getOpCode(), r.getFlags()));
                buf.writeLong(r.getLedgerId());
                buf.writeLong(r.getEntryId());
                if (r.hasMasterKey()) {
                    buf.writeBytes(r.getMasterKey(), 0, BookieProtocol.MASTER_KEY_LENGTH);
                }

                return buf;
            } else if (r instanceof BookieProtocol.AuthRequest) {
                BookkeeperProtocol.AuthMessage am = ((BookieProtocol.AuthRequest) r).getAuthMessage();
                int totalHeaderSize = 4; // for request type
                int totalSize = totalHeaderSize + am.getSerializedSize();
                ByteBuf buf = allocator.buffer(totalSize);
                buf.writeInt(PacketHeader.toInt(r.getProtocolVersion(), r.getOpCode(), r.getFlags()));
                ByteBufOutputStream bufStream = new ByteBufOutputStream(buf);
                am.writeTo(bufStream);
                return buf;
            } else {
                return msg;
            }
        }

        @Override
        public Object decode(ByteBuf packet)
                throws Exception {
            int packetHeader = packet.readInt();
            byte version = PacketHeader.getVersion(packetHeader);
            byte opCode = PacketHeader.getOpCode(packetHeader);
            short flags = PacketHeader.getFlags(packetHeader);

            // packet format is different between ADDENTRY and READENTRY
            long ledgerId = -1;
            long entryId = BookieProtocol.INVALID_ENTRY_ID;

            switch (opCode) {
            case BookieProtocol.ADDENTRY: {
                byte[] masterKey = readMasterKey(packet);

                // Read ledger and entry id without advancing the reader index
                ledgerId = packet.getLong(packet.readerIndex());
                entryId = packet.getLong(packet.readerIndex() + 8);
                // mark the reader index so that any resets will return to the
                // start of the payload
                packet.markReaderIndex();
                return BookieProtocol.ParsedAddRequest.create(
                        version, ledgerId, entryId, flags,
                        masterKey, packet.retain());
            }

            case BookieProtocol.READENTRY:
                ledgerId = packet.readLong();
                entryId = packet.readLong();

                if ((flags & BookieProtocol.FLAG_DO_FENCING) == BookieProtocol.FLAG_DO_FENCING
                    && version >= 2) {
                    byte[] masterKey = readMasterKey(packet);
                    return new BookieProtocol.ReadRequest(version, ledgerId, entryId, flags, masterKey);
                } else {
                    return new BookieProtocol.ReadRequest(version, ledgerId, entryId, flags, null);
                }
            case BookieProtocol.AUTH:
                BookkeeperProtocol.AuthMessage.Builder builder = BookkeeperProtocol.AuthMessage.newBuilder();
                builder.mergeFrom(new ByteBufInputStream(packet), extensionRegistry);
                return new BookieProtocol.AuthRequest(version, builder.build());

            default:
                throw new IllegalStateException("Received unknown request op code = " + opCode);
            }
        }

        private static byte[] readMasterKey(ByteBuf packet) {
            byte[] masterKey = null;

            // check if the master key is an empty master key
            boolean isEmptyKey = true;
            for (int i = 0; i < BookieProtocol.MASTER_KEY_LENGTH; i++) {
                if (packet.getByte(packet.readerIndex() + i) != emptyPasswordMasterKey[i]) {
                    isEmptyKey = false;
                    break;
                }
            }

            if (isEmptyKey) {
                // avoid new allocations if incoming master key is empty and use the static master key
                masterKey = emptyPasswordMasterKey;
                packet.readerIndex(packet.readerIndex() + BookieProtocol.MASTER_KEY_LENGTH);
            } else {
                // Master key is set, we need to copy and check it
                masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];
                packet.readBytes(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);
            }

            return masterKey;
        }
    }

    /**
     * A response encoder/decoder for the Bookkeeper protocol before version 3.
     */
    public static class ResponseEnDeCoderPreV3 implements EnDecoder {
        final ExtensionRegistry extensionRegistry;

        public ResponseEnDeCoderPreV3(ExtensionRegistry extensionRegistry) {
            this.extensionRegistry = extensionRegistry;
        }

        @Override
        public Object encode(Object msg, ByteBufAllocator allocator)
                throws Exception {
            if (!(msg instanceof BookieProtocol.Response)) {
                return msg;
            }
            BookieProtocol.Response r = (BookieProtocol.Response) msg;
            ByteBuf buf = allocator.buffer(24);
            buf.writeInt(PacketHeader.toInt(r.getProtocolVersion(), r.getOpCode(), (short) 0));

            try {
                if (msg instanceof BookieProtocol.ReadResponse) {
                    buf.writeInt(r.getErrorCode());
                    buf.writeLong(r.getLedgerId());
                    buf.writeLong(r.getEntryId());

                    BookieProtocol.ReadResponse rr = (BookieProtocol.ReadResponse) r;
                    if (rr.hasData()) {
                        return ByteBufList.get(buf, rr.getData());
                    } else {
                        return buf;
                    }
                } else if (msg instanceof BookieProtocol.AddResponse) {
                    buf.writeInt(r.getErrorCode());
                    buf.writeLong(r.getLedgerId());
                    buf.writeLong(r.getEntryId());

                    return buf;
                } else if (msg instanceof BookieProtocol.AuthResponse) {
                    BookkeeperProtocol.AuthMessage am = ((BookieProtocol.AuthResponse) r).getAuthMessage();
                    return ByteBufList.get(buf, Unpooled.wrappedBuffer(am.toByteArray()));
                } else {
                    LOG.error("Cannot encode unknown response type {}", msg.getClass().getName());
                    return msg;
                }
            } finally {
                r.recycle();
            }
        }
        @Override
        public Object decode(ByteBuf buffer)
                throws Exception {
            int rc;
            long ledgerId, entryId;

            int packetHeader = buffer.readInt();
            byte version = PacketHeader.getVersion(packetHeader);
            byte opCode = PacketHeader.getOpCode(packetHeader);

            switch (opCode) {
            case BookieProtocol.ADDENTRY:
                rc = buffer.readInt();
                ledgerId = buffer.readLong();
                entryId = buffer.readLong();
                return BookieProtocol.AddResponse.create(version, rc, ledgerId, entryId);
            case BookieProtocol.READENTRY:
                rc = buffer.readInt();
                ledgerId = buffer.readLong();
                entryId = buffer.readLong();

                return new BookieProtocol.ReadResponse(
                        version, rc, ledgerId, entryId, buffer.retainedSlice());
            case BookieProtocol.AUTH:
                ByteBufInputStream bufStream = new ByteBufInputStream(buffer);
                BookkeeperProtocol.AuthMessage.Builder builder = BookkeeperProtocol.AuthMessage.newBuilder();
                builder.mergeFrom(bufStream, extensionRegistry);
                BookkeeperProtocol.AuthMessage am = builder.build();
                return new BookieProtocol.AuthResponse(version, am);
            default:
                throw new IllegalStateException("Received unknown response : op code = " + opCode);
            }
        }
    }

    /**
     * A request encoder/decoder for the Bookkeeper protocol version 3.
     */
    public static class RequestEnDecoderV3 implements EnDecoder {
        final ExtensionRegistry extensionRegistry;

        public RequestEnDecoderV3(ExtensionRegistry extensionRegistry) {
            this.extensionRegistry = extensionRegistry;
        }

        @Override
        public Object decode(ByteBuf packet) throws Exception {
            return BookkeeperProtocol.Request.parseFrom(new ByteBufInputStream(packet), extensionRegistry);
        }

        @Override
        public Object encode(Object msg, ByteBufAllocator allocator) throws Exception {
            BookkeeperProtocol.Request request = (BookkeeperProtocol.Request) msg;
            return serializeProtobuf(request, allocator);
        }

    }

    /**
     * A response encoder/decoder for the Bookkeeper protocol version 3.
     */
    public static class ResponseEnDecoderV3 implements EnDecoder {
        final ExtensionRegistry extensionRegistry;

        public ResponseEnDecoderV3(ExtensionRegistry extensionRegistry) {
            this.extensionRegistry = extensionRegistry;
        }

        @Override
        public Object decode(ByteBuf packet) throws Exception {
            return BookkeeperProtocol.Response.parseFrom(new ByteBufInputStream(packet),
                                                         extensionRegistry);
        }

        @Override
        public Object encode(Object msg, ByteBufAllocator allocator) throws Exception {
            BookkeeperProtocol.Response response = (BookkeeperProtocol.Response) msg;
            return serializeProtobuf(response, allocator);
        }

    }

    private static ByteBuf serializeProtobuf(MessageLite msg, ByteBufAllocator allocator) {
        int size = msg.getSerializedSize();
        ByteBuf buf = allocator.heapBuffer(size, size);

        try {
            msg.writeTo(CodedOutputStream.newInstance(buf.array(), buf.arrayOffset() + buf.writerIndex(), size));
        } catch (IOException e) {
            // This is in-memory serialization, should not fail
            throw new RuntimeException(e);
        }

        // Advance writer idx
        buf.writerIndex(buf.capacity());
        return buf;
    }

    /**
     * A request message encoder.
     */
    @Sharable
    public static class RequestEncoder extends ChannelOutboundHandlerAdapter {

        final EnDecoder reqPreV3;
        final EnDecoder reqV3;

        public RequestEncoder(ExtensionRegistry extensionRegistry) {
            reqPreV3 = new RequestEnDeCoderPreV3(extensionRegistry);
            reqV3 = new RequestEnDecoderV3(extensionRegistry);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Encode request {} to channel {}.", msg, ctx.channel());
            }
            if (msg instanceof BookkeeperProtocol.Request) {
                ctx.write(reqV3.encode(msg, ctx.alloc()), promise);
            } else if (msg instanceof BookieProtocol.Request) {
                ctx.write(reqPreV3.encode(msg, ctx.alloc()), promise);
            } else {
                LOG.error("Invalid request to encode to {}: {}", ctx.channel(), msg.getClass().getName());
                ctx.write(msg, promise);
            }
        }
    }

    /**
     * A request message decoder.
     */
    @Sharable
    public static class RequestDecoder extends ChannelInboundHandlerAdapter {
        final EnDecoder reqPreV3;
        final EnDecoder reqV3;
        boolean usingV3Protocol;

        RequestDecoder(ExtensionRegistry extensionRegistry) {
            reqPreV3 = new RequestEnDeCoderPreV3(extensionRegistry);
            reqV3 = new RequestEnDecoderV3(extensionRegistry);
            usingV3Protocol = true;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Received request {} from channel {} to decode.", msg, ctx.channel());
            }
            try {
                if (!(msg instanceof ByteBuf)) {
                    LOG.error("Received invalid request {} from channel {} to decode.", msg, ctx.channel());
                    ctx.fireChannelRead(msg);
                    return;
                }
                ByteBuf buffer = (ByteBuf) msg;
                buffer.markReaderIndex();
                Object result;
                if (usingV3Protocol) {
                    try {
                        result = reqV3.decode(buffer);
                    } catch (InvalidProtocolBufferException e) {
                        usingV3Protocol = false;
                        buffer.resetReaderIndex();
                        result = reqPreV3.decode(buffer);
                    }
                } else {
                    result = reqPreV3.decode(buffer);
                }
                ctx.fireChannelRead(result);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }
    }

    /**
     * A response message encoder.
     */
    @Sharable
    public static class ResponseEncoder extends ChannelOutboundHandlerAdapter {
        final EnDecoder repPreV3;
        final EnDecoder repV3;

        ResponseEncoder(ExtensionRegistry extensionRegistry) {
            repPreV3 = new ResponseEnDeCoderPreV3(extensionRegistry);
            repV3 = new ResponseEnDecoderV3(extensionRegistry);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Encode response {} to channel {}.", msg, ctx.channel());
            }
            if (msg instanceof BookkeeperProtocol.Response) {
                ctx.write(repV3.encode(msg, ctx.alloc()), promise);
            } else if (msg instanceof BookieProtocol.Response) {
                ctx.write(repPreV3.encode(msg, ctx.alloc()), promise);
            } else {
                LOG.error("Invalid response to encode to {}: {}", ctx.channel(), msg.getClass().getName());
                ctx.write(msg, promise);
            }
        }
    }

    /**
     * A response message decoder.
     */
    @Sharable
    public static class ResponseDecoder extends ChannelInboundHandlerAdapter {
        final EnDecoder repPreV3;
        final EnDecoder repV3;
        final boolean useV2Protocol;
        final boolean tlsEnabled;
        boolean usingV3Protocol;

        ResponseDecoder(ExtensionRegistry extensionRegistry,
                        boolean useV2Protocol,
                        boolean tlsEnabled) {
            this.repPreV3 = new ResponseEnDeCoderPreV3(extensionRegistry);
            this.repV3 = new ResponseEnDecoderV3(extensionRegistry);
            this.useV2Protocol = useV2Protocol;
            this.tlsEnabled = tlsEnabled;
            usingV3Protocol = true;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Received response {} from channel {} to decode.", msg, ctx.channel());
            }
            try {
                if (!(msg instanceof ByteBuf)) {
                    LOG.error("Received invalid response {} from channel {} to decode.", msg, ctx.channel());
                    ctx.fireChannelRead(msg);
                    return;
                }
                ByteBuf buffer = (ByteBuf) msg;
                buffer.markReaderIndex();

                Object result;
                if (!useV2Protocol) { // always use v3 protocol
                    result = repV3.decode(buffer);
                } else { // use v2 protocol but
                    // if TLS enabled, the first message `startTLS` is a protobuf message
                    if (tlsEnabled && usingV3Protocol) {
                        try {
                            result = repV3.decode(buffer);
                            if (result instanceof Response
                                && OperationType.START_TLS == ((Response) result).getHeader().getOperation()) {
                                usingV3Protocol = false;
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Degrade bookkeeper to v2 after starting TLS.");
                                }
                            }
                        } catch (InvalidProtocolBufferException e) {
                            usingV3Protocol = false;
                            buffer.resetReaderIndex();
                            result = repPreV3.decode(buffer);
                        }
                    } else {
                        result = repPreV3.decode(buffer);
                    }
                }
                ctx.fireChannelRead(result);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }
    }
}
