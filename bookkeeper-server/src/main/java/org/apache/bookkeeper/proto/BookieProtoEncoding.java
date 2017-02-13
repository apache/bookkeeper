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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.io.IOException;
import java.util.List;

import org.apache.bookkeeper.proto.BookieProtocol.PacketHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

public class BookieProtoEncoding {
    private final static Logger LOG = LoggerFactory.getLogger(BookieProtoEncoding.class);

    static interface EnDecoder {

        /**
         * Encode a <i>object</i> into channel buffer.
         *
         * @param object
         *          object.
         * @return encode buffer.
         * @throws Exception
         */
        public Object encode(Object object, ByteBufAllocator allocator) throws Exception;

        /**
         * Decode a <i>packet</i> into an object.
         *
         * @param packet
         *          received packet.
         * @return parsed object.
         * @throws Exception
         */
        public Object decode(ByteBuf packet) throws Exception;

    }

    static class RequestEnDeCoderPreV3 implements EnDecoder {
        final ExtensionRegistry extensionRegistry;

        RequestEnDeCoderPreV3(ExtensionRegistry extensionRegistry) {
            this.extensionRegistry = extensionRegistry;
        }

        @Override
        public Object encode(Object msg, ByteBufAllocator allocator)
                throws Exception {
            if (!(msg instanceof BookieProtocol.Request)) {
                return msg;
            }
            BookieProtocol.Request r = (BookieProtocol.Request)msg;
            if (r instanceof BookieProtocol.AddRequest) {
                BookieProtocol.AddRequest ar = (BookieProtocol.AddRequest)r;
                int totalHeaderSize = 4 // for the header
                    + BookieProtocol.MASTER_KEY_LENGTH; // for the master key
                ByteBuf buf = allocator.buffer(totalHeaderSize);
                buf.writeInt(new PacketHeader(r.getProtocolVersion(), r.getOpCode(), r.getFlags()).toInt());
                buf.writeBytes(r.getMasterKey(), 0, BookieProtocol.MASTER_KEY_LENGTH);
                return new CompositeByteBuf(allocator, false, 2, buf, ar.getData());
            } else if (r instanceof BookieProtocol.ReadRequest) {
                int totalHeaderSize = 4 // for request type
                    + 8 // for ledgerId
                    + 8; // for entryId
                if (r.hasMasterKey()) {
                    totalHeaderSize += BookieProtocol.MASTER_KEY_LENGTH;
                }

                ByteBuf buf = allocator.buffer(totalHeaderSize);
                buf.writeInt(new PacketHeader(r.getProtocolVersion(), r.getOpCode(), r.getFlags()).toInt());
                buf.writeLong(r.getLedgerId());
                buf.writeLong(r.getEntryId());
                if (r.hasMasterKey()) {
                    buf.writeBytes(r.getMasterKey(), 0, BookieProtocol.MASTER_KEY_LENGTH);
                }

                return buf;
            } else if (r instanceof BookieProtocol.AuthRequest) {
                BookkeeperProtocol.AuthMessage am = ((BookieProtocol.AuthRequest)r).getAuthMessage();
                int totalHeaderSize = 4; // for request type
                int totalSize = totalHeaderSize + am.getSerializedSize();
                ByteBuf buf = allocator.buffer(totalSize);
                buf.writeInt(new PacketHeader(r.getProtocolVersion(),
                                              r.getOpCode(),
                                              r.getFlags()).toInt());
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
            PacketHeader h = PacketHeader.fromInt(packet.readInt());

            // packet format is different between ADDENTRY and READENTRY
            long ledgerId = -1;
            long entryId = BookieProtocol.INVALID_ENTRY_ID;
            byte[] masterKey = null;
            short flags = h.getFlags();

            ServerStats.getInstance().incrementPacketsReceived();

            switch (h.getOpCode()) {
            case BookieProtocol.ADDENTRY:
                // first read master key
                masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];
                packet.readBytes(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);

                ByteBuf bb = packet.duplicate();

                ledgerId = bb.readLong();
                entryId = bb.readLong();
                return new BookieProtocol.AddRequest(h.getVersion(), ledgerId, entryId,
                        flags, masterKey, packet.slice());
            case BookieProtocol.READENTRY:
                ledgerId = packet.readLong();
                entryId = packet.readLong();

                if ((flags & BookieProtocol.FLAG_DO_FENCING) == BookieProtocol.FLAG_DO_FENCING
                    && h.getVersion() >= 2) {
                    masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];
                    packet.readBytes(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);
                    return new BookieProtocol.ReadRequest(h.getVersion(), ledgerId, entryId, flags, masterKey);
                } else {
                    return new BookieProtocol.ReadRequest(h.getVersion(), ledgerId, entryId, flags);
                }
            case BookieProtocol.AUTH:
                BookkeeperProtocol.AuthMessage.Builder builder
                    = BookkeeperProtocol.AuthMessage.newBuilder();
                builder.mergeFrom(new ByteBufInputStream(packet), extensionRegistry);
                return new BookieProtocol.AuthRequest(h.getVersion(), builder.build());
            }
            return packet;
        }
    }

    static class ResponseEnDeCoderPreV3 implements EnDecoder {
        final ExtensionRegistry extensionRegistry;

        ResponseEnDeCoderPreV3(ExtensionRegistry extensionRegistry) {
            this.extensionRegistry = extensionRegistry;
        }

        @Override
        public Object encode(Object msg, ByteBufAllocator allocator)
                throws Exception {
            if (!(msg instanceof BookieProtocol.Response)) {
                return msg;
            }
            BookieProtocol.Response r = (BookieProtocol.Response)msg;
            ByteBuf buf = allocator.buffer(24);
            buf.writeInt(new PacketHeader(r.getProtocolVersion(),
                                          r.getOpCode(), (short)0).toInt());

            ServerStats.getInstance().incrementPacketsSent();
            if (msg instanceof BookieProtocol.ReadResponse) {
                buf.writeInt(r.getErrorCode());
                buf.writeLong(r.getLedgerId());
                buf.writeLong(r.getEntryId());

                BookieProtocol.ReadResponse rr = (BookieProtocol.ReadResponse)r;
                if (rr.hasData()) {
                    return new CompositeByteBuf(allocator, false, 2, buf, rr.getData());
                } else {
                    return buf;
                }
            } else if (msg instanceof BookieProtocol.AddResponse) {
                buf.writeInt(r.getErrorCode());
                buf.writeLong(r.getLedgerId());
                buf.writeLong(r.getEntryId());

                return buf;
            } else if (msg instanceof BookieProtocol.AuthResponse) {
                BookkeeperProtocol.AuthMessage am = ((BookieProtocol.AuthResponse)r).getAuthMessage();
                return new CompositeByteBuf(allocator, false, 2, buf, Unpooled.wrappedBuffer(am.toByteArray()));
            } else {
                LOG.error("Cannot encode unknown response type {}", msg.getClass().getName());
                return msg;
            }
        }
        @Override
        public Object decode(ByteBuf buffer)
                throws Exception {
            int rc;
            long ledgerId, entryId;
            final PacketHeader header;

            header = PacketHeader.fromInt(buffer.readInt());

            switch (header.getOpCode()) {
            case BookieProtocol.ADDENTRY:
                rc = buffer.readInt();
                ledgerId = buffer.readLong();
                entryId = buffer.readLong();
                return new BookieProtocol.AddResponse(header.getVersion(), rc, ledgerId, entryId);
            case BookieProtocol.READENTRY:
                rc = buffer.readInt();
                ledgerId = buffer.readLong();
                entryId = buffer.readLong();

                if (rc == BookieProtocol.EOK) {
                    return new BookieProtocol.ReadResponse(header.getVersion(), rc,
                                                           ledgerId, entryId, buffer.slice());
                } else {
                    return new BookieProtocol.ReadResponse(header.getVersion(), rc,
                                                           ledgerId, entryId);
                }
            case BookieProtocol.AUTH:
                ByteBufInputStream bufStream = new ByteBufInputStream(buffer);
                BookkeeperProtocol.AuthMessage.Builder builder
                    = BookkeeperProtocol.AuthMessage.newBuilder();
                builder.mergeFrom(bufStream, extensionRegistry);
                BookkeeperProtocol.AuthMessage am = builder.build();
                return new BookieProtocol.AuthResponse(header.getVersion(), am);
            default:
                return buffer;
            }
        }
    }

    static class RequestEnDecoderV3 implements EnDecoder {
        final ExtensionRegistry extensionRegistry;

        RequestEnDecoderV3(ExtensionRegistry extensionRegistry) {
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

    static class ResponseEnDecoderV3 implements EnDecoder {
        final ExtensionRegistry extensionRegistry;

        ResponseEnDecoderV3(ExtensionRegistry extensionRegistry) {
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

    @Sharable
    public static class RequestEncoder extends MessageToMessageEncoder<Object> {

        final EnDecoder REQ_PREV3;
        final EnDecoder REQ_V3;

        RequestEncoder(ExtensionRegistry extensionRegistry) {
            REQ_PREV3 = new RequestEnDeCoderPreV3(extensionRegistry);
            REQ_V3 = new RequestEnDecoderV3(extensionRegistry);
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Encode request {} to channel {}.", msg, ctx.channel());
            }
            if (msg instanceof BookkeeperProtocol.Request) {
                out.add(REQ_V3.encode(msg, ctx.alloc()));
            } else if (msg instanceof BookieProtocol.Request) {
                out.add(REQ_PREV3.encode(msg, ctx.alloc()));
            } else {
                LOG.error("Invalid request to encode to {}: {}", ctx.channel(), msg.getClass().getName());
                out.add(msg);
            }
        }
    }

    @Sharable
    public static class RequestDecoder extends MessageToMessageDecoder<Object> {
        final EnDecoder REQ_PREV3;
        final EnDecoder REQ_V3;

        RequestDecoder(ExtensionRegistry extensionRegistry) {
            REQ_PREV3 = new RequestEnDeCoderPreV3(extensionRegistry);
            REQ_V3 = new RequestEnDecoderV3(extensionRegistry);
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received request {} from channel {} to decode.", msg, ctx.channel());
            }
            if (!(msg instanceof ByteBuf)) {
                out.add(msg);
                return;
            }
            ByteBuf buffer = (ByteBuf) msg;
            try {
                buffer.markReaderIndex();
                try {
                    out.add(REQ_V3.decode(buffer));
                } catch (InvalidProtocolBufferException e) {
                    buffer.resetReaderIndex();
                    out.add(REQ_PREV3.decode(buffer));
                }
            } catch (Exception e) {
                LOG.error("Failed to decode a request from {} : ", ctx.channel(), e);
                throw e;
            }
        }
    }

    @Sharable
    public static class ResponseEncoder extends MessageToMessageEncoder<Object> {
        final EnDecoder REP_PREV3;
        final EnDecoder REP_V3;

        ResponseEncoder(ExtensionRegistry extensionRegistry) {
            REP_PREV3 = new ResponseEnDeCoderPreV3(extensionRegistry);
            REP_V3 = new ResponseEnDecoderV3(extensionRegistry);
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out)
                throws Exception {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Encode response {} to channel {}.", msg, ctx.channel());
            }
            if (msg instanceof BookkeeperProtocol.Response) {
                out.add(REP_V3.encode(msg, ctx.alloc()));
            } else if (msg instanceof BookieProtocol.Response) {
                out.add(REP_PREV3.encode(msg, ctx.alloc()));
            } else {
                LOG.error("Invalid response to encode to {}: {}", ctx.channel(), msg.getClass().getName());
                out.add(msg);
            }
        }
    }

    @Sharable
    public static class ResponseDecoder extends MessageToMessageDecoder<Object> {
        final EnDecoder REP_PREV3;
        final EnDecoder REP_V3;

        ResponseDecoder(ExtensionRegistry extensionRegistry) {
            REP_PREV3 = new ResponseEnDeCoderPreV3(extensionRegistry);
            REP_V3 = new ResponseEnDecoderV3(extensionRegistry);
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received response {} from channel {} to decode.", msg, ctx.channel());
            }
            if (!(msg instanceof ByteBuf)) {
                out.add(msg);
            }
            ByteBuf buffer = (ByteBuf) msg;
            try {
                buffer.markReaderIndex();
                try {
                    out.add(REP_V3.decode(buffer));
                } catch (InvalidProtocolBufferException e) {
                    buffer.resetReaderIndex();
                    out.add(REP_PREV3.decode(buffer));
                }
            } catch (Exception e) {
                LOG.error("Failed to decode a response from channel {} : ", ctx.channel(), e);
                throw e;
            }
        }
    }
}
