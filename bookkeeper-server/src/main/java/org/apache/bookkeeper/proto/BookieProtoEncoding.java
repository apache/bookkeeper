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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.bookkeeper.client.MacDigestManager;
import org.apache.bookkeeper.proto.BookieProtocol.PacketHeader;
import org.apache.bookkeeper.util.DoubleByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

public class BookieProtoEncoding {
    private static final Logger LOG = LoggerFactory.getLogger(BookieProtoEncoding.class);

    public static interface EnDecoder {
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

    public static class RequestEnDeCoderPreV3 implements EnDecoder {
        final ExtensionRegistry extensionRegistry;

        //This empty master key is used when an empty password is provided which is the hash of an empty string
        private final static byte[] emptyPasswordMasterKey;
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
            BookieProtocol.Request r = (BookieProtocol.Request)msg;
            if (r instanceof BookieProtocol.AddRequest) {
                BookieProtocol.AddRequest ar = (BookieProtocol.AddRequest)r;
                int totalHeaderSize = 4 // for the header
                    + BookieProtocol.MASTER_KEY_LENGTH; // for the master key
                ByteBuf buf = allocator.buffer(totalHeaderSize);
                buf.writeInt(PacketHeader.toInt(r.getProtocolVersion(), r.getOpCode(), r.getFlags()));
                buf.writeBytes(r.getMasterKey(), 0, BookieProtocol.MASTER_KEY_LENGTH);
                ByteBuf data = ar.getData();
                ar.recycle();
                return DoubleByteBuf.get(buf, data);
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
                BookkeeperProtocol.AuthMessage am = ((BookieProtocol.AuthRequest)r).getAuthMessage();
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

            ServerStats.getInstance().incrementPacketsReceived();

            switch (opCode) {
            case BookieProtocol.ADDENTRY: {
                byte[] masterKey = readMasterKey(packet);

                // Read ledger and entry id without advancing the reader index
                ledgerId = packet.getLong(packet.readerIndex());
                entryId = packet.getLong(packet.readerIndex() + 8);
                return BookieProtocol.AddRequest.create(
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
                    return new BookieProtocol.ReadRequest(version, ledgerId, entryId, flags);
                }
            case BookieProtocol.AUTH:
                BookkeeperProtocol.AuthMessage.Builder builder
                    = BookkeeperProtocol.AuthMessage.newBuilder();
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
            BookieProtocol.Response r = (BookieProtocol.Response)msg;
            ByteBuf buf = allocator.buffer(24);
            buf.writeInt(PacketHeader.toInt(r.getProtocolVersion(), r.getOpCode(), (short) 0));

            ServerStats.getInstance().incrementPacketsSent();
            try {
                if (msg instanceof BookieProtocol.ReadResponse) {
                    buf.writeInt(r.getErrorCode());
                    buf.writeLong(r.getLedgerId());
                    buf.writeLong(r.getEntryId());

                    BookieProtocol.ReadResponse rr = (BookieProtocol.ReadResponse) r;
                    if (rr.hasData()) {
                        return DoubleByteBuf.get(buf, rr.getData());
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
                    return DoubleByteBuf.get(buf, Unpooled.wrappedBuffer(am.toByteArray()));
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

                if (rc == BookieProtocol.EOK) {
                    return new BookieProtocol.ReadResponse(version, rc,
                                                           ledgerId, entryId, buffer.retainedSlice());
                } else {
                    return new BookieProtocol.ReadResponse(version, rc, ledgerId, entryId);
                }
            case BookieProtocol.AUTH:
                ByteBufInputStream bufStream = new ByteBufInputStream(buffer);
                BookkeeperProtocol.AuthMessage.Builder builder
                    = BookkeeperProtocol.AuthMessage.newBuilder();
                builder.mergeFrom(bufStream, extensionRegistry);
                BookkeeperProtocol.AuthMessage am = builder.build();
                return new BookieProtocol.AuthResponse(version, am);
            default:
                throw new IllegalStateException("Received unknown response : op code = " + opCode);
            }
        }
    }

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

    @Sharable
    public static class RequestEncoder extends MessageToMessageEncoder<Object> {

        final EnDecoder REQ_PREV3;
        final EnDecoder REQ_V3;

        public RequestEncoder(ExtensionRegistry extensionRegistry) {
            REQ_PREV3 = new RequestEnDeCoderPreV3(extensionRegistry);
            REQ_V3 = new RequestEnDecoderV3(extensionRegistry);
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
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
        boolean usingV3Protocol;

        RequestDecoder(ExtensionRegistry extensionRegistry) {
            REQ_PREV3 = new RequestEnDeCoderPreV3(extensionRegistry);
            REQ_V3 = new RequestEnDecoderV3(extensionRegistry);
            usingV3Protocol = true;
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
            buffer.markReaderIndex();

            if (usingV3Protocol) {
                try {
                    out.add(REQ_V3.decode(buffer));
                } catch (InvalidProtocolBufferException e) {
                    usingV3Protocol = false;
                    buffer.resetReaderIndex();
                    out.add(REQ_PREV3.decode(buffer));
                }
            } else {
                out.add(REQ_PREV3.decode(buffer));
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
        boolean usingV2Protocol;

        ResponseDecoder(ExtensionRegistry extensionRegistry, boolean useV2Protocol) {
            REP_PREV3 = new ResponseEnDeCoderPreV3(extensionRegistry);
            REP_V3 = new ResponseEnDecoderV3(extensionRegistry);
            usingV2Protocol = useV2Protocol;
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
            buffer.markReaderIndex();

            if (!usingV2Protocol) {
                out.add(REP_V3.decode(buffer));
            } else {
                // If in the same connection we already got preV3 messages, don't try again to decode V3 messages
                out.add(REP_PREV3.decode(buffer));
            }
        }
    }
}
