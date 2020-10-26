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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;

import org.apache.bookkeeper.proto.BookkeeperProtocol.AuthMessage;
import org.apache.bookkeeper.util.ByteBufList;

/**
 * The packets of the Bookie protocol all have a 4-byte integer indicating the
 * type of request or response at the very beginning of the packet followed by a
 * payload.
 *
 */
public interface BookieProtocol {

    /**
     * Lowest protocol version which will work with the bookie.
     */
    byte LOWEST_COMPAT_PROTOCOL_VERSION = 0;

    /**
     * Current version of the protocol, which client will use.
     */
    byte CURRENT_PROTOCOL_VERSION = 2;

    /**
     * Entry Entry ID. To be used when no valid entry id can be assigned.
     */
    long INVALID_ENTRY_ID = -1;

    /**
     * Entry identifier representing a request to obtain the last add entry confirmed.
     */
    long LAST_ADD_CONFIRMED = -1;

    /**
     * The length of the master key in add packets. This
     * is fixed at 20 for historic reasons. This is because it
     * is always generated using the MacDigestManager regardless
     * of whether Mac is being used for the digest or not
     */
    int MASTER_KEY_LENGTH = 20;

    /**
     * The first int of a packet is the header.
     * It contains the version, opCode and flags.
     * The initial versions of BK didn't have this structure
     * and just had an int representing the opCode as the
     * first int. This handles that case also.
     */
    final class PacketHeader {
        public static int toInt(byte version, byte opCode, short flags) {
            if (version == 0) {
                return (int) opCode;
            } else {
                return ((version & 0xFF) << 24)
                    | ((opCode & 0xFF) << 16)
                    | (flags & 0xFFFF);
            }
        }

        public static byte getVersion(int packetHeader) {
            return (byte) (packetHeader >> 24);
        }

        public static byte getOpCode(int packetHeader) {
            int version = getVersion(packetHeader);
            if (version == 0) {
                return (byte) packetHeader;
            } else {
                return (byte) ((packetHeader >> 16) & 0xFF);
            }
        }

        public static short getFlags(int packetHeader) {
            byte version = (byte) (packetHeader >> 24);
            if (version == 0) {
                return 0;
            } else {
                return (short) (packetHeader & 0xFFFF);
            }
        }
    }

    /**
     * The Add entry request payload will be a ledger entry exactly as it should
     * be logged. The response payload will be a 4-byte integer that has the
     * error code followed by the 8-byte ledger number and 8-byte entry number
     * of the entry written.
     */
    byte ADDENTRY = 1;
    /**
     * The Read entry request payload will be the ledger number and entry number
     * to read. (The ledger number is an 8-byte integer and the entry number is
     * a 8-byte integer.) The response payload will be a 4-byte integer
     * representing an error code and a ledger entry if the error code is EOK,
     * otherwise it will be the 8-byte ledger number and the 4-byte entry number
     * requested. (Note that the first sixteen bytes of the entry happen to be
     * the ledger number and entry number as well.)
     */
    byte READENTRY = 2;

    /**
     * Auth message. This code is for passing auth messages between the auth
     * providers on the client and bookie. The message payload is determined
     * by the auth providers themselves.
     */
    byte AUTH = 3;
    byte READ_LAC = 4;
    byte WRITE_LAC = 5;
    byte GET_BOOKIE_INFO = 6;

    /**
     * The error code that indicates success.
     */
    int EOK = 0;
    /**
     * The error code that indicates that the ledger does not exist.
     */
    int ENOLEDGER = 1;
    /**
     * The error code that indicates that the requested entry does not exist.
     */
    int ENOENTRY = 2;
    /**
     * The error code that indicates an invalid request type.
     */
    int EBADREQ = 100;
    /**
     * General error occurred at the server.
     */
    int EIO = 101;

    /**
     * Unauthorized access to ledger.
     */
    int EUA = 102;

    /**
     * The server version is incompatible with the client.
     */
    int EBADVERSION = 103;

    /**
     * Attempt to write to fenced ledger.
     */
    int EFENCED = 104;

    /**
     * The server is running as read-only mode.
     */
    int EREADONLY = 105;

    /**
     * Too many concurrent requests.
     */
    int ETOOMANYREQUESTS = 106;

    short FLAG_NONE = 0x0;
    short FLAG_DO_FENCING = 0x0001;
    short FLAG_RECOVERY_ADD = 0x0002;
    short FLAG_HIGH_PRIORITY = 0x0004;

    /**
     * A Bookie request object.
     */
    class Request {
        byte protocolVersion;
        byte opCode;
        long ledgerId;
        long entryId;
        short flags;
        byte[] masterKey;

        protected void init(byte protocolVersion, byte opCode, long ledgerId,
                          long entryId, short flags, byte[] masterKey) {
            this.protocolVersion = protocolVersion;
            this.opCode = opCode;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.flags = flags;
            this.masterKey = masterKey;
        }

        byte getProtocolVersion() {
            return protocolVersion;
        }

        byte getOpCode() {
            return opCode;
        }

        long getLedgerId() {
            return ledgerId;
        }

        long getEntryId() {
            return entryId;
        }

        short getFlags() {
            return flags;
        }

        boolean hasMasterKey() {
            return masterKey != null;
        }

        byte[] getMasterKey() {
            assert hasMasterKey();
            return masterKey;
        }

        boolean isHighPriority() {
            return (flags & FLAG_HIGH_PRIORITY) == FLAG_HIGH_PRIORITY;
        }

        @Override
        public String toString() {
            return String.format("Op(%d)[Ledger:%d,Entry:%d]", opCode, ledgerId, entryId);
        }

        public void recycle() {}
    }

    /**
     * A Request that adds data.
     */
    class AddRequest extends Request {
        ByteBufList data;

        static AddRequest create(byte protocolVersion, long ledgerId,
                                 long entryId, short flags, byte[] masterKey,
                                 ByteBufList data) {
            AddRequest add = RECYCLER.get();
            add.protocolVersion = protocolVersion;
            add.opCode = ADDENTRY;
            add.ledgerId = ledgerId;
            add.entryId = entryId;
            add.flags = flags;
            add.masterKey = masterKey;
            add.data = data.retain();
            return add;
        }

        ByteBufList getData() {
            // We need to have different ByteBufList instances for each bookie write
            return ByteBufList.clone(data);
        }

        boolean isRecoveryAdd() {
            return (flags & FLAG_RECOVERY_ADD) == FLAG_RECOVERY_ADD;
        }

        private final Handle<AddRequest> recyclerHandle;
        private AddRequest(Handle<AddRequest> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<AddRequest> RECYCLER = new Recycler<AddRequest>() {
            @Override
            protected AddRequest newObject(Handle<AddRequest> handle) {
                return new AddRequest(handle);
            }
        };

        @Override
        public void recycle() {
            ledgerId = -1;
            entryId = -1;
            masterKey = null;
            ReferenceCountUtil.safeRelease(data);
            data = null;
            recyclerHandle.recycle(this);
        }
    }

    /**
     * This is similar to add request, but it used when processing the request on the bookie side.
     */
    class ParsedAddRequest extends Request {
        ByteBuf data;

        static ParsedAddRequest create(byte protocolVersion, long ledgerId, long entryId, short flags, byte[] masterKey,
                ByteBuf data) {
            ParsedAddRequest add = RECYCLER.get();
            add.protocolVersion = protocolVersion;
            add.opCode = ADDENTRY;
            add.ledgerId = ledgerId;
            add.entryId = entryId;
            add.flags = flags;
            add.masterKey = masterKey;
            add.data = data.retain();
            return add;
        }

        ByteBuf getData() {
            // We need to have different ByteBufList instances for each bookie write
            return data;
        }

        boolean isRecoveryAdd() {
            return (flags & FLAG_RECOVERY_ADD) == FLAG_RECOVERY_ADD;
        }

        void release() {
            data.release();
        }

        private final Handle<ParsedAddRequest> recyclerHandle;
        private ParsedAddRequest(Handle<ParsedAddRequest> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<ParsedAddRequest> RECYCLER = new Recycler<ParsedAddRequest>() {
            @Override
            protected ParsedAddRequest newObject(Handle<ParsedAddRequest> handle) {
                return new ParsedAddRequest(handle);
            }
        };

        @Override
        public void recycle() {
            ledgerId = -1;
            entryId = -1;
            masterKey = null;
            data = null;
            recyclerHandle.recycle(this);
        }
    }

    /**
     * A Request that reads data.
     */
    class ReadRequest extends Request {
        ReadRequest(byte protocolVersion, long ledgerId, long entryId,
                    short flags, byte[] masterKey) {
            init(protocolVersion, READENTRY, ledgerId, entryId, flags, masterKey);
        }

        boolean isFencing() {
            return (flags & FLAG_DO_FENCING) == FLAG_DO_FENCING;
        }
    }

    /**
     * An authentication request.
     */
    class AuthRequest extends Request {
        final AuthMessage authMessage;

        AuthRequest(byte protocolVersion, AuthMessage authMessage) {
            init(protocolVersion, AUTH, -1, -1, FLAG_NONE, null);
            this.authMessage = authMessage;
        }

        AuthMessage getAuthMessage() {
            return authMessage;
        }
    }

    /**
     * A response object.
     */
    abstract class Response {
        byte protocolVersion;
        byte opCode;
        int errorCode;
        long ledgerId;
        long entryId;

        protected void init(byte protocolVersion, byte opCode,
                           int errorCode, long ledgerId, long entryId) {
            this.protocolVersion = protocolVersion;
            this.opCode = opCode;
            this.errorCode = errorCode;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        byte getProtocolVersion() {
            return protocolVersion;
        }

        byte getOpCode() {
            return opCode;
        }

        long getLedgerId() {
            return ledgerId;
        }

        long getEntryId() {
            return entryId;
        }

        int getErrorCode() {
            return errorCode;
        }

        @Override
        public String toString() {
            return String.format("Op(%d)[Ledger:%d,Entry:%d,errorCode=%d]",
                                 opCode, ledgerId, entryId, errorCode);
        }

        void retain() {
        }

        void release() {
        }

        void recycle() {
        }
    }

    /**
     * A request that reads data.
     */
    class ReadResponse extends Response {
        final ByteBuf data;

        ReadResponse(byte protocolVersion, int errorCode, long ledgerId, long entryId) {
            this(protocolVersion, errorCode, ledgerId, entryId, Unpooled.EMPTY_BUFFER);
        }

        ReadResponse(byte protocolVersion, int errorCode, long ledgerId, long entryId, ByteBuf data) {
            init(protocolVersion, READENTRY, errorCode, ledgerId, entryId);
            this.data = data;
        }

        boolean hasData() {
            return data.readableBytes() > 0;
        }

        ByteBuf getData() {
            return data;
        }

        @Override
        public void retain() {
            data.retain();
        }

        @Override
        public void release() {
            data.release();
        }
    }

    /**
     * A response that adds data.
     */
    class AddResponse extends Response {
        static AddResponse create(byte protocolVersion, int errorCode, long ledgerId, long entryId) {
            AddResponse response = RECYCLER.get();
            response.init(protocolVersion, ADDENTRY, errorCode, ledgerId, entryId);
            return response;
        }

        private final Handle<AddResponse> recyclerHandle;
        private AddResponse(Handle<AddResponse> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<AddResponse> RECYCLER = new Recycler<AddResponse>() {
            @Override
            protected AddResponse newObject(Handle<AddResponse> handle) {
                return new AddResponse(handle);
            }
        };

        @Override
        public void recycle() {
            recyclerHandle.recycle(this);
        }
    }

    /**
     * An error response.
     */
    class ErrorResponse extends Response {
        ErrorResponse(byte protocolVersion, byte opCode, int errorCode,
                      long ledgerId, long entryId) {
            init(protocolVersion, opCode, errorCode, ledgerId, entryId);
        }
    }

    /**
     * A response with an authentication message.
     */
    class AuthResponse extends Response {
        final AuthMessage authMessage;

        AuthResponse(byte protocolVersion, AuthMessage authMessage) {
            init(protocolVersion, AUTH, EOK, -1, -1);
            this.authMessage = authMessage;
        }

        AuthMessage getAuthMessage() {
            return authMessage;
        }
    }

}
