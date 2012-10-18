package org.apache.bookkeeper.proto;

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
    public static final byte LOWEST_COMPAT_PROTOCOL_VERSION = 0;

    /**
     * Current version of the protocol, which client will use. 
     */
    public static final byte CURRENT_PROTOCOL_VERSION = 2;

    /**
     * Entry Entry ID. To be used when no valid entry id can be assigned.
     */
    public static final long INVALID_ENTRY_ID = -1;

    /**
     * Entry identifier representing a request to obtain the last add entry confirmed
     */
    public static final long LAST_ADD_CONFIRMED = -1;

    /**
     * The length of the master key in add packets. This
     * is fixed at 20 for historic reasons. This is because it
     * is always generated using the MacDigestManager regardless
     * of whether Mac is being used for the digest or not
     */
    public static final int MASTER_KEY_LENGTH = 20;

    /** 
     * The first int of a packet is the header.
     * It contains the version, opCode and flags.
     * The initial versions of BK didn't have this structure
     * and just had an int representing the opCode as the 
     * first int. This handles that case also. 
     */
    static class PacketHeader {
        final byte version;
        final byte opCode;
        final short flags;

        public PacketHeader(byte version, byte opCode, short flags) {
            this.version = version;
            this.opCode = opCode;
            this.flags = flags;
        }
        
        int toInt() {
            if (version == 0) {
                return (int)opCode;
            } else {
                return ((version & 0xFF) << 24) 
                    | ((opCode & 0xFF) << 16)
                    | (flags & 0xFFFF);
            }
        }

        static PacketHeader fromInt(int i) {
            byte version = (byte)(i >> 24); 
            byte opCode = 0;
            short flags = 0;
            if (version == 0) {
                opCode = (byte)i;
            } else {
                opCode = (byte)((i >> 16) & 0xFF);
                flags = (short)(i & 0xFFFF);
            }
            return new PacketHeader(version, opCode, flags);
        }

        byte getVersion() {
            return version;
        }

        byte getOpCode() {
            return opCode;
        }

        short getFlags() {
            return flags;
        }
    }

    /**
     * The Add entry request payload will be a ledger entry exactly as it should
     * be logged. The response payload will be a 4-byte integer that has the
     * error code followed by the 8-byte ledger number and 8-byte entry number
     * of the entry written.
     */
    public static final byte ADDENTRY = 1;
    /**
     * The Read entry request payload will be the ledger number and entry number
     * to read. (The ledger number is an 8-byte integer and the entry number is
     * a 8-byte integer.) The response payload will be a 4-byte integer
     * representing an error code and a ledger entry if the error code is EOK,
     * otherwise it will be the 8-byte ledger number and the 4-byte entry number
     * requested. (Note that the first sixteen bytes of the entry happen to be
     * the ledger number and entry number as well.)
     */
    public static final byte READENTRY = 2;

    /**
     * The error code that indicates success
     */
    public static final int EOK = 0;
    /**
     * The error code that indicates that the ledger does not exist
     */
    public static final int ENOLEDGER = 1;
    /**
     * The error code that indicates that the requested entry does not exist
     */
    public static final int ENOENTRY = 2;
    /**
     * The error code that indicates an invalid request type
     */
    public static final int EBADREQ = 100;
    /**
     * General error occurred at the server
     */
    public static final int EIO = 101;

    /**
     * Unauthorized access to ledger
     */
    public static final int EUA = 102;

    /**
     * The server version is incompatible with the client
     */
    public static final int EBADVERSION = 103;

    /**
     * Attempt to write to fenced ledger
     */
    public static final int EFENCED = 104;

    /**
     * The server is running as read-only mode
     */
    public static final int EREADONLY = 105;

    public static final short FLAG_NONE = 0x0;
    public static final short FLAG_DO_FENCING = 0x0001;
    public static final short FLAG_RECOVERY_ADD = 0x0002;
}
