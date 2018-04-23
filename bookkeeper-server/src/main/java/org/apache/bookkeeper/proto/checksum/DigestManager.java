/**
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
package org.apache.bookkeeper.proto.checksum;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.security.GeneralSecurityException;

import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.util.ByteBufList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes an entry, attaches a digest to it and packages it with relevant
 * data so that it can be shipped to the bookie. On the return side, it also
 * gets a packet, checks that the digest matches, and extracts the original entry
 * for the packet. Currently 3 types of digests are supported: MAC (based on SHA-1) and CRC32 and CRC32C.
 */

public abstract class DigestManager {
    private static final Logger logger = LoggerFactory.getLogger(DigestManager.class);

    public static final int METADATA_LENGTH = 32;
    public static final int LAC_METADATA_LENGTH = 16;

    final long ledgerId;
    final boolean useV2Protocol;

    abstract int getMacCodeLength();

    void update(byte[] data) {
        update(Unpooled.wrappedBuffer(data, 0, data.length));
    }

    abstract void update(ByteBuf buffer);

    abstract void populateValueAndReset(ByteBuf buffer);

    final int macCodeLength;

    public DigestManager(long ledgerId, boolean useV2Protocol) {
        this.ledgerId = ledgerId;
        this.useV2Protocol = useV2Protocol;
        macCodeLength = getMacCodeLength();
    }

    public static DigestManager instantiate(long ledgerId, byte[] passwd, DigestType digestType)
            throws GeneralSecurityException {
        return instantiate(ledgerId, passwd, digestType, false);
    }

    public static DigestManager instantiate(long ledgerId, byte[] passwd, DigestType digestType,
            boolean useV2Protocol) throws GeneralSecurityException {
        switch(digestType) {
        case HMAC:
            return new MacDigestManager(ledgerId, passwd, useV2Protocol);
        case CRC32:
            return new CRC32DigestManager(ledgerId, useV2Protocol);
        case CRC32C:
            return new CRC32CDigestManager(ledgerId, useV2Protocol);
        case DUMMY:
            return new DummyDigestManager(ledgerId, useV2Protocol);
        default:
            throw new GeneralSecurityException("Unknown checksum type: " + digestType);
        }
    }

    /**
     * Computes the digest for an entry and put bytes together for sending.
     *
     * @param entryId
     * @param lastAddConfirmed
     * @param length
     * @param data
     * @return
     */
    public ByteBufList computeDigestAndPackageForSending(long entryId, long lastAddConfirmed, long length,
            ByteBuf data) {
        if (this.useV2Protocol) {
            /*
             * For V2 protocol, use pooled direct ByteBuf's to avoid object allocation in DigestManager.
             */
            ByteBuf headersBuffer = PooledByteBufAllocator.DEFAULT.buffer(METADATA_LENGTH + macCodeLength);
            headersBuffer.writeLong(ledgerId);
            headersBuffer.writeLong(entryId);
            headersBuffer.writeLong(lastAddConfirmed);
            headersBuffer.writeLong(length);

            update(headersBuffer);
            update(data);
            populateValueAndReset(headersBuffer);

            return ByteBufList.get(headersBuffer, data);
        } else {
            /*
             * For V3 protocol, use unpooled heap ByteBuf's (backed by accessible array): The one object
             * allocation here saves us later allocations when converting to protobuf ByteString.
             */
            ByteBuf sendBuffer = Unpooled.buffer(METADATA_LENGTH + macCodeLength + data.readableBytes());
            sendBuffer.writeLong(ledgerId);
            sendBuffer.writeLong(entryId);
            sendBuffer.writeLong(lastAddConfirmed);
            sendBuffer.writeLong(length);

            update(sendBuffer);
            update(data);
            populateValueAndReset(sendBuffer);

            sendBuffer.writeBytes(data, data.readerIndex(), data.readableBytes());

            return ByteBufList.get(sendBuffer);
        }
    }

    /**
     * Computes the digest for writeLac for sending.
     *
     * @param lac
     * @return
     */

    public ByteBufList computeDigestAndPackageForSendingLac(long lac) {
        ByteBuf headersBuffer;
        if (this.useV2Protocol) {
            headersBuffer = PooledByteBufAllocator.DEFAULT.buffer(LAC_METADATA_LENGTH + macCodeLength);
        } else {
            headersBuffer = Unpooled.buffer(LAC_METADATA_LENGTH + macCodeLength);
        }
        headersBuffer.writeLong(ledgerId);
        headersBuffer.writeLong(lac);

        update(headersBuffer);
        populateValueAndReset(headersBuffer);

        return ByteBufList.get(headersBuffer);
    }

    private void verifyDigest(ByteBuf dataReceived) throws BKDigestMatchException {
        verifyDigest(LedgerHandle.INVALID_ENTRY_ID, dataReceived, true);
    }

    private void verifyDigest(long entryId, ByteBuf dataReceived) throws BKDigestMatchException {
        verifyDigest(entryId, dataReceived, false);
    }

    private void verifyDigest(long entryId, ByteBuf dataReceived, boolean skipEntryIdCheck)
            throws BKDigestMatchException {

        if ((METADATA_LENGTH + macCodeLength) > dataReceived.readableBytes()) {
            logger.error("Data received is smaller than the minimum for this digest type. "
                    + " Either the packet it corrupt, or the wrong digest is configured. "
                    + " Digest type: {}, Packet Length: {}",
                    this.getClass().getName(), dataReceived.readableBytes());
            throw new BKDigestMatchException();
        }
        update(dataReceived.slice(0, METADATA_LENGTH));

        int offset = METADATA_LENGTH + macCodeLength;
        update(dataReceived.slice(offset, dataReceived.readableBytes() - offset));

        ByteBuf digest = PooledByteBufAllocator.DEFAULT.buffer(macCodeLength);
        populateValueAndReset(digest);

        try {
            if (digest.compareTo(dataReceived.slice(METADATA_LENGTH, macCodeLength)) != 0) {
                logger.error("Mac mismatch for ledger-id: " + ledgerId + ", entry-id: " + entryId);
                throw new BKDigestMatchException();
            }
        } finally {
            digest.release();
        }

        long actualLedgerId = dataReceived.readLong();
        long actualEntryId = dataReceived.readLong();

        if (actualLedgerId != ledgerId) {
            logger.error("Ledger-id mismatch in authenticated message, expected: " + ledgerId + " , actual: "
                         + actualLedgerId);
            throw new BKDigestMatchException();
        }

        if (!skipEntryIdCheck && actualEntryId != entryId) {
            logger.error("Entry-id mismatch in authenticated message, expected: " + entryId + " , actual: "
                         + actualEntryId);
            throw new BKDigestMatchException();
        }

    }

    public long verifyDigestAndReturnLac(ByteBuf dataReceived) throws BKDigestMatchException{
        if ((LAC_METADATA_LENGTH + macCodeLength) > dataReceived.readableBytes()) {
            logger.error("Data received is smaller than the minimum for this digest type."
                    + " Either the packet it corrupt, or the wrong digest is configured. "
                    + " Digest type: {}, Packet Length: {}",
                    this.getClass().getName(), dataReceived.readableBytes());
            throw new BKDigestMatchException();
        }

        update(dataReceived.slice(0, LAC_METADATA_LENGTH));

        ByteBuf digest = PooledByteBufAllocator.DEFAULT.buffer(macCodeLength);
        try {
            populateValueAndReset(digest);

            if (digest.compareTo(dataReceived.slice(LAC_METADATA_LENGTH, macCodeLength)) != 0) {
                logger.error("Mac mismatch for ledger-id LAC: " + ledgerId);
                throw new BKDigestMatchException();
            }
        } finally {
            digest.release();
        }

        long actualLedgerId = dataReceived.readLong();
        long lac = dataReceived.readLong();
        if (actualLedgerId != ledgerId) {
            logger.error("Ledger-id mismatch in authenticated message, expected: " + ledgerId + " , actual: "
                         + actualLedgerId);
            throw new BKDigestMatchException();
        }
        return lac;
    }

    /**
     * Verify that the digest matches and returns the data in the entry.
     *
     * @param entryId
     * @param dataReceived
     * @return
     * @throws BKDigestMatchException
     */
    public ByteBuf verifyDigestAndReturnData(long entryId, ByteBuf dataReceived)
            throws BKDigestMatchException {
        verifyDigest(entryId, dataReceived);
        dataReceived.readerIndex(METADATA_LENGTH + macCodeLength);
        return dataReceived;
    }

    /**
     * A representation of RecoveryData.
     */
    public static final class RecoveryData {
        final long lastAddConfirmed;
        final long length;

        public RecoveryData(long lastAddConfirmed, long length) {
            this.lastAddConfirmed = lastAddConfirmed;
            this.length = length;
        }

        public long getLastAddConfirmed() {
            return lastAddConfirmed;
        }

        public long getLength() {
            return length;
        }

    }

    public RecoveryData verifyDigestAndReturnLastConfirmed(ByteBuf dataReceived) throws BKDigestMatchException {
        verifyDigest(dataReceived);
        dataReceived.readerIndex(8);

        dataReceived.readLong(); // skip unused entryId
        long lastAddConfirmed = dataReceived.readLong();
        long length = dataReceived.readLong();
        return new RecoveryData(lastAddConfirmed, length);
    }
}
