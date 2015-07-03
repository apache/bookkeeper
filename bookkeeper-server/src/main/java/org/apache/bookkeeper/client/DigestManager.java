package org.apache.bookkeeper.client;

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.security.GeneralSecurityException;

import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.util.DoubleByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes an entry, attaches a digest to it and packages it with relevant
 * data so that it can be shipped to the bookie. On the return side, it also
 * gets a packet, checks that the digest matches, and extracts the original entry
 * for the packet. Currently 2 types of digests are supported: MAC (based on SHA-1) and CRC32
 */

abstract class DigestManager {
    static final Logger logger = LoggerFactory.getLogger(DigestManager.class);

    static final int METADATA_LENGTH = 32;
    static final int LAC_METADATA_LENGTH = 16;

    long ledgerId;

    abstract int getMacCodeLength();

    void update(byte[] data) {
        update(Unpooled.wrappedBuffer(data, 0, data.length));
    }

    abstract void update(ByteBuf buffer);

    abstract void populateValueAndReset(ByteBuf buffer);

    final int macCodeLength;

    public DigestManager(long ledgerId) {
        this.ledgerId = ledgerId;
        macCodeLength = getMacCodeLength();
    }

    static DigestManager instantiate(long ledgerId, byte[] passwd, DigestType digestType) throws GeneralSecurityException {
        switch(digestType) {
        case MAC:
            return new MacDigestManager(ledgerId, passwd);
        case CRC32:
            return new CRC32DigestManager(ledgerId);
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

    public ByteBuf computeDigestAndPackageForSending(long entryId, long lastAddConfirmed, long length, byte[] data,
            int doffset, int dlength) {
        ByteBuf headersBuffer = PooledByteBufAllocator.DEFAULT.buffer(METADATA_LENGTH + macCodeLength);
        headersBuffer.writeLong(ledgerId);
        headersBuffer.writeLong(entryId);
        headersBuffer.writeLong(lastAddConfirmed);
        headersBuffer.writeLong(length);

        ByteBuf dataBuffer = Unpooled.wrappedBuffer(data, doffset, dlength);

        update(headersBuffer);
        update(dataBuffer);
        populateValueAndReset(headersBuffer);

        return DoubleByteBuf.get(headersBuffer, dataBuffer);
    }

    /**
     * Computes the digest for writeLac for sending.
     *
     * @param lac
     * @return
     */

    public ByteBuf computeDigestAndPackageForSendingLac(long lac) {
        ByteBuf headersBuffer = PooledByteBufAllocator.DEFAULT.buffer(LAC_METADATA_LENGTH + macCodeLength);
        headersBuffer.writeLong(ledgerId);
        headersBuffer.writeLong(lac);

        update(headersBuffer);
        populateValueAndReset(headersBuffer);

        return headersBuffer;
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

    long verifyDigestAndReturnLac(ByteBuf dataReceived) throws BKDigestMatchException{
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
    ByteBufInputStream verifyDigestAndReturnData(long entryId, ByteBuf dataReceived)
            throws BKDigestMatchException {
        verifyDigest(entryId, dataReceived);
        dataReceived.readerIndex(METADATA_LENGTH + macCodeLength);
        return new ByteBufInputStream(dataReceived);
    }

    static class RecoveryData {
        long lastAddConfirmed;
        long length;

        public RecoveryData(long lastAddConfirmed, long length) {
            this.lastAddConfirmed = lastAddConfirmed;
            this.length = length;
        }

    }

    RecoveryData verifyDigestAndReturnLastConfirmed(ByteBuf dataReceived) throws BKDigestMatchException {
        verifyDigest(dataReceived);
        dataReceived.readerIndex(8);

        dataReceived.readLong(); // skip unused entryId
        long lastAddConfirmed = dataReceived.readLong();
        long length = dataReceived.readLong();
        return new RecoveryData(lastAddConfirmed, length);
    }
}
