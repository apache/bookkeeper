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
package org.apache.bookkeeper.proto.checksum;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.FastThreadLocal;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookieProtoEncoding;
import org.apache.bookkeeper.proto.BookieProtocol;
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
    private final ByteBufAllocator allocator;

    abstract int getMacCodeLength();

    abstract int update(int digest, ByteBuf buffer, int offset, int len);

    abstract void populateValueAndReset(int digest, ByteBuf buffer);

    abstract boolean isInt32Digest();

    final int macCodeLength;

    public DigestManager(long ledgerId, boolean useV2Protocol, ByteBufAllocator allocator) {
        this.ledgerId = ledgerId;
        this.useV2Protocol = useV2Protocol;
        this.macCodeLength = getMacCodeLength();
        this.allocator = allocator;
    }

    public static DigestManager instantiate(long ledgerId, byte[] passwd, DigestType digestType,
            ByteBufAllocator allocator, boolean useV2Protocol) throws GeneralSecurityException {
        switch(digestType) {
        case HMAC:
            return new MacDigestManager(ledgerId, passwd, useV2Protocol, allocator);
        case CRC32:
            return new CRC32DigestManager(ledgerId, useV2Protocol, allocator);
        case CRC32C:
            return new CRC32CDigestManager(ledgerId, useV2Protocol, allocator);
        case DUMMY:
            return new DummyDigestManager(ledgerId, useV2Protocol, allocator);
        default:
            throw new GeneralSecurityException("Unknown checksum type: " + digestType);
        }
    }

    public static byte[] generateMasterKey(byte[] password) throws NoSuchAlgorithmException {
        return password.length > 0 ? MacDigestManager.genDigest("ledger", password) : MacDigestManager.EMPTY_LEDGER_KEY;
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
    public ReferenceCounted computeDigestAndPackageForSending(long entryId, long lastAddConfirmed, long length,
                                                              ByteBuf data, byte[] masterKey, int flags) {
        if (this.useV2Protocol) {
            return computeDigestAndPackageForSendingV2(entryId, lastAddConfirmed, length, data, masterKey, flags);
        } else {
            return computeDigestAndPackageForSendingV3(entryId, lastAddConfirmed, length, data);
        }
    }

    private ReferenceCounted computeDigestAndPackageForSendingV2(long entryId, long lastAddConfirmed, long length,
                                                                 ByteBuf data, byte[] masterKey, int flags) {
        boolean isSmallEntry = data.readableBytes() < BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD;

        int headersSize = 4 // Request header
                        + BookieProtocol.MASTER_KEY_LENGTH // for the master key
                        + METADATA_LENGTH  //
                        + macCodeLength;
        int payloadSize = data.readableBytes();
        int bufferSize = 4 + headersSize + (isSmallEntry ? payloadSize : 0);

        ByteBuf buf = allocator.buffer(bufferSize, bufferSize);
        buf.writeInt(headersSize + payloadSize);
        buf.writeInt(
                BookieProtocol.PacketHeader.toInt(
                        BookieProtocol.CURRENT_PROTOCOL_VERSION, BookieProtocol.ADDENTRY, (short) flags));
        buf.writeBytes(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);

        // The checksum is computed on the next part of the buffer only
        buf.readerIndex(buf.writerIndex());
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeLong(lastAddConfirmed);
        buf.writeLong(length);

        // Compute checksum over the headers
        int digest = update(0, buf, buf.readerIndex(), buf.readableBytes());

        // don't unwrap slices
        final ByteBuf unwrapped = data.unwrap() != null && data.unwrap() instanceof CompositeByteBuf
                ? data.unwrap() : data;
        ReferenceCountUtil.retain(unwrapped);
        ReferenceCountUtil.safeRelease(data);

        if (unwrapped instanceof CompositeByteBuf) {
            CompositeByteBuf cbb = (CompositeByteBuf) unwrapped;
            for (int i = 0; i < cbb.numComponents(); i++) {
                ByteBuf b = cbb.component(i);
                digest = update(digest, b, b.readerIndex(), b.readableBytes());
            }
        } else {
            digest = update(digest, unwrapped, unwrapped.readerIndex(), unwrapped.readableBytes());
        }

        populateValueAndReset(digest, buf);

        // Reset the reader index to the beginning
        buf.readerIndex(0);

        if (isSmallEntry) {
            buf.writeBytes(unwrapped, unwrapped.readerIndex(), unwrapped.readableBytes());
            unwrapped.release();
            return buf;
        } else {
            return ByteBufList.get(buf, unwrapped);
        }
    }

    private ByteBufList computeDigestAndPackageForSendingV3(long entryId, long lastAddConfirmed, long length,
                                                            ByteBuf data) {
        ByteBuf headersBuffer = Unpooled.buffer(METADATA_LENGTH + macCodeLength);
        headersBuffer.writeLong(ledgerId);
        headersBuffer.writeLong(entryId);
        headersBuffer.writeLong(lastAddConfirmed);
        headersBuffer.writeLong(length);

        int digest = update(0, headersBuffer, 0, METADATA_LENGTH);

        // don't unwrap slices
        final ByteBuf unwrapped = data.unwrap() != null && data.unwrap() instanceof CompositeByteBuf
                ? data.unwrap() : data;
        ReferenceCountUtil.retain(unwrapped);
        ReferenceCountUtil.release(data);

        if (unwrapped instanceof CompositeByteBuf) {
            CompositeByteBuf cbb = ((CompositeByteBuf) unwrapped);
            for (int i = 0; i < cbb.numComponents(); i++) {
                ByteBuf b = cbb.component(i);
                digest = update(digest, b, b.readerIndex(), b.readableBytes());
            }
        } else {
            digest = update(digest, unwrapped, unwrapped.readerIndex(), unwrapped.readableBytes());
        }
        populateValueAndReset(digest, headersBuffer);

        return ByteBufList.get(headersBuffer, unwrapped);
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
            headersBuffer = allocator.buffer(LAC_METADATA_LENGTH + macCodeLength);
        } else {
            headersBuffer = Unpooled.buffer(LAC_METADATA_LENGTH + macCodeLength);
        }
        headersBuffer.writeLong(ledgerId);
        headersBuffer.writeLong(lac);

        int digest = update(0, headersBuffer, 0, LAC_METADATA_LENGTH);
        populateValueAndReset(digest, headersBuffer);

        return ByteBufList.get(headersBuffer);
    }

    private void verifyDigest(ByteBuf dataReceived) throws BKDigestMatchException {
        verifyDigest(LedgerHandle.INVALID_ENTRY_ID, dataReceived, true);
    }

    private void verifyDigest(long entryId, ByteBuf dataReceived) throws BKDigestMatchException {
        verifyDigest(entryId, dataReceived, false);
    }

    private static final FastThreadLocal<ByteBuf> DIGEST_BUFFER = new FastThreadLocal<ByteBuf>() {
        @Override
        protected ByteBuf initialValue() throws Exception {
            return PooledByteBufAllocator.DEFAULT.directBuffer(1024);
        }

        @Override
        protected void onRemoval(ByteBuf value) throws Exception {
            value.release();
        }
    };

    private void verifyDigest(long entryId, ByteBuf dataReceived, boolean skipEntryIdCheck)
            throws BKDigestMatchException {

        if ((METADATA_LENGTH + macCodeLength) > dataReceived.readableBytes()) {
            logger.error("Data received is smaller than the minimum for this digest type. "
                    + " Either the packet it corrupt, or the wrong digest is configured. "
                    + " Digest type: {}, Packet Length: {}",
                    this.getClass().getName(), dataReceived.readableBytes());
            throw new BKDigestMatchException();
        }
        int digest = update(0, dataReceived, 0, METADATA_LENGTH);

        int offset = METADATA_LENGTH + macCodeLength;
        digest = update(digest, dataReceived, offset, dataReceived.readableBytes() - offset);

        if (isInt32Digest()) {
            int receivedDigest = dataReceived.getInt(METADATA_LENGTH);
            if (receivedDigest != digest) {
                logger.error("Digest mismatch for ledger-id: " + ledgerId + ", entry-id: " + entryId);
                throw new BKDigestMatchException();
            }
        } else {
            ByteBuf digestBuf = DIGEST_BUFFER.get();
            digestBuf.clear();
            populateValueAndReset(digest, digestBuf);

            if (!ByteBufUtil.equals(digestBuf, 0, dataReceived, METADATA_LENGTH, macCodeLength)) {
                logger.error("Mac mismatch for ledger-id: " + ledgerId + ", entry-id: " + entryId);
                throw new BKDigestMatchException();
            }
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

        int digest = update(0, dataReceived, 0, LAC_METADATA_LENGTH);

        if (isInt32Digest()) {
            int receivedDigest = dataReceived.getInt(LAC_METADATA_LENGTH);
            if (receivedDigest != digest) {
                logger.error("Digest mismatch for ledger-id LAC: " + ledgerId);
                throw new BKDigestMatchException();
            }
        } else {
            ByteBuf digestBuf = DIGEST_BUFFER.get();
            digestBuf.clear();
            populateValueAndReset(digest, digestBuf);

            if (!ByteBufUtil.equals(digestBuf, 0, dataReceived, LAC_METADATA_LENGTH, macCodeLength)) {
                logger.error("Mac mismatch for ledger-id LAC: " + ledgerId);
                throw new BKDigestMatchException();
            }
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
