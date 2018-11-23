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
package org.apache.bookkeeper.client;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates all the ledger metadata that is persistently stored
 * in metadata store.
 *
 * <p>It provides parsing and serialization methods of such metadata.
 */
@EqualsAndHashCode
public class LedgerMetadata implements org.apache.bookkeeper.client.api.LedgerMetadata {
    static final Logger LOG = LoggerFactory.getLogger(LedgerMetadata.class);

    private static final String closed = "CLOSED";
    private static final String lSplitter = "\n";
    private static final String tSplitter = "\t";

    // can't use -1 for NOTCLOSED because that is reserved for a closed, empty
    // ledger
    private static final int NOTCLOSED = -101;
    private static final int IN_RECOVERY = -102;

    public static final int LOWEST_COMPAT_METADATA_FORMAT_VERSION = 0;
    public static final int CURRENT_METADATA_FORMAT_VERSION = 2;
    public static final String VERSION_KEY = "BookieMetadataFormatVersion";

    private final int metadataFormatVersion;
    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;

    private final LedgerMetadataFormat.State state;
    private final long length;
    private final long lastEntryId;
    private final long ctime;
    final boolean storeCtime; // non-private so builder can access for copy

    private final NavigableMap<Long, ImmutableList<BookieSocketAddress>> ensembles;
    private final ImmutableList<BookieSocketAddress> currentEnsemble;

    private final boolean hasPassword; // IKTODO other things should be optionals instead
    private final LedgerMetadataFormat.DigestType digestType;
    private final byte[] password;

    private Map<String, byte[]> customMetadata = Maps.newHashMap();

    LedgerMetadata(int metadataFormatVersion,
                   int ensembleSize,
                   int writeQuorumSize,
                   int ackQuorumSize,
                   LedgerMetadataFormat.State state,
                   Optional<Long> lastEntryId,
                   Optional<Long> length,
                   Map<Long, List<BookieSocketAddress>> ensembles,
                   DigestType digestType,
                   Optional<byte[]> password,
                   long ctime,
                   boolean storeCtime,
                   Map<String, byte[]> customMetadata) {
        checkArgument(ensembles.size() > 0, "There must be at least one ensemble in the ledger");
        if (state == LedgerMetadataFormat.State.CLOSED) {
            checkArgument(length.isPresent(), "Closed ledger must have a length");
            checkArgument(lastEntryId.isPresent(), "Closed ledger must have a last entry");
        } else {
            checkArgument(!length.isPresent(), "Non-closed ledger must not have a length");
            checkArgument(!lastEntryId.isPresent(), "Non-closed ledger must not have a last entry");
        }

        this.metadataFormatVersion = metadataFormatVersion;
        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.state = state;

        this.lastEntryId = lastEntryId.orElse(LedgerHandle.INVALID_ENTRY_ID);
        this.length = length.orElse(0L);

        this.ensembles = Collections.unmodifiableNavigableMap(
                ensembles.entrySet().stream().collect(TreeMap::new,
                                                      (m, e) -> m.put(e.getKey(),
                                                                      ImmutableList.copyOf(e.getValue())),
                                                      TreeMap::putAll));

        if (state != LedgerMetadataFormat.State.CLOSED) {
            currentEnsemble = this.ensembles.lastEntry().getValue();
        } else {
            currentEnsemble = null;
        }

        this.digestType = digestType.equals(DigestType.MAC)
            ? LedgerMetadataFormat.DigestType.HMAC : LedgerMetadataFormat.DigestType.valueOf(digestType.toString());

        if (password.isPresent()) {
            this.password = password.get();
            this.hasPassword = true;
        } else {
            this.password = null;
            this.hasPassword = false;
        }
        this.ctime = ctime;
        this.storeCtime = storeCtime;

        this.customMetadata.putAll(customMetadata);
    }

    @Override
    public NavigableMap<Long, ? extends List<BookieSocketAddress>> getAllEnsembles() {
        return ensembles;
    }

    @Override
    public int getEnsembleSize() {
        return ensembleSize;
    }

    @Override
    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    @Override
    public int getAckQuorumSize() {
        return ackQuorumSize;
    }

    @Override
    public long getCtime() {
        return ctime;
    }

    /**
     * In versions 4.1.0 and below, the digest type and password were not
     * stored in the metadata.
     *
     * @return whether the password has been stored in the metadata
     */
    boolean hasPassword() {
        return hasPassword;
    }

    @VisibleForTesting
    public byte[] getPassword() {
        if (!hasPassword()) {
            return new byte[0];
        } else {
            return Arrays.copyOf(password, password.length);
        }
    }

    @Override
    public DigestType getDigestType() {
        switch (digestType) {
            case HMAC:
                return DigestType.MAC;
            case CRC32:
                return DigestType.CRC32;
            case CRC32C:
                return DigestType.CRC32C;
            case DUMMY:
                return DigestType.DUMMY;
            default:
                throw new IllegalArgumentException("Unable to convert digest type " + digestType);
        }
    }

    @Override
    public long getLastEntryId() {
        return lastEntryId;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public boolean isClosed() {
        return state == LedgerMetadataFormat.State.CLOSED;
    }

    public boolean isInRecovery() {
        return state == LedgerMetadataFormat.State.IN_RECOVERY;
    }

    public LedgerMetadataFormat.State getState() {
        return state;
    }

    List<BookieSocketAddress> getCurrentEnsemble() {
        return currentEnsemble;
    }

    List<BookieSocketAddress> getEnsemble(long entryId) {
        // the head map cannot be empty, since we insert an ensemble for
        // entry-id 0, right when we start
        return ensembles.get(ensembles.headMap(entryId + 1).lastKey());
    }

    @Override
    public List<BookieSocketAddress> getEnsembleAt(long entryId) {
        return getEnsemble(entryId);
    }

    /**
     * the entry id greater than the given entry-id at which the next ensemble change takes
     * place.
     *
     * @param entryId
     * @return the entry id of the next ensemble change (-1 if no further ensemble changes)
     */
    long getNextEnsembleChange(long entryId) {
        SortedMap<Long, ? extends List<BookieSocketAddress>> tailMap = ensembles.tailMap(entryId + 1);

        if (tailMap.isEmpty()) {
            return -1;
        } else {
            return tailMap.firstKey();
        }
    }

    @Override
    public Map<String, byte[]> getCustomMetadata() {
        return this.customMetadata;
    }

    void setCustomMetadata(Map<String, byte[]> customMetadata) {
        this.customMetadata = customMetadata;
    }

    LedgerMetadataFormat buildProtoFormat() {
        return buildProtoFormat(true);
    }

    LedgerMetadataFormat buildProtoFormat(boolean withPassword) {
        LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();
        builder.setQuorumSize(writeQuorumSize).setAckQuorumSize(ackQuorumSize)
            .setEnsembleSize(ensembleSize).setLength(length)
            .setState(state).setLastEntryId(lastEntryId);

        if (storeCtime) {
            builder.setCtime(ctime);
        }

        if (hasPassword) {
            builder.setDigestType(digestType);
            if (withPassword) {
                builder.setPassword(ByteString.copyFrom(password));
            }
        }

        if (customMetadata != null) {
            LedgerMetadataFormat.cMetadataMapEntry.Builder cMetadataBuilder =
                LedgerMetadataFormat.cMetadataMapEntry.newBuilder();
            for (Map.Entry<String, byte[]> entry : customMetadata.entrySet()) {
                cMetadataBuilder.setKey(entry.getKey()).setValue(ByteString.copyFrom(entry.getValue()));
                builder.addCustomMetadata(cMetadataBuilder.build());
            }
        }

        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> entry : ensembles.entrySet()) {
            LedgerMetadataFormat.Segment.Builder segmentBuilder = LedgerMetadataFormat.Segment.newBuilder();
            segmentBuilder.setFirstEntryId(entry.getKey());
            for (BookieSocketAddress addr : entry.getValue()) {
                segmentBuilder.addEnsembleMember(addr.toString());
            }
            builder.addSegment(segmentBuilder.build());
        }
        return builder.build();
    }

    /**
     * Generates a byte array of this object.
     *
     * @return the metadata serialized into a byte array
     */
    public byte[] serialize() {
        return serialize(true);
    }

    public byte[] serialize(boolean withPassword) {
        if (metadataFormatVersion == 1) {
            return serializeVersion1();
        }

        StringBuilder s = new StringBuilder();
        s.append(VERSION_KEY).append(tSplitter).append(CURRENT_METADATA_FORMAT_VERSION).append(lSplitter);
        s.append(TextFormat.printToString(buildProtoFormat(withPassword)));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Serialized config: {}", s);
        }
        return s.toString().getBytes(UTF_8);
    }

    private byte[] serializeVersion1() {
        StringBuilder s = new StringBuilder();
        s.append(VERSION_KEY).append(tSplitter).append(metadataFormatVersion).append(lSplitter);
        s.append(writeQuorumSize).append(lSplitter).append(ensembleSize).append(lSplitter).append(length);

        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> entry : ensembles.entrySet()) {
            s.append(lSplitter).append(entry.getKey());
            for (BookieSocketAddress addr : entry.getValue()) {
                s.append(tSplitter);
                s.append(addr.toString());
            }
        }

        if (state == LedgerMetadataFormat.State.IN_RECOVERY) {
            s.append(lSplitter).append(IN_RECOVERY).append(tSplitter).append(closed);
        } else if (state == LedgerMetadataFormat.State.CLOSED) {
            s.append(lSplitter).append(getLastEntryId()).append(tSplitter).append(closed);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Serialized config: {}", s);
        }

        return s.toString().getBytes(UTF_8);
    }

    /**
     * Parses a given byte array and transforms into a LedgerConfig object.
     *
     * @param bytes
     *            byte array to parse
     * @param metadataStoreCtime
     *            metadata store creation time, used for legacy ledgers
     * @return LedgerConfig
     * @throws IOException
     *             if the given byte[] cannot be parsed
     */
    public static LedgerMetadata parseConfig(byte[] bytes,
                                             Optional<Long> metadataStoreCtime) throws IOException {
        String config = new String(bytes, UTF_8);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parsing Config: {}", config);
        }
        BufferedReader reader = new BufferedReader(new StringReader(config));
        String versionLine = reader.readLine();
        if (versionLine == null) {
            throw new IOException("Invalid metadata. Content missing");
        }
        final int metadataFormatVersion;
        if (versionLine.startsWith(VERSION_KEY)) {
            String parts[] = versionLine.split(tSplitter);
            metadataFormatVersion = Integer.parseInt(parts[1]);
        } else {
            // if no version is set, take it to be version 1
            // as the parsing is the same as what we had before
            // we introduce versions
            metadataFormatVersion = 1;
            // reset the reader
            reader.close();
            reader = new BufferedReader(new StringReader(config));
        }

        if (metadataFormatVersion < LOWEST_COMPAT_METADATA_FORMAT_VERSION
            || metadataFormatVersion > CURRENT_METADATA_FORMAT_VERSION) {
            throw new IOException(
                    String.format("Metadata version not compatible. Expected between %d and %d, but got %d",
                                  LOWEST_COMPAT_METADATA_FORMAT_VERSION, CURRENT_METADATA_FORMAT_VERSION,
                                  metadataFormatVersion));
        }

        if (metadataFormatVersion == 1) {
            return parseVersion1Config(reader);
        }

        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create()
            .withMetadataFormatVersion(metadataFormatVersion);

        // remaining size is total minus the length of the version line and '\n'
        char[] configBuffer = new char[config.length() - (versionLine.length() + 1)];
        if (configBuffer.length != reader.read(configBuffer, 0, configBuffer.length)) {
            throw new IOException("Invalid metadata buffer");
        }

        LedgerMetadataFormat.Builder formatBuilder = LedgerMetadataFormat.newBuilder();
        TextFormat.merge((CharSequence) CharBuffer.wrap(configBuffer), formatBuilder);
        LedgerMetadataFormat data = formatBuilder.build();

        builder.withEnsembleSize(data.getEnsembleSize());
        builder.withWriteQuorumSize(data.getQuorumSize());
        if (data.hasAckQuorumSize()) {
            builder.withAckQuorumSize(data.getAckQuorumSize());
        } else {
            builder.withAckQuorumSize(data.getQuorumSize());
        }

        if (data.hasCtime()) {
            builder.withCreationTime(data.getCtime()).storingCreationTime(true);
        } else if (metadataStoreCtime.isPresent()) {
            builder.withCreationTime(metadataStoreCtime.get()).storingCreationTime(false);
        }

        if (data.getState() == LedgerMetadataFormat.State.IN_RECOVERY) {
            builder.withInRecoveryState();
        } else if (data.getState() == LedgerMetadataFormat.State.CLOSED) {
            builder.closingAt(data.getLastEntryId(), data.getLength());
        }

        if (data.hasPassword()) {
            builder.withPassword(data.getPassword().toByteArray())
                .withDigestType(protoToApiDigestType(data.getDigestType()));
        }

        for (LedgerMetadataFormat.Segment s : data.getSegmentList()) {
            List<BookieSocketAddress> addrs = new ArrayList<>();
            for (String addr : s.getEnsembleMemberList()) {
                addrs.add(new BookieSocketAddress(addr));
            }
            builder.newEnsembleEntry(s.getFirstEntryId(), addrs);
        }

        if (data.getCustomMetadataCount() > 0) {
            builder.withCustomMetadata(data.getCustomMetadataList().stream().collect(
                                               Collectors.toMap(e -> e.getKey(),
                                                                e -> e.getValue().toByteArray())));
        }
        return builder.build();
    }

    static LedgerMetadata parseVersion1Config(BufferedReader reader) throws IOException {
        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create().withMetadataFormatVersion(1);
        try {
            int quorumSize = Integer.parseInt(reader.readLine());
            int ensembleSize = Integer.parseInt(reader.readLine());
            long length = Long.parseLong(reader.readLine());

            builder.withEnsembleSize(ensembleSize).withWriteQuorumSize(quorumSize).withAckQuorumSize(quorumSize);

            String line = reader.readLine();
            while (line != null) {
                String parts[] = line.split(tSplitter);

                if (parts[1].equals(closed)) {
                    Long l = Long.parseLong(parts[0]);
                    if (l == IN_RECOVERY) {
                        builder.withInRecoveryState();
                    } else {
                        builder.closingAt(l, length);
                    }
                    break;
                }

                ArrayList<BookieSocketAddress> addrs = new ArrayList<BookieSocketAddress>();
                for (int j = 1; j < parts.length; j++) {
                    addrs.add(new BookieSocketAddress(parts[j]));
                }
                builder.newEnsembleEntry(Long.parseLong(parts[0]), addrs);

                line = reader.readLine();
            }
            return builder.build();
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String toString() {
        return toStringRepresentation(true);
    }

    /**
     * Returns a string representation of this LedgerMetadata object by
     * filtering out the password field.
     *
     * @return a string representation of the object without password field in
     *         it.
     */
    public String toSafeString() {
        return toStringRepresentation(false);
    }

    private String toStringRepresentation(boolean withPassword) {
        StringBuilder sb = new StringBuilder();
        sb.append("(meta:").append(new String(serialize(withPassword), UTF_8)).append(")");
        return sb.toString();
    }

    Set<BookieSocketAddress> getBookiesInThisLedger() {
        Set<BookieSocketAddress> bookies = new HashSet<BookieSocketAddress>();
        for (List<BookieSocketAddress> ensemble : ensembles.values()) {
            bookies.addAll(ensemble);
        }
        return bookies;
    }

    List<BookieSocketAddress> getLastEnsembleValue() {
        checkState(!ensembles.isEmpty(), "Metadata should never be created with no ensembles");
        return ensembles.lastEntry().getValue();
    }

    Long getLastEnsembleKey() {
        checkState(!ensembles.isEmpty(), "Metadata should never be created with no ensembles");
        return ensembles.lastKey();
    }

    int getMetadataFormatVersion() {
        return metadataFormatVersion;
    }

    private static DigestType protoToApiDigestType(LedgerMetadataFormat.DigestType digestType) {
        switch (digestType) {
        case HMAC:
            return DigestType.MAC;
        case CRC32:
            return DigestType.CRC32;
        case CRC32C:
            return DigestType.CRC32C;
        case DUMMY:
            return DigestType.DUMMY;
        default:
            throw new IllegalArgumentException("Unable to convert digest type " + digestType);
        }
    }
}
