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
import com.google.common.base.Optional;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import lombok.EqualsAndHashCode;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.apache.bookkeeper.versioning.Version;
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

    private int metadataFormatVersion = 0;

    private int ensembleSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private long length;
    private long lastEntryId;
    private long ctime;
    boolean storeSystemtimeAsLedgerCreationTime = false;

    private LedgerMetadataFormat.State state;
    private TreeMap<Long, ImmutableList<BookieSocketAddress>> ensembles =  new TreeMap<>();
    private List<BookieSocketAddress> currentEnsemble;
    volatile Version version = Version.NEW;

    private boolean hasPassword = false;
    private LedgerMetadataFormat.DigestType digestType;
    private byte[] password;

    private Map<String, byte[]> customMetadata = Maps.newHashMap();

    public LedgerMetadata(int ensembleSize,
                          int writeQuorumSize,
                          int ackQuorumSize,
                          BookKeeper.DigestType digestType,
                          byte[] password,
                          Map<String, byte[]> customMetadata,
                          boolean storeSystemtimeAsLedgerCreationTime) {
        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        if (storeSystemtimeAsLedgerCreationTime) {
            this.ctime = System.currentTimeMillis();
        } else {
            // if client disables storing its system time as ledger creation time, there should be no ctime at this
            // moment.
            this.ctime = -1L;
        }
        this.storeSystemtimeAsLedgerCreationTime = storeSystemtimeAsLedgerCreationTime;

        /*
         * It is set in PendingReadOp.readEntryComplete, and
         * we read it in LedgerRecoveryOp.readComplete.
         */
        this.length = 0;
        this.state = LedgerMetadataFormat.State.OPEN;
        this.lastEntryId = LedgerHandle.INVALID_ENTRY_ID;
        this.metadataFormatVersion = CURRENT_METADATA_FORMAT_VERSION;

        this.digestType = digestType.equals(BookKeeper.DigestType.MAC)
            ? LedgerMetadataFormat.DigestType.HMAC : LedgerMetadataFormat.DigestType.valueOf(digestType.toString());
        this.password = Arrays.copyOf(password, password.length);
        this.hasPassword = true;
        if (customMetadata != null) {
            this.customMetadata = customMetadata;
        }
    }

    LedgerMetadata(int ensembleSize,
                   int writeQuorumSize,
                   int ackQuorumSize,
                   LedgerMetadataFormat.State state,
                   java.util.Optional<Long> lastEntryId,
                   java.util.Optional<Long> length,
                   Map<Long, List<BookieSocketAddress>> ensembles,
                   DigestType digestType,
                   java.util.Optional<byte[]> password,
                   java.util.Optional<Long> ctime,
                   Map<String, byte[]> customMetadata,
                   Version version) {
        checkArgument(ensembles.size() > 0, "There must be at least one ensemble in the ledger");

        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.state = state;
        if (lastEntryId.isPresent()) {
            this.lastEntryId = lastEntryId.get();
        } else {
            this.lastEntryId = LedgerHandle.INVALID_ENTRY_ID;
        }
        length.ifPresent((l) -> this.length = l);
        setEnsembles(ensembles);

        if (state != LedgerMetadataFormat.State.CLOSED) {
            currentEnsemble = this.ensembles.lastEntry().getValue();
        }

        this.digestType = digestType.equals(DigestType.MAC)
            ? LedgerMetadataFormat.DigestType.HMAC : LedgerMetadataFormat.DigestType.valueOf(digestType.toString());

        password.ifPresent((pw) -> {
                this.password = pw;
                this.hasPassword = true;
            });

        ctime.ifPresent((c) -> {
                this.ctime = c;
                this.storeSystemtimeAsLedgerCreationTime = true;
            });

        this.customMetadata.putAll(customMetadata);
        this.version = version;
    }

    /**
     * Used for testing purpose only.
     */
    @VisibleForTesting
    public LedgerMetadata(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            BookKeeper.DigestType digestType, byte[] password) {
        this(ensembleSize, writeQuorumSize, ackQuorumSize, digestType, password, null, false);
    }

    /**
     * Copy Constructor.
     */
    LedgerMetadata(LedgerMetadata other) {
        this.ensembleSize = other.ensembleSize;
        this.writeQuorumSize = other.writeQuorumSize;
        this.ackQuorumSize = other.ackQuorumSize;
        this.length = other.length;
        this.lastEntryId = other.lastEntryId;
        this.metadataFormatVersion = other.metadataFormatVersion;
        this.state = other.state;
        this.version = other.version;
        this.hasPassword = other.hasPassword;
        this.digestType = other.digestType;
        this.ctime = other.ctime;
        this.storeSystemtimeAsLedgerCreationTime = other.storeSystemtimeAsLedgerCreationTime;
        this.password = new byte[other.password.length];
        System.arraycopy(other.password, 0, this.password, 0, other.password.length);
        // copy the ensembles
        for (Entry<Long, ? extends List<BookieSocketAddress>> entry : other.ensembles.entrySet()) {
            this.addEnsemble(entry.getKey(), entry.getValue());
        }
        this.customMetadata = other.customMetadata;
    }

    private LedgerMetadata() {
        this(0, 0, 0, BookKeeper.DigestType.MAC, new byte[] {});
        this.hasPassword = false;
    }

    /**
     * Get the Map of bookie ensembles for the various ledger fragments
     * that make up the ledger.
     *
     * @return SortedMap of Ledger Fragments and the corresponding
     * bookie ensembles that store the entries.
     */
    public TreeMap<Long, ? extends List<BookieSocketAddress>> getEnsembles() {
        return ensembles;
    }

    @Override
    public NavigableMap<Long, ? extends List<BookieSocketAddress>> getAllEnsembles() {
        return ensembles;
    }

    void setEnsembles(Map<Long, ? extends List<BookieSocketAddress>> newEnsembles) {
        this.ensembles = newEnsembles.entrySet().stream()
            .collect(TreeMap::new,
                     (m, e) -> m.put(e.getKey(), ImmutableList.copyOf(e.getValue())),
                     TreeMap::putAll);
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

    @VisibleForTesting
    void setCtime(long ctime) {
        this.ctime = ctime;
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
        return Arrays.copyOf(password, password.length);
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

    void setLength(long length) {
        this.length = length;
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

    void setState(LedgerMetadataFormat.State state) {
        this.state = state;
    }

    void markLedgerInRecovery() {
        state = LedgerMetadataFormat.State.IN_RECOVERY;
    }

    void close(long entryId) {
        lastEntryId = entryId;
        state = LedgerMetadataFormat.State.CLOSED;
    }

    public void addEnsemble(long startEntryId, List<BookieSocketAddress> ensemble) {
        checkArgument(ensembles.isEmpty() || startEntryId >= ensembles.lastKey());

        ensembles.put(startEntryId, ImmutableList.copyOf(ensemble));
        currentEnsemble = ensemble;
    }

    List<BookieSocketAddress> getCurrentEnsemble() {
        return currentEnsemble;
    }

    public void updateEnsemble(long startEntryId, List<BookieSocketAddress> ensemble) {
        checkArgument(ensembles.containsKey(startEntryId));
        ensembles.put(startEntryId, ImmutableList.copyOf(ensemble));
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

        if (storeSystemtimeAsLedgerCreationTime) {
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
     * @param version
     *            version of the ledger metadata
     * @param msCtime
     *            metadata store creation time, used for legacy ledgers
     * @return LedgerConfig
     * @throws IOException
     *             if the given byte[] cannot be parsed
     */
    public static LedgerMetadata parseConfig(byte[] bytes, Version version, Optional<Long> msCtime) throws IOException {
        LedgerMetadata lc = new LedgerMetadata();
        lc.version = version;

        String config = new String(bytes, UTF_8);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parsing Config: {}", config);
        }
        BufferedReader reader = new BufferedReader(new StringReader(config));
        String versionLine = reader.readLine();
        if (versionLine == null) {
            throw new IOException("Invalid metadata. Content missing");
        }
        if (versionLine.startsWith(VERSION_KEY)) {
            String parts[] = versionLine.split(tSplitter);
            lc.metadataFormatVersion = Integer.parseInt(parts[1]);
        } else {
            // if no version is set, take it to be version 1
            // as the parsing is the same as what we had before
            // we introduce versions
            lc.metadataFormatVersion = 1;
            // reset the reader
            reader.close();
            reader = new BufferedReader(new StringReader(config));
        }

        if (lc.metadataFormatVersion < LOWEST_COMPAT_METADATA_FORMAT_VERSION
            || lc.metadataFormatVersion > CURRENT_METADATA_FORMAT_VERSION) {
            throw new IOException("Metadata version not compatible. Expected between "
                    + LOWEST_COMPAT_METADATA_FORMAT_VERSION + " and " + CURRENT_METADATA_FORMAT_VERSION
                                  + ", but got " + lc.metadataFormatVersion);
        }

        if (lc.metadataFormatVersion == 1) {
            return parseVersion1Config(lc, reader);
        }

        // remaining size is total minus the length of the version line and '\n'
        char[] configBuffer = new char[config.length() - (versionLine.length() + 1)];
        if (configBuffer.length != reader.read(configBuffer, 0, configBuffer.length)) {
            throw new IOException("Invalid metadata buffer");
        }

        LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();

        TextFormat.merge((CharSequence) CharBuffer.wrap(configBuffer), builder);
        LedgerMetadataFormat data = builder.build();
        lc.writeQuorumSize = data.getQuorumSize();
        if (data.hasCtime()) {
            lc.ctime = data.getCtime();
            lc.storeSystemtimeAsLedgerCreationTime = true;
        } else if (msCtime.isPresent()) {
            lc.ctime = msCtime.get();
            lc.storeSystemtimeAsLedgerCreationTime = false;
        }
        if (data.hasAckQuorumSize()) {
            lc.ackQuorumSize = data.getAckQuorumSize();
        } else {
            lc.ackQuorumSize = lc.writeQuorumSize;
        }

        lc.ensembleSize = data.getEnsembleSize();
        lc.length = data.getLength();
        lc.state = data.getState();
        lc.lastEntryId = data.getLastEntryId();

        if (data.hasPassword()) {
            lc.digestType = data.getDigestType();
            lc.password = data.getPassword().toByteArray();
            lc.hasPassword = true;
        }

        for (LedgerMetadataFormat.Segment s : data.getSegmentList()) {
            ArrayList<BookieSocketAddress> addrs = new ArrayList<BookieSocketAddress>();
            for (String member : s.getEnsembleMemberList()) {
                addrs.add(new BookieSocketAddress(member));
            }
            lc.addEnsemble(s.getFirstEntryId(), addrs);
        }

        if (data.getCustomMetadataCount() > 0) {
            List<LedgerMetadataFormat.cMetadataMapEntry> cMetadataList = data.getCustomMetadataList();
            lc.customMetadata = Maps.newHashMap();
            for (LedgerMetadataFormat.cMetadataMapEntry ent : cMetadataList) {
                lc.customMetadata.put(ent.getKey(), ent.getValue().toByteArray());
            }
        }
        return lc;
    }

    static LedgerMetadata parseVersion1Config(LedgerMetadata lc,
                                              BufferedReader reader) throws IOException {
        try {
            lc.writeQuorumSize = lc.ackQuorumSize = Integer.parseInt(reader.readLine());
            lc.ensembleSize = Integer.parseInt(reader.readLine());
            lc.length = Long.parseLong(reader.readLine());

            String line = reader.readLine();
            while (line != null) {
                String parts[] = line.split(tSplitter);

                if (parts[1].equals(closed)) {
                    Long l = Long.parseLong(parts[0]);
                    if (l == IN_RECOVERY) {
                        lc.state = LedgerMetadataFormat.State.IN_RECOVERY;
                    } else {
                        lc.state = LedgerMetadataFormat.State.CLOSED;
                        lc.lastEntryId = l;
                    }
                    break;
                } else {
                    lc.state = LedgerMetadataFormat.State.OPEN;
                }

                ArrayList<BookieSocketAddress> addrs = new ArrayList<BookieSocketAddress>();
                for (int j = 1; j < parts.length; j++) {
                    addrs.add(new BookieSocketAddress(parts[j]));
                }
                lc.addEnsemble(Long.parseLong(parts[0]), addrs);
                line = reader.readLine();
            }
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
        return lc;
    }

    /**
     * Updates the version of this metadata.
     *
     * @param v Version
     */
    public void setVersion(Version v) {
        this.version = v;
    }

    /**
     * Returns the last version.
     *
     * @return version
     */
    public Version getVersion() {
        return this.version;
    }

    /**
     * Is the metadata newer than given <i>newMeta</i>.
     *
     * @param newMeta the metadata to compare
     * @return true if <i>this</i> is newer than <i>newMeta</i>, false otherwise
     */
    boolean isNewerThan(LedgerMetadata newMeta) {
        if (null == version) {
            return false;
        }
        return Version.Occurred.AFTER == version.compare(newMeta.version);
    }

    /**
     * Routine to compare two {@code Map<String, byte[]>}; Since the values in the map are {@code byte[]}, we can't use
     * {@code Map.equals}.
     * @param first
     *          The first map
     * @param second
     *          The second map to compare with
     * @return true if the 2 maps contain the exact set of {@code <K,V>} pairs.
     */
    public static boolean areByteArrayValMapsEqual(Map<String, byte[]> first, Map<String, byte[]> second) {
        if (first == null && second == null) {
            return true;
        }

        // above check confirms that both are not null;
        // if one is null the other isn't; so they must
        // be different
        if (first == null || second == null) {
            return false;
        }

        if (first.size() != second.size()) {
            return false;
        }
        for (Map.Entry<String, byte[]> entry : first.entrySet()) {
            if (!Arrays.equals(entry.getValue(), second.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Is the metadata conflict with new updated metadata.
     *
     * @param newMeta
     *          Re-read metadata
     * @return true if the metadata is conflict.
     */
    boolean isConflictWith(LedgerMetadata newMeta) {
        /*
         *  if length & close have changed, then another client has
         *  opened the ledger, can't resolve this conflict.
         */

        if (metadataFormatVersion != newMeta.metadataFormatVersion
            || ensembleSize != newMeta.ensembleSize
            || writeQuorumSize != newMeta.writeQuorumSize
            || ackQuorumSize != newMeta.ackQuorumSize
            || length != newMeta.length
            || state != newMeta.state
            || !digestType.equals(newMeta.digestType)
            || !Arrays.equals(password, newMeta.password)
            || !LedgerMetadata.areByteArrayValMapsEqual(customMetadata, newMeta.customMetadata)) {
            return true;
        }

        // verify the ctime
        if (storeSystemtimeAsLedgerCreationTime != newMeta.storeSystemtimeAsLedgerCreationTime) {
            return true;
        } else if (storeSystemtimeAsLedgerCreationTime) {
            return ctime != newMeta.ctime;
        }

        if (state == LedgerMetadataFormat.State.CLOSED
            && lastEntryId != newMeta.lastEntryId) {
            return true;
        }
        // if ledger is closed, we can just take the new ensembles
        if (newMeta.state != LedgerMetadataFormat.State.CLOSED) {
            // allow new metadata to be one ensemble less than current metadata
            // since ensemble change might kick in when recovery changed metadata
            int diff = ensembles.size() - newMeta.ensembles.size();
            if (0 != diff && 1 != diff) {
                return true;
            }
            // ensemble distribution should be same
            // we don't check the detail ensemble, since new bookie will be set
            // using recovery tool.
            Iterator<Long> keyIter = ensembles.keySet().iterator();
            Iterator<Long> newMetaKeyIter = newMeta.ensembles.keySet().iterator();
            for (int i = 0; i < newMeta.ensembles.size(); i++) {
                Long curKey = keyIter.next();
                Long newMetaKey = newMetaKeyIter.next();
                if (!curKey.equals(newMetaKey)) {
                    return true;
                }
            }
        }
        return false;
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
        sb.append("(meta:").append(new String(serialize(withPassword), UTF_8)).append(", version:").append(version)
                .append(")");
        return sb.toString();
    }

    void mergeEnsembles(SortedMap<Long, ? extends List<BookieSocketAddress>> newEnsembles) {
        // allow new metadata to be one ensemble less than current metadata
        // since ensemble change might kick in when recovery changed metadata
        int diff = ensembles.size() - newEnsembles.size();
        if (0 != diff && 1 != diff) {
            return;
        }
        int i = 0;
        for (Entry<Long, ? extends List<BookieSocketAddress>> entry : newEnsembles.entrySet()) {
            ++i;
            if (ensembles.size() != i) {
                // we should use last ensemble from current metadata
                // not the new metadata read from zookeeper
                long key = entry.getKey();
                List<BookieSocketAddress> ensemble = entry.getValue();
                ensembles.put(key, ImmutableList.copyOf(ensemble));
            }
        }
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
}
