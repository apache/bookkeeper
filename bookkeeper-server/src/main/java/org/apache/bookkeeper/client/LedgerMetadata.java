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

import java.io.BufferedReader;
import java.io.StringReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Arrays;

import org.apache.bookkeeper.versioning.Version;
import com.google.protobuf.TextFormat;
import com.google.protobuf.ByteString;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.apache.bookkeeper.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates all the ledger metadata that is persistently stored
 * in zookeeper. It provides parsing and serialization methods of such metadata.
 *
 */
public class LedgerMetadata {
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

    private LedgerMetadataFormat.State state;
    private SortedMap<Long, ArrayList<InetSocketAddress>> ensembles = new TreeMap<Long, ArrayList<InetSocketAddress>>();
    ArrayList<InetSocketAddress> currentEnsemble;
    volatile Version version = Version.NEW;

    private boolean hasPassword = false;
    private LedgerMetadataFormat.DigestType digestType;
    private byte[] password;

    public LedgerMetadata(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                          BookKeeper.DigestType digestType, byte[] password) {
        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;

        /*
         * It is set in PendingReadOp.readEntryComplete, and
         * we read it in LedgerRecoveryOp.readComplete.
         */
        this.length = 0;
        this.state = LedgerMetadataFormat.State.OPEN;
        this.lastEntryId = LedgerHandle.INVALID_ENTRY_ID;
        this.metadataFormatVersion = CURRENT_METADATA_FORMAT_VERSION;

        this.digestType = digestType.equals(BookKeeper.DigestType.MAC) ?
            LedgerMetadataFormat.DigestType.HMAC : LedgerMetadataFormat.DigestType.CRC32;
        this.password = Arrays.copyOf(password, password.length);
        this.hasPassword = true;
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
        this.password = new byte[other.password.length];
        System.arraycopy(other.password, 0, this.password, 0, other.password.length);
        // copy the ensembles
        for (Entry<Long, ArrayList<InetSocketAddress>> entry : other.ensembles.entrySet()) {
            long startEntryId = entry.getKey();
            ArrayList<InetSocketAddress> newEnsemble = new ArrayList<InetSocketAddress>(entry.getValue());
            this.addEnsemble(startEntryId, newEnsemble);
        }
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
    public SortedMap<Long, ArrayList<InetSocketAddress>> getEnsembles() {
        return ensembles;
    }

    void setEnsembles(SortedMap<Long, ArrayList<InetSocketAddress>> ensembles) {
        this.ensembles = ensembles;
    }

    public int getEnsembleSize() {
        return ensembleSize;
    }

    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    public int getAckQuorumSize() {
        return ackQuorumSize;
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

    byte[] getPassword() {
        return Arrays.copyOf(password, password.length);
    }

    BookKeeper.DigestType getDigestType() {
        if (digestType.equals(LedgerMetadataFormat.DigestType.HMAC)) {
            return BookKeeper.DigestType.MAC;
        } else {
            return BookKeeper.DigestType.CRC32;
        }
    }

    public long getLastEntryId() {
        return lastEntryId;
    }

    public long getLength() {
        return length;
    }

    void setLength(long length) {
        this.length = length;
    }

    public boolean isClosed() {
        return state == LedgerMetadataFormat.State.CLOSED;
    }

    public boolean isInRecovery() {
        return state == LedgerMetadataFormat.State.IN_RECOVERY;
    }

    LedgerMetadataFormat.State getState() {
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

    void addEnsemble(long startEntryId, ArrayList<InetSocketAddress> ensemble) {
        assert ensembles.isEmpty() || startEntryId >= ensembles.lastKey();

        ensembles.put(startEntryId, ensemble);
        currentEnsemble = ensemble;
    }

    ArrayList<InetSocketAddress> getEnsemble(long entryId) {
        // the head map cannot be empty, since we insert an ensemble for
        // entry-id 0, right when we start
        return ensembles.get(ensembles.headMap(entryId + 1).lastKey());
    }

    /**
     * the entry id > the given entry-id at which the next ensemble change takes
     * place ( -1 if no further ensemble changes)
     *
     * @param entryId
     * @return
     */
    long getNextEnsembleChange(long entryId) {
        SortedMap<Long, ArrayList<InetSocketAddress>> tailMap = ensembles.tailMap(entryId + 1);

        if (tailMap.isEmpty()) {
            return -1;
        } else {
            return tailMap.firstKey();
        }
    }

    /**
     * Generates a byte array of this object
     *
     * @return the metadata serialized into a byte array
     */
    public byte[] serialize() {
        if (metadataFormatVersion == 1) {
            return serializeVersion1();
        }
        LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();
        builder.setQuorumSize(writeQuorumSize).setAckQuorumSize(ackQuorumSize)
            .setEnsembleSize(ensembleSize).setLength(length)
            .setState(state).setLastEntryId(lastEntryId);

        if (hasPassword) {
            builder.setDigestType(digestType).setPassword(ByteString.copyFrom(password));
        }

        for (Map.Entry<Long, ArrayList<InetSocketAddress>> entry : ensembles.entrySet()) {
            LedgerMetadataFormat.Segment.Builder segmentBuilder = LedgerMetadataFormat.Segment.newBuilder();
            segmentBuilder.setFirstEntryId(entry.getKey());
            for (InetSocketAddress addr : entry.getValue()) {
                segmentBuilder.addEnsembleMember(StringUtils.addrToString(addr));
            }
            builder.addSegment(segmentBuilder.build());
        }

        StringBuilder s = new StringBuilder();
        s.append(VERSION_KEY).append(tSplitter).append(CURRENT_METADATA_FORMAT_VERSION).append(lSplitter);
        s.append(TextFormat.printToString(builder.build()));
        LOG.debug("Serialized config: {}", s);
        return s.toString().getBytes();
    }

    private byte[] serializeVersion1() {
        StringBuilder s = new StringBuilder();
        s.append(VERSION_KEY).append(tSplitter).append(metadataFormatVersion).append(lSplitter);
        s.append(writeQuorumSize).append(lSplitter).append(ensembleSize).append(lSplitter).append(length);

        for (Map.Entry<Long, ArrayList<InetSocketAddress>> entry : ensembles.entrySet()) {
            s.append(lSplitter).append(entry.getKey());
            for (InetSocketAddress addr : entry.getValue()) {
                s.append(tSplitter);
                s.append(StringUtils.addrToString(addr));
            }
        }

        if (state == LedgerMetadataFormat.State.IN_RECOVERY) {
            s.append(lSplitter).append(IN_RECOVERY).append(tSplitter).append(closed);
        } else if (state == LedgerMetadataFormat.State.CLOSED) {
            s.append(lSplitter).append(getLastEntryId()).append(tSplitter).append(closed);
        }

        LOG.debug("Serialized config: {}", s);

        return s.toString().getBytes();
    }

    /**
     * Parses a given byte array and transforms into a LedgerConfig object
     *
     * @param bytes
     *            byte array to parse
     * @param version
     *            version of the ledger metadata
     * @return LedgerConfig
     * @throws IOException
     *             if the given byte[] cannot be parsed
     */
    public static LedgerMetadata parseConfig(byte[] bytes, Version version) throws IOException {
        LedgerMetadata lc = new LedgerMetadata();
        lc.version = version;

        String config = new String(bytes);

        LOG.debug("Parsing Config: {}", config);
        BufferedReader reader = new BufferedReader(new StringReader(config));
        String versionLine = reader.readLine();
        if (versionLine == null) {
            throw new IOException("Invalid metadata. Content missing");
        }
        int i = 0;
        if (versionLine.startsWith(VERSION_KEY)) {
            String parts[] = versionLine.split(tSplitter);
            lc.metadataFormatVersion = new Integer(parts[1]);
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

        LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();
        TextFormat.merge(reader, builder);
        LedgerMetadataFormat data = builder.build();
        lc.writeQuorumSize = data.getQuorumSize();
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
            ArrayList<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
            for (String member : s.getEnsembleMemberList()) {
                addrs.add(StringUtils.parseAddr(member));
            }
            lc.addEnsemble(s.getFirstEntryId(), addrs);
        }
        return lc;
    }

    static LedgerMetadata parseVersion1Config(LedgerMetadata lc,
                                              BufferedReader reader) throws IOException {
        try {
            lc.writeQuorumSize = lc.ackQuorumSize = new Integer(reader.readLine());
            lc.ensembleSize = new Integer(reader.readLine());
            lc.length = new Long(reader.readLine());

            String line = reader.readLine();
            while (line != null) {
                String parts[] = line.split(tSplitter);

                if (parts[1].equals(closed)) {
                    Long l = new Long(parts[0]);
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

                ArrayList<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
                for (int j = 1; j < parts.length; j++) {
                    addrs.add(StringUtils.parseAddr(parts[j]));
                }
                lc.addEnsemble(new Long(parts[0]), addrs);
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
     * Is the metadata newer that given <i>newMeta</i>.
     *
     * @param newMeta
     * @return
     */
    boolean isNewerThan(LedgerMetadata newMeta) {
        if (null == version) {
            return false;
        }
        return Version.Occurred.AFTER == version.compare(newMeta.version);
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

        if (metadataFormatVersion != newMeta.metadataFormatVersion ||
            ensembleSize != newMeta.ensembleSize ||
            writeQuorumSize != newMeta.writeQuorumSize ||
            ackQuorumSize != newMeta.ackQuorumSize ||
            length != newMeta.length ||
            state != newMeta.state ||
            !digestType.equals(newMeta.digestType) ||
            !Arrays.equals(password, newMeta.password)) {
            return true;
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
            for (int i=0; i<newMeta.ensembles.size(); i++) {
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
        StringBuilder sb = new StringBuilder();
        sb.append("(meta:").append(new String(serialize(), UTF_8)).append(", version:").append(version).append(")");
        return sb.toString();
    }

    void mergeEnsembles(SortedMap<Long, ArrayList<InetSocketAddress>> newEnsembles) {
        // allow new metadata to be one ensemble less than current metadata
        // since ensemble change might kick in when recovery changed metadata
        int diff = ensembles.size() - newEnsembles.size();
        if (0 != diff && 1 != diff) {
            return;
        }
        int i = 0;
        for (Entry<Long, ArrayList<InetSocketAddress>> entry : newEnsembles.entrySet()) {
            ++i;
            if (ensembles.size() != i) {
                // we should use last ensemble from current metadata
                // not the new metadata read from zookeeper
                long key = entry.getKey();
                ArrayList<InetSocketAddress> ensemble = entry.getValue();
                ensembles.put(key, ensemble);
            }
        }
    }

}
