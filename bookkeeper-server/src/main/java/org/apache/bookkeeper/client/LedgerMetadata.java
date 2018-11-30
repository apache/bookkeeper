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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata.State;
import org.apache.bookkeeper.net.BookieSocketAddress;
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

    private final int metadataFormatVersion;
    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;

    private final State state;
    private final long length;
    private final long lastEntryId;
    private final long ctime;
    final boolean storeCtime; // non-private so builder can access for copy

    private final NavigableMap<Long, ImmutableList<BookieSocketAddress>> ensembles;
    private final ImmutableList<BookieSocketAddress> currentEnsemble;

    private final boolean hasPassword;
    private final DigestType digestType;
    private final byte[] password;

    private final Map<String, byte[]> customMetadata;

    LedgerMetadata(int metadataFormatVersion,
                   int ensembleSize,
                   int writeQuorumSize,
                   int ackQuorumSize,
                   State state,
                   Optional<Long> lastEntryId,
                   Optional<Long> length,
                   Map<Long, List<BookieSocketAddress>> ensembles,
                   DigestType digestType,
                   Optional<byte[]> password,
                   long ctime,
                   boolean storeCtime,
                   Map<String, byte[]> customMetadata) {
        checkArgument(ensembles.size() > 0, "There must be at least one ensemble in the ledger");
        if (state == State.CLOSED) {
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

        if (state != State.CLOSED) {
            currentEnsemble = this.ensembles.lastEntry().getValue();
        } else {
            currentEnsemble = null;
        }

        this.digestType = digestType;

        if (password.isPresent()) {
            this.password = password.get();
            this.hasPassword = true;
        } else {
            this.password = null;
            this.hasPassword = false;
        }
        this.ctime = ctime;
        this.storeCtime = storeCtime;

        this.customMetadata = ImmutableMap.copyOf(customMetadata);
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
    public boolean hasPassword() {
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
        return digestType;
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
        return state == State.CLOSED;
    }

    public boolean isInRecovery() {
        return state == State.IN_RECOVERY;
    }

    @Override
    public State getState() {
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
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper("LedgerMetadata");
        helper.add("formatVersion", metadataFormatVersion)
            .add("ensembleSize", ensembleSize)
            .add("writeQuorumSize", writeQuorumSize)
            .add("ackQuorumSize", ackQuorumSize)
            .add("state", state);
        if (state == State.CLOSED) {
            helper.add("length", length)
                .add("lastEntryId", lastEntryId);
        }
        if (hasPassword()) {
            helper.add("digestType", digestType);
            if (withPassword) {
                helper.add("password", "base64:" + Base64.getEncoder().encodeToString(password));
            } else {
                helper.add("password", "OMITTED");
            }
        }
        helper.add("ensembles", ensembles.toString());
        helper.add("customMetadata",
                   customMetadata.entrySet().stream().collect(
                           Collectors.toMap(e -> e.getKey(),
                                            e -> "base64:" + Base64.getEncoder().encodeToString(e.getValue()))));
        return helper.toString();
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

    public int getMetadataFormatVersion() {
        return metadataFormatVersion;
    }

    // temporarily method, until storeCtime is removed from the metadata object itself
    public boolean shouldStoreCtime() {
        return storeCtime;
    }
}
