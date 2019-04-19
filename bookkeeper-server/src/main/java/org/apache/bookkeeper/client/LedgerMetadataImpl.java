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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
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
class LedgerMetadataImpl implements LedgerMetadata {
    static final Logger LOG = LoggerFactory.getLogger(LedgerMetadataImpl.class);

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

    private long cToken;

    LedgerMetadataImpl(int metadataFormatVersion,
                       int ensembleSize,
                       int writeQuorumSize,
                       int ackQuorumSize,
                       State state,
                       Optional<Long> lastEntryId,
                       Optional<Long> length,
                       Map<Long, List<BookieSocketAddress>> ensembles,
                       Optional<DigestType> digestType,
                       Optional<byte[]> password,
                       long ctime,
                       boolean storeCtime,
                       long cToken,
                       Map<String, byte[]> customMetadata) {
        checkArgument(ensembles.size() > 0, "There must be at least one ensemble in the ledger");
        if (state == State.CLOSED) {
            checkArgument(length.isPresent(), "Closed ledger must have a length");
            checkArgument(lastEntryId.isPresent(), "Closed ledger must have a last entry");
        } else {
            checkArgument(!length.isPresent(), "Non-closed ledger must not have a length");
            checkArgument(!lastEntryId.isPresent(), "Non-closed ledger must not have a last entry");
        }
        checkArgument((digestType.isPresent() && password.isPresent())
                      || (!digestType.isPresent() && !password.isPresent()),
                      "Either both password and digest type must be set, or neither");

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

        if (password.isPresent()) {
            this.password = password.get();
            this.digestType = digestType.get();
            this.hasPassword = true;
        } else {
            this.password = null;
            this.hasPassword = false;
            this.digestType = null;
        }
        this.ctime = ctime;
        this.storeCtime = storeCtime;

        this.cToken = cToken;

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
    @Override
    public boolean hasPassword() {
        return hasPassword;
    }

    @Override
    public byte[] getPassword() {
        if (!hasPassword()) {
            return new byte[0];
        } else {
            return Arrays.copyOf(password, password.length);
        }
    }

    @Override
    public DigestType getDigestType() {
        if (!hasPassword()) {
            return null;
        } else {
            return digestType;
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
        return state == State.CLOSED;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public List<BookieSocketAddress> getEnsembleAt(long entryId) {
        // the head map cannot be empty, since we insert an ensemble for
        // entry-id 0, right when we start
        return ensembles.get(ensembles.headMap(entryId + 1).lastKey());
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
    @Override
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

    @Override
    public int getMetadataFormatVersion() {
        return metadataFormatVersion;
    }

    boolean shouldStoreCtime() {
        return storeCtime;
    }

    @Override
    public long getCToken() {
        return cToken;
    }
}
