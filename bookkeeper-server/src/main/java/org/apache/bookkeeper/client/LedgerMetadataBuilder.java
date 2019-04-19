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
import static org.apache.bookkeeper.meta.LedgerMetadataSerDe.CURRENT_METADATA_FORMAT_VERSION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.LedgerMetadata.State;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.LimitedPrivate;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;
import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * Builder for building LedgerMetadata objects.
 */
@LimitedPrivate
@Unstable
@VisibleForTesting
public class LedgerMetadataBuilder {
    private int metadataFormatVersion = CURRENT_METADATA_FORMAT_VERSION;
    private int ensembleSize = 3;
    private int writeQuorumSize = 3;
    private int ackQuorumSize = 2;

    private State state = State.OPEN;
    private Optional<Long> lastEntryId = Optional.empty();
    private Optional<Long> length = Optional.empty();

    private TreeMap<Long, List<BookieSocketAddress>> ensembles = new TreeMap<>();

    private Optional<DigestType> digestType = Optional.empty();
    private Optional<byte[]> password = Optional.empty();

    private long ctime = -1;
    private boolean storeCtime = false;
    private Map<String, byte[]> customMetadata = Collections.emptyMap();

    private static final long BLANK_CTOKEN = 0;
    private long cToken = BLANK_CTOKEN;

    public static LedgerMetadataBuilder create() {
        return new LedgerMetadataBuilder();
    }

    public static LedgerMetadataBuilder from(LedgerMetadata other) {
        LedgerMetadataBuilder builder = new LedgerMetadataBuilder();
        builder.metadataFormatVersion = other.getMetadataFormatVersion();
        builder.ensembleSize = other.getEnsembleSize();
        builder.writeQuorumSize = other.getWriteQuorumSize();
        builder.ackQuorumSize = other.getAckQuorumSize();

        builder.state = other.getState();
        if (builder.state == State.CLOSED) {
            builder.lastEntryId = Optional.of(other.getLastEntryId());
            builder.length = Optional.of(other.getLength());
        }

        builder.ensembles.putAll(other.getAllEnsembles());

        if (other.hasPassword()) {
            builder.password = Optional.of(other.getPassword());
            builder.digestType = Optional.of(other.getDigestType());
        }

        builder.ctime = other.getCtime();

        /** Hack to get around fact that ctime was never versioned correctly */
        builder.storeCtime = LedgerMetadataUtils.shouldStoreCtime(other);

        builder.customMetadata = ImmutableMap.copyOf(other.getCustomMetadata());

        return builder;
    }

    public LedgerMetadataBuilder withMetadataFormatVersion(int version) {
        this.metadataFormatVersion = version;
        return this;
    }

    public LedgerMetadataBuilder withPassword(byte[] password) {
        this.password = Optional.of(Arrays.copyOf(password, password.length));
        return this;
    }

    public LedgerMetadataBuilder withDigestType(DigestType digestType) {
        this.digestType = Optional.of(digestType);
        return this;
    }

    public LedgerMetadataBuilder withEnsembleSize(int ensembleSize) {
        checkState(ensembles.size() == 0, "Can only set ensemble size before adding ensembles to the builder");
        this.ensembleSize = ensembleSize;
        return this;
    }

    public LedgerMetadataBuilder withWriteQuorumSize(int writeQuorumSize) {
        this.writeQuorumSize = writeQuorumSize;
        return this;
    }

    public LedgerMetadataBuilder withAckQuorumSize(int ackQuorumSize) {
        this.ackQuorumSize = ackQuorumSize;
        return this;
    }

    public LedgerMetadataBuilder newEnsembleEntry(long firstEntry, List<BookieSocketAddress> ensemble) {
        checkArgument(ensemble.size() == ensembleSize,
                      "Size of passed in ensemble must match the ensembleSize of the builder");
        checkArgument(ensembles.isEmpty() || firstEntry > ensembles.lastKey(),
                      "New entry must have a first entry greater than any existing ensemble key");
        ensembles.put(firstEntry, ensemble);
        return this;
    }

    public LedgerMetadataBuilder replaceEnsembleEntry(long firstEntry, List<BookieSocketAddress> ensemble) {
        checkArgument(ensemble.size() == ensembleSize,
                      "Size of passed in ensemble must match the ensembleSize of the builder");
        checkArgument(ensembles.containsKey(firstEntry),
                      "Ensemble must replace an existing ensemble in the ensemble map");
        ensembles.put(firstEntry, ensemble);
        return this;
    }

    public LedgerMetadataBuilder withInRecoveryState() {
        this.state = State.IN_RECOVERY;
        return this;
    }

    public LedgerMetadataBuilder withClosedState() {
        this.state = State.CLOSED;
        return this;
    }

    public LedgerMetadataBuilder withLastEntryId(long lastEntryId) {
        this.lastEntryId = Optional.of(lastEntryId);
        return this;
    }

    public LedgerMetadataBuilder withLength(long length) {
        this.length = Optional.of(length);
        return this;
    }

    public LedgerMetadataBuilder withCustomMetadata(Map<String, byte[]> customMetadata) {
        this.customMetadata = ImmutableMap.copyOf(customMetadata);
        return this;
    }

    public LedgerMetadataBuilder withCreationTime(long ctime) {
        this.ctime = ctime;
        return this;
    }

    public LedgerMetadataBuilder storingCreationTime(boolean storing) {
        this.storeCtime = storing;
        return this;
    }

    public LedgerMetadataBuilder withCToken(long cToken) {
        this.cToken = cToken;
        return this;
    }

    public LedgerMetadata build() {
        checkArgument(ensembleSize >= writeQuorumSize, "Write quorum must be less or equal to ensemble size");
        checkArgument(writeQuorumSize >= ackQuorumSize, "Write quorum must be greater or equal to ack quorum");

        return new LedgerMetadataImpl(metadataFormatVersion,
                                      ensembleSize, writeQuorumSize, ackQuorumSize,
                                      state, lastEntryId, length, ensembles,
                                      digestType, password, ctime, storeCtime,
                                      cToken,
                                      customMetadata);
    }

}
