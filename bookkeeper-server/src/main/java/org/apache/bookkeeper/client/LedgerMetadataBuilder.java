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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.apache.bookkeeper.versioning.Version;

class LedgerMetadataBuilder {
    private int ensembleSize = 3;
    private int writeQuorumSize = 3;
    private int ackQuorumSize = 2;

    private LedgerMetadataFormat.State state = LedgerMetadataFormat.State.OPEN;
    private Optional<Long> lastEntryId = Optional.empty();

    private Map<Long, List<BookieSocketAddress>> ensembles = new HashMap<>();

    private DigestType digestType = DigestType.CRC32C;
    private Optional<byte[]> password = Optional.empty();

    private Optional<Long> ctime = Optional.empty();
    private Map<String, byte[]> customMetadata = new HashMap<>();

    private Version version = Version.NEW;

    static LedgerMetadataBuilder create() {
        return new LedgerMetadataBuilder();
    }

    static LedgerMetadataBuilder from(LedgerMetadata other) {
        LedgerMetadataBuilder builder = new LedgerMetadataBuilder();
        builder.ensembleSize = other.getEnsembleSize();
        builder.writeQuorumSize = other.getWriteQuorumSize();
        builder.ackQuorumSize = other.getAckQuorumSize();

        builder.state = other.getState();

        long lastEntryId = other.getLastEntryId();
        if (lastEntryId != LedgerHandle.INVALID_ENTRY_ID) {
            builder.lastEntryId = Optional.of(lastEntryId);
        }

        builder.ensembles.putAll(other.getEnsembles());

        builder.digestType = other.getDigestType();
        if (other.hasPassword()) {
            builder.password = Optional.of(other.getPassword());
        }

        if (other.storeSystemtimeAsLedgerCreationTime) {
            builder.ctime = Optional.of(other.getCtime());
        }
        builder.customMetadata.putAll(other.getCustomMetadata());

        builder.version = other.getVersion();

        return builder;
    }

    LedgerMetadataBuilder withEnsembleSize(int ensembleSize) {
        checkState(ensembles.size() == 0, "Can only set ensemble size when ensembles is empty");
        this.ensembleSize = ensembleSize;
        return this;
    }

    LedgerMetadataBuilder withEnsembleEntry(long firstEntry, List<BookieSocketAddress> ensemble) {
        checkArgument(ensemble.size() == ensembleSize,
                      "Size of passed in ensemble must match the ensembleSize of the builder");

        ensembles.put(firstEntry, ensemble);
        return this;
    }

    LedgerMetadataBuilder closingAtEntry(long lastEntryId) {
        this.lastEntryId = Optional.of(lastEntryId);
        this.state = LedgerMetadataFormat.State.CLOSED;
        return this;
    }

    LedgerMetadata build() {
        return new LedgerMetadata(ensembleSize, writeQuorumSize, ackQuorumSize,
                                  state, lastEntryId, ensembles,
                                  digestType, password, ctime, customMetadata,
                                  version);
    }

}
