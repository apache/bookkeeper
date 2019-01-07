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
package org.apache.bookkeeper.meta;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.LedgerMetadataUtils;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.LedgerMetadata.State;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serialization and deserialization for LedgerMetadata.
 */
public class LedgerMetadataSerDe {
    private static final Logger log = LoggerFactory.getLogger(LedgerMetadataSerDe.class);

    public static final int CURRENT_METADATA_FORMAT_VERSION = 2;
    private static final int LOWEST_COMPAT_METADATA_FORMAT_VERSION = 0;

    // for pulling the version
    private static final String VERSION_KEY = "BookieMetadataFormatVersion";
    private static final String LINE_SPLITTER = "\n";
    private static final String FIELD_SPLITTER = "\t";

    // old V1 constants
    private static final String V1_CLOSED_TAG = "CLOSED";
    private static final int V1_IN_RECOVERY_ENTRY_ID = -102;

    public byte[] serialize(LedgerMetadata metadata) {
        if (metadata.getMetadataFormatVersion() == 1) {
            return serializeVersion1(metadata);
        }

        StringBuilder s = new StringBuilder();
        s.append(VERSION_KEY).append(FIELD_SPLITTER)
            .append(CURRENT_METADATA_FORMAT_VERSION).append(LINE_SPLITTER);
        s.append(TextFormat.printToString(buildProtoFormat(metadata)));
        if (log.isDebugEnabled()) {
            log.debug("Serialized config: {}", s);
        }
        return s.toString().getBytes(UTF_8);
    }

    private byte[] serializeVersion1(LedgerMetadata metadata) {
        StringBuilder s = new StringBuilder();
        s.append(VERSION_KEY).append(FIELD_SPLITTER)
            .append(metadata.getMetadataFormatVersion()).append(LINE_SPLITTER);
        s.append(metadata.getWriteQuorumSize()).append(LINE_SPLITTER)
            .append(metadata.getEnsembleSize()).append(LINE_SPLITTER).append(metadata.getLength());

        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> entry : metadata.getAllEnsembles().entrySet()) {
            s.append(LINE_SPLITTER).append(entry.getKey());
            for (BookieSocketAddress addr : entry.getValue()) {
                s.append(FIELD_SPLITTER);
                s.append(addr.toString());
            }
        }

        if (metadata.getState() == State.IN_RECOVERY) {
            s.append(LINE_SPLITTER).append(V1_IN_RECOVERY_ENTRY_ID)
                .append(FIELD_SPLITTER).append(V1_CLOSED_TAG);
        } else if (metadata.getState() == State.CLOSED) {
            s.append(LINE_SPLITTER).append(metadata.getLastEntryId())
                .append(FIELD_SPLITTER).append(V1_CLOSED_TAG);
        } else {
            checkArgument(metadata.getState() == State.OPEN,
                          String.format("Unknown state %s for V1 serialization", metadata.getState()));
        }

        if (log.isDebugEnabled()) {
            log.debug("Serialized config: {}", s);
        }

        return s.toString().getBytes(UTF_8);
    }

    @VisibleForTesting
    public LedgerMetadataFormat buildProtoFormat(LedgerMetadata metadata) {
        LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();
        builder.setQuorumSize(metadata.getWriteQuorumSize())
            .setAckQuorumSize(metadata.getAckQuorumSize())
            .setEnsembleSize(metadata.getEnsembleSize())
            .setLength(metadata.getLength())
            .setLastEntryId(metadata.getLastEntryId());

        switch (metadata.getState()) {
        case CLOSED:
            builder.setState(LedgerMetadataFormat.State.CLOSED);
            break;
        case IN_RECOVERY:
            builder.setState(LedgerMetadataFormat.State.IN_RECOVERY);
            break;
        case OPEN:
            builder.setState(LedgerMetadataFormat.State.OPEN);
            break;
        default:
            checkArgument(false,
                          String.format("Unknown state %s for protobuf serialization", metadata.getState()));
            break;
        }

        /** Hack to get around fact that ctime was never versioned correctly */
        if (LedgerMetadataUtils.shouldStoreCtime(metadata)) {
            builder.setCtime(metadata.getCtime());
        }

        if (metadata.hasPassword()) {
            builder.setDigestType(apiToProtoDigestType(metadata.getDigestType()))
                .setPassword(ByteString.copyFrom(metadata.getPassword()));
        }

        Map<String, byte[]> customMetadata = metadata.getCustomMetadata();
        if (customMetadata.size() > 0) {
            LedgerMetadataFormat.cMetadataMapEntry.Builder cMetadataBuilder =
                LedgerMetadataFormat.cMetadataMapEntry.newBuilder();
            for (Map.Entry<String, byte[]> entry : customMetadata.entrySet()) {
                cMetadataBuilder.setKey(entry.getKey()).setValue(ByteString.copyFrom(entry.getValue()));
                builder.addCustomMetadata(cMetadataBuilder.build());
            }
        }

        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> entry : metadata.getAllEnsembles().entrySet()) {
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
    public LedgerMetadata parseConfig(byte[] bytes,
                                      Optional<Long> metadataStoreCtime) throws IOException {
        String config = new String(bytes, UTF_8);

        if (log.isDebugEnabled()) {
            log.debug("Parsing Config: {}", config);
        }
        BufferedReader reader = new BufferedReader(new StringReader(config));
        String versionLine = reader.readLine();
        if (versionLine == null) {
            throw new IOException("Invalid metadata. Content missing");
        }
        final int metadataFormatVersion;
        if (versionLine.startsWith(VERSION_KEY)) {
            String parts[] = versionLine.split(FIELD_SPLITTER);
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
            builder.withClosedState().withLastEntryId(data.getLastEntryId()).withLength(data.getLength());
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
                String parts[] = line.split(FIELD_SPLITTER);

                if (parts[1].equals(V1_CLOSED_TAG)) {
                    Long l = Long.parseLong(parts[0]);
                    if (l == V1_IN_RECOVERY_ENTRY_ID) {
                        builder.withInRecoveryState();
                    } else {
                        builder.withClosedState().withLastEntryId(l).withLength(length);
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

    private static LedgerMetadataFormat.DigestType apiToProtoDigestType(DigestType digestType) {
        switch (digestType) {
        case MAC:
            return LedgerMetadataFormat.DigestType.HMAC;
        case CRC32:
            return LedgerMetadataFormat.DigestType.CRC32;
        case CRC32C:
            return LedgerMetadataFormat.DigestType.CRC32C;
        case DUMMY:
            return LedgerMetadataFormat.DigestType.DUMMY;
        default:
            throw new IllegalArgumentException("Unable to convert digest type " + digestType);
        }
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
