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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
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

    /**
     * Text based manual serialization.
     * Available from v4.0.x onwards.
     */
    public static final int METADATA_FORMAT_VERSION_1 = 1;

    /**
     * Protobuf based, serialized using TextFormat.
     * Available from v4.2.x onwards.
     * Can contain ctime or not, but if it contains ctime it can only be parse by v4.4.x onwards.
     */
    public static final int METADATA_FORMAT_VERSION_2 = 2;

    /**
     * Protobuf based, serialized in binary format.
     * Available from v4.9.x onwards.
     */
    public static final int METADATA_FORMAT_VERSION_3 = 3;

    public static final int MAXIMUM_METADATA_FORMAT_VERSION = METADATA_FORMAT_VERSION_3;
    public static final int CURRENT_METADATA_FORMAT_VERSION = METADATA_FORMAT_VERSION_3;
    private static final int LOWEST_COMPAT_METADATA_FORMAT_VERSION = METADATA_FORMAT_VERSION_1;

    // for pulling the version
    private static final int MAX_VERSION_DIGITS = 10;
    private static final byte[] VERSION_KEY_BYTES = "BookieMetadataFormatVersion\t".getBytes(UTF_8);
    private static final String LINE_SPLITTER = "\n";
    private static final byte[] LINE_SPLITTER_BYTES = LINE_SPLITTER.getBytes(UTF_8);
    private static final String FIELD_SPLITTER = "\t";

    // old V1 constants
    private static final String V1_CLOSED_TAG = "CLOSED";
    private static final int V1_IN_RECOVERY_ENTRY_ID = -102;

    private static void writeHeader(OutputStream os, int version) throws IOException {
        os.write(VERSION_KEY_BYTES);
        os.write(String.valueOf(version).getBytes(UTF_8));
        os.write(LINE_SPLITTER_BYTES);
    }

    private static int readHeader(InputStream is) throws IOException {
        checkState(LINE_SPLITTER_BYTES.length == 1, "LINE_SPLITTER must be single byte");

        for (int i = 0; i < VERSION_KEY_BYTES.length; i++) {
            int b = is.read();
            if (b < 0 || ((byte) b) != VERSION_KEY_BYTES[i]) {
                throw new IOException("Ledger metadata header corrupt at index " + i);
            }
        }
        byte[] versionBuf = new byte[MAX_VERSION_DIGITS];
        int i = 0;
        while (i < MAX_VERSION_DIGITS) {
            int b = is.read();
            if (b == LINE_SPLITTER_BYTES[0]) {
                String versionStr = new String(versionBuf, 0, i, UTF_8);
                try {
                    return Integer.parseInt(versionStr);
                } catch (NumberFormatException nfe) {
                    throw new IOException("Unable to parse version number from " + versionStr);
                }
            } else if (b < 0) {
                break;
            } else {
                versionBuf[i++] = (byte) b;
            }
        }
        throw new IOException("Unable to find end of version number, metadata appears corrupt");
    }

    public byte[] serialize(LedgerMetadata metadata) throws IOException {
        int formatVersion = metadata.getMetadataFormatVersion();
        final byte[] serialized;
        switch (formatVersion) {
        case METADATA_FORMAT_VERSION_3:
            serialized = serializeVersion3(metadata);
            break;
        case METADATA_FORMAT_VERSION_2:
            serialized = serializeVersion2(metadata);
            break;
        case METADATA_FORMAT_VERSION_1:
            serialized = serializeVersion1(metadata);
            break;
        default:
            throw new IllegalArgumentException("Invalid format version " + formatVersion);
        }
        if (log.isDebugEnabled()) {
            String serializedStr;
            if (formatVersion > METADATA_FORMAT_VERSION_2) {
                serializedStr = Base64.getEncoder().encodeToString(serialized);
            } else {
                serializedStr = new String(serialized, UTF_8);
            }
            log.debug("Serialized with format {}: {}", formatVersion, serializedStr);
        }
        return serialized;
    }

    private static byte[] serializeVersion3(LedgerMetadata metadata) throws IOException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            writeHeader(os, METADATA_FORMAT_VERSION_3);
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


            builder.setDigestType(apiToProtoDigestType(metadata.getDigestType()));

            serializePassword(metadata.getPassword(), builder);

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

            builder.setCToken(metadata.getCToken());

            builder.build().writeDelimitedTo(os);
            return os.toByteArray();
        }
    }

    private static byte[] serializeVersion2(LedgerMetadata metadata) throws IOException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            writeHeader(os, METADATA_FORMAT_VERSION_2);
            try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(os, UTF_8.name()))) {
                /***********************************************************************
                 * WARNING: Do not modify to add fields.
                 * This code is purposefully duplicated, as version 2 does not support adding
                 * fields, and if this code was shared with version 3, it would be easy to
                 * accidently add new fields and create BC issues.
                 **********************************************************************/
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

                builder.setDigestType(apiToProtoDigestType(metadata.getDigestType()));
                serializePassword(metadata.getPassword(), builder);

                Map<String, byte[]> customMetadata = metadata.getCustomMetadata();
                if (customMetadata.size() > 0) {
                    LedgerMetadataFormat.cMetadataMapEntry.Builder cMetadataBuilder =
                        LedgerMetadataFormat.cMetadataMapEntry.newBuilder();
                    for (Map.Entry<String, byte[]> entry : customMetadata.entrySet()) {
                        cMetadataBuilder.setKey(entry.getKey()).setValue(ByteString.copyFrom(entry.getValue()));
                        builder.addCustomMetadata(cMetadataBuilder.build());
                    }
                }

                for (Map.Entry<Long, ? extends List<BookieSocketAddress>> entry :
                         metadata.getAllEnsembles().entrySet()) {
                    LedgerMetadataFormat.Segment.Builder segmentBuilder = LedgerMetadataFormat.Segment.newBuilder();
                    segmentBuilder.setFirstEntryId(entry.getKey());
                    for (BookieSocketAddress addr : entry.getValue()) {
                        segmentBuilder.addEnsembleMember(addr.toString());
                    }
                    builder.addSegment(segmentBuilder.build());
                }

                TextFormat.print(builder.build(), writer);
                writer.flush();
            }
            return os.toByteArray();
        }
    }

    private static byte[] serializeVersion1(LedgerMetadata metadata) throws IOException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            writeHeader(os, METADATA_FORMAT_VERSION_1);

            try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(os, UTF_8.name()))) {
                writer.append(String.valueOf(metadata.getWriteQuorumSize())).append(LINE_SPLITTER);
                writer.append(String.valueOf(metadata.getEnsembleSize())).append(LINE_SPLITTER);
                writer.append(String.valueOf(metadata.getLength())).append(LINE_SPLITTER);

                for (Map.Entry<Long, ? extends List<BookieSocketAddress>> entry :
                         metadata.getAllEnsembles().entrySet()) {
                    writer.append(String.valueOf(entry.getKey()));
                    for (BookieSocketAddress addr : entry.getValue()) {
                        writer.append(FIELD_SPLITTER).append(addr.toString());
                    }
                    writer.append(LINE_SPLITTER);
                }

                if (metadata.getState() == State.IN_RECOVERY) {
                    writer.append(String.valueOf(V1_IN_RECOVERY_ENTRY_ID)).append(FIELD_SPLITTER).append(V1_CLOSED_TAG);
                } else if (metadata.getState() == State.CLOSED) {
                    writer.append(String.valueOf(metadata.getLastEntryId()))
                        .append(FIELD_SPLITTER).append(V1_CLOSED_TAG);
                } else {
                    checkArgument(metadata.getState() == State.OPEN,
                                  String.format("Unknown state %s for V1 serialization", metadata.getState()));
                }
                writer.flush();
            } catch (UnsupportedEncodingException uee) {
                throw new RuntimeException("UTF_8 should be supported everywhere");
            }
            return os.toByteArray();
        }
    }

    private static void serializePassword(byte[] password, LedgerMetadataFormat.Builder builder) {
        if (password == null || password.length == 0) {
            builder.setPassword(ByteString.EMPTY);
        } else {
            builder.setPassword(ByteString.copyFrom(password));
        }
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
        if (log.isDebugEnabled()) {
            log.debug("Deserializing {}", Base64.getEncoder().encodeToString(bytes));
        }
        try (ByteArrayInputStream is = new ByteArrayInputStream(bytes)) {
            int metadataFormatVersion = readHeader(is);
            if (log.isDebugEnabled()) {
                String contentStr = "";
                if (metadataFormatVersion <= METADATA_FORMAT_VERSION_2) {
                    contentStr = ", content: " + new String(bytes, UTF_8);
                }
                log.debug("Format version {} detected{}", metadataFormatVersion, contentStr);
            }

            switch (metadataFormatVersion) {
            case METADATA_FORMAT_VERSION_3:
                return parseVersion3Config(is, metadataStoreCtime);
            case METADATA_FORMAT_VERSION_2:
                return parseVersion2Config(is, metadataStoreCtime);
            case METADATA_FORMAT_VERSION_1:
                return parseVersion1Config(is);
            default:
                throw new IOException(
                        String.format("Metadata version not compatible. Expected between %d and %d, but got %d",
                                      LOWEST_COMPAT_METADATA_FORMAT_VERSION, CURRENT_METADATA_FORMAT_VERSION,
                                      metadataFormatVersion));
            }
        }
    }

    private static LedgerMetadata parseVersion3Config(InputStream is, Optional<Long> metadataStoreCtime)
            throws IOException {
        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create()
                .withMetadataFormatVersion(METADATA_FORMAT_VERSION_3);
        LedgerMetadataFormat.Builder formatBuilder = LedgerMetadataFormat.newBuilder();
        formatBuilder.mergeDelimitedFrom(is);
        LedgerMetadataFormat data = formatBuilder.build();
        decodeFormat(data, builder);
        if (data.hasCtime()) {
            builder.storingCreationTime(true);
        } else if (metadataStoreCtime.isPresent()) {
            builder.withCreationTime(metadataStoreCtime.get()).storingCreationTime(false);
        }
        return builder.build();
    }

    private static LedgerMetadata parseVersion2Config(InputStream is, Optional<Long> metadataStoreCtime)
            throws IOException {
        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create()
            .withMetadataFormatVersion(METADATA_FORMAT_VERSION_2);

        LedgerMetadataFormat.Builder formatBuilder = LedgerMetadataFormat.newBuilder();
        try (InputStreamReader reader = new InputStreamReader(is, UTF_8.name())) {
            TextFormat.merge(reader, formatBuilder);
        }
        LedgerMetadataFormat data = formatBuilder.build();
        decodeFormat(data, builder);
        if (data.hasCtime()) {
            // 'storingCreationTime' is only ever taken into account for serializing version 2
            builder.storingCreationTime(true);
        } else if (metadataStoreCtime.isPresent()) {
            builder.withCreationTime(metadataStoreCtime.get()).storingCreationTime(false);
        }
        return builder.build();
    }

    private static void decodeFormat(LedgerMetadataFormat data, LedgerMetadataBuilder builder) throws IOException {
        builder.withEnsembleSize(data.getEnsembleSize());
        builder.withWriteQuorumSize(data.getQuorumSize());
        if (data.hasAckQuorumSize()) {
            builder.withAckQuorumSize(data.getAckQuorumSize());
        } else {
            builder.withAckQuorumSize(data.getQuorumSize());
        }

        if (data.hasCtime()) {
            builder.withCreationTime(data.getCtime());
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

        if (data.hasCToken()) {
            builder.withCToken(data.getCToken());
        }
    }

    private static LedgerMetadata parseVersion1Config(InputStream is) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, UTF_8.name()))) {
            LedgerMetadataBuilder builder = LedgerMetadataBuilder.create().withMetadataFormatVersion(1);
            int quorumSize = Integer.parseInt(reader.readLine());
            int ensembleSize = Integer.parseInt(reader.readLine());
            long length = Long.parseLong(reader.readLine());

            builder.withEnsembleSize(ensembleSize).withWriteQuorumSize(quorumSize).withAckQuorumSize(quorumSize);

            String line = reader.readLine();
            while (line != null) {
                String[] parts = line.split(FIELD_SPLITTER);

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
