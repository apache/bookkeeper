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

import java.io.IOException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * This class encapsulates ledger layout information that is persistently stored
 * in registration service. It provides parsing and serialization methods of such
 * information.
 */
@Slf4j
@Getter
@EqualsAndHashCode
@ToString
public class LedgerLayout {

    // version of compability layout version
    public static final int LAYOUT_MIN_COMPAT_VERSION = 1;
    // version of ledger layout metadata
    public static final int LAYOUT_FORMAT_VERSION = 2;

    private static final String FIELD_SPLITTER = ":";
    private static final String LINE_SPLITTER = "\n";

    // For version 2 and below, max ledger metadata format wasn't stored in the layout
    // so assume 2 if it is missing.
    private static final int DEFAULT_MAX_LEDGER_METADATA_FORMAT_VERSION = 2;
    private static final String MAX_LEDGER_METADATA_FORMAT_VERSION_FIELD =
        "MAX_LEDGER_METADATA_FORMAT_VERSION";

    // ledger manager factory class
    private final String managerFactoryClass;
    // ledger manager version
    private final int managerVersion;

    // layout version of how to store layout information
    private final int layoutFormatVersion;

    // maximum format version that can be used for storing ledger metadata
    private final int maxLedgerMetadataFormatVersion;

    /**
     * Ledger Layout Constructor.
     *
     * @param managerFactoryCls
     *          Ledger Manager Factory Class
     * @param managerVersion
     *          Ledger Manager Version
     */
    public LedgerLayout(String managerFactoryCls, int managerVersion) {
        this(managerFactoryCls, managerVersion,
             LedgerMetadataSerDe.CURRENT_METADATA_FORMAT_VERSION,
             LAYOUT_FORMAT_VERSION);
    }

    LedgerLayout(String managerFactoryCls, int managerVersion,
                 int maxLedgerMetadataFormatVersion,
                 int layoutVersion) {
        this.managerFactoryClass = managerFactoryCls;
        this.managerVersion = managerVersion;
        this.maxLedgerMetadataFormatVersion = maxLedgerMetadataFormatVersion;
        this.layoutFormatVersion = layoutVersion;
    }

    /**
     * Generates a byte array based on the LedgerLayout object.
     *
     * @return byte[]
     */
    public byte[] serialize() throws IOException {
        String s =
          new StringBuilder().append(layoutFormatVersion).append(LINE_SPLITTER)
            .append(managerFactoryClass).append(FIELD_SPLITTER).append(managerVersion).append(LINE_SPLITTER)
            .append(MAX_LEDGER_METADATA_FORMAT_VERSION_FIELD).append(FIELD_SPLITTER)
            .append(maxLedgerMetadataFormatVersion)
            .toString();

        if (log.isDebugEnabled()) {
            log.debug("Serialized layout info: {}", s);
        }
        return s.getBytes("UTF-8");
    }

    /**
     * Parses a given byte array and transforms into a LedgerLayout object.
     *
     * @param bytes
     *          byte array to parse
     * @return LedgerLayout
     * @throws IOException
     *             if the given byte[] cannot be parsed
     */
    public static LedgerLayout parseLayout(byte[] bytes) throws IOException {
        String layout = new String(bytes, "UTF-8");
        if (log.isDebugEnabled()) {
            log.debug("Parsing Layout: {}", layout);
        }

        String lines[] = layout.split(LINE_SPLITTER);

        try {
            int layoutFormatVersion = Integer.parseInt(lines[0]);
            if (LAYOUT_FORMAT_VERSION < layoutFormatVersion || LAYOUT_MIN_COMPAT_VERSION > layoutFormatVersion) {
                throw new IOException("Metadata version not compatible. Expected "
                        + LAYOUT_FORMAT_VERSION + ", but got " + layoutFormatVersion);
            }

            if (lines.length < 2) {
                throw new IOException("Ledger manager and its version absent from layout: " + layout);
            }

            String[] parts = lines[1].split(FIELD_SPLITTER);
            if (parts.length != 2) {
                throw new IOException("Invalid Ledger Manager defined in layout : " + layout);
            }
            // ledger manager factory class
            String managerFactoryCls = parts[0];
            // ledger manager version
            int managerVersion = Integer.parseInt(parts[1]);

            int maxLedgerMetadataFormatVersion = DEFAULT_MAX_LEDGER_METADATA_FORMAT_VERSION;
            if (lines.length >= 3) {
                String[] metadataFormatParts = lines[2].split(FIELD_SPLITTER);
                if (metadataFormatParts.length != 2
                    || !metadataFormatParts[0].equals(MAX_LEDGER_METADATA_FORMAT_VERSION_FIELD)) {
                    throw new IOException("Invalid field for max ledger metadata format:" + lines[2]);
                }
                maxLedgerMetadataFormatVersion = Integer.parseInt(metadataFormatParts[1]);
            }
            return new LedgerLayout(managerFactoryCls, managerVersion,
                                    maxLedgerMetadataFormatVersion, layoutFormatVersion);
        } catch (NumberFormatException e) {
            throw new IOException("Could not parse layout '" + layout + "'", e);
        }
    }

}
