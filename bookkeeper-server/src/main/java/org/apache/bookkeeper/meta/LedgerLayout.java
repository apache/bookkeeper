package org.apache.bookkeeper.meta;

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

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.configuration.ConfigurationException;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.FlatLedgerManager;

/**
 * This class encapsulates ledger layout information that is persistently stored
 * in zookeeper. It provides parsing and serialization methods of such information.
 *
 */
class LedgerLayout {
    static final Logger LOG = LoggerFactory.getLogger(LedgerLayout.class);

    // Znode name to store layout information
    public static final String LAYOUT_ZNODE = "LAYOUT";
    // version of ledger layout metadata
    public static final int LAYOUT_FORMAT_VERSION = 1;

    /**
     * Read ledger layout from zookeeper
     *
     * @param zk            ZooKeeper Client
     * @param ledgersRoot   Root of the ledger namespace to check
     * @return ledger layout, or null if none set in zookeeper
     */
    public static LedgerLayout readLayout(final ZooKeeper zk, final String ledgersRoot)
            throws IOException, KeeperException {
        String ledgersLayout = ledgersRoot + "/" + LAYOUT_ZNODE;

        try {
            LedgerLayout layout;

            try {
                byte[] layoutData = zk.getData(ledgersLayout, false, null);
                layout = parseLayout(layoutData);
            } catch (KeeperException.NoNodeException nne) {
                return null;
            }
            
            return layout;
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }

    static final String splitter = ":";
    static final String lSplitter = "\n";

    // ledger manager class
    private String managerType;
    // ledger manager version
    private int managerVersion;

    // layout version of how to store layout information
    private int layoutFormatVersion = LAYOUT_FORMAT_VERSION;

    /**
     * Ledger Layout Constructor
     *
     * @param type
     *          Ledger Manager Type
     * @param managerVersion
     *          Ledger Manager Version
     * @param layoutFormatVersion
     *          Ledger Layout Format Version
     */
    public LedgerLayout(String managerType, int managerVersion) {
        this.managerType = managerType;
        this.managerVersion = managerVersion;
    }

    public String getManagerType() {
        return this.managerType;
    }

    public int getManagerVersion() {
        return this.managerVersion;
    }

    /**
     * Store the ledger layout into zookeeper
     */
    public void store(final ZooKeeper zk, String ledgersRoot) 
            throws IOException, KeeperException, InterruptedException {
        String ledgersLayout = ledgersRoot + "/" + LAYOUT_ZNODE;
        zk.create(ledgersLayout, serialize(), 
                  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * Generates a byte array based on the LedgerLayout object.
     *
     * @return byte[]
     */
    private byte[] serialize() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(layoutFormatVersion).append(lSplitter)
            .append(managerType).append(splitter).append(managerVersion);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Serialized layout info: " + sb.toString());
        }

        return sb.toString().getBytes("UTF-8");
    }

    /**
     * Parses a given byte array and transforms into a LedgerLayout object
     *
     * @param bytes
     *          byte array to parse
     * @param znodeVersion
     *          version of znode
     * @return LedgerLayout
     * @throws IOException
     *             if the given byte[] cannot be parsed
     */
    private static LedgerLayout parseLayout(byte[] bytes) throws IOException {
        String layout = new String(bytes, "UTF-8");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parsing Layout: " + layout);
        }

        String lines[] = layout.split(lSplitter);

        try {
            int layoutFormatVersion = new Integer(lines[0]);
            if (LAYOUT_FORMAT_VERSION != layoutFormatVersion) {
                throw new IOException("Metadata version not compatible. Expected " 
                        + LAYOUT_FORMAT_VERSION + ", but got " + layoutFormatVersion);
            }

            if (lines.length < 2) {
                throw new IOException("Ledger manager and its version absent from layout: " + layout);
            }

            String[] parts = lines[1].split(splitter);
            if (parts.length != 2) {
                throw new IOException("Invalid Ledger Manager defined in layout : " + layout);
            }
            // ledger manager class
            String managerType = parts[0];
            // ledger manager version
            int managerVersion = new Integer(parts[1]);
            return new LedgerLayout(managerType, managerVersion);
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (!(obj instanceof LedgerLayout)) {
            return false;
        }
        LedgerLayout other = (LedgerLayout)obj;
        return managerType.equals(other.managerType) &&
            managerVersion == other.managerVersion;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LV").append(layoutFormatVersion).append(":")
            .append(",Type:").append(managerType).append(":")
            .append(managerVersion);
        return sb.toString();
    }
}
