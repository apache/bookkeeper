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

import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates ledger layout information that is persistently stored
 * in zookeeper. It provides parsing and serialization methods of such information.
 *
 */
class LedgerLayout {
    static final Logger LOG = LoggerFactory.getLogger(LedgerLayout.class);

   
    // version of compability layout version
    public static final int LAYOUT_MIN_COMPAT_VERSION = 1;
    // version of ledger layout metadata
    public static final int LAYOUT_FORMAT_VERSION = 2;

    /**
     * Read ledger layout from zookeeper
     *
     * @param zk            ZooKeeper Client
     * @param ledgersRoot   Root of the ledger namespace to check
     * @return ledger layout, or null if none set in zookeeper
     */
    public static LedgerLayout readLayout(final ZooKeeper zk, final String ledgersRoot)
            throws IOException, KeeperException {
        String ledgersLayout = ledgersRoot + "/" + BookKeeperConstants.LAYOUT_ZNODE;

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

    // ledger manager factory class
    private String managerFactoryCls;
    // ledger manager version
    private int managerVersion;

    // layout version of how to store layout information
    private int layoutFormatVersion = LAYOUT_FORMAT_VERSION;

    /**
     * Ledger Layout Constructor
     *
     * @param managerFactoryCls
     *          Ledger Manager Factory Class
     * @param managerVersion
     *          Ledger Manager Version
     * @param layoutFormatVersion
     *          Ledger Layout Format Version
     */
    public LedgerLayout(String managerFactoryCls, int managerVersion) {
        this(managerFactoryCls, managerVersion, LAYOUT_FORMAT_VERSION);
    }

    LedgerLayout(String managerFactoryCls, int managerVersion,
                 int layoutVersion) {
        this.managerFactoryCls = managerFactoryCls;
        this.managerVersion = managerVersion;
        this.layoutFormatVersion = layoutVersion;
    }

    /**
     * Get Ledger Manager Type
     *
     * @return ledger manager type
     * @deprecated replaced by {@link #getManagerFactoryClass()}
     */
    @Deprecated
    public String getManagerType() {
        // pre V2 layout store as manager type
        return this.managerFactoryCls;
    }

    /**
     * Get ledger manager factory class
     *
     * @return ledger manager factory class
     */
    public String getManagerFactoryClass() {
        return this.managerFactoryCls;
    }

    public int getManagerVersion() {
        return this.managerVersion;
    }

    /**
     * Return layout format version
     *
     * @return layout format version
     */
    public int getLayoutFormatVersion() {
        return this.layoutFormatVersion;
    }

    /**
     * Store the ledger layout into zookeeper
     */
    public void store(final ZooKeeper zk, String ledgersRoot) 
            throws IOException, KeeperException, InterruptedException {
        String ledgersLayout = ledgersRoot + "/"
                + BookKeeperConstants.LAYOUT_ZNODE;
        zk.create(ledgersLayout, serialize(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    /**
     * Delete the LAYOUT from zookeeper
     */
    public void delete(final ZooKeeper zk, String ledgersRoot)
            throws KeeperException, InterruptedException {
        String ledgersLayout = ledgersRoot + "/"
                + BookKeeperConstants.LAYOUT_ZNODE;
        zk.delete(ledgersLayout, -1);
    }

    /**
     * Generates a byte array based on the LedgerLayout object.
     *
     * @return byte[]
     */
    private byte[] serialize() throws IOException {
        String s =
          new StringBuilder().append(layoutFormatVersion).append(lSplitter)
              .append(managerFactoryCls).append(splitter).append(managerVersion).toString();

        LOG.debug("Serialized layout info: {}", s);
        return s.getBytes("UTF-8");
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

        LOG.debug("Parsing Layout: {}", layout);

        String lines[] = layout.split(lSplitter);

        try {
            int layoutFormatVersion = new Integer(lines[0]);
            if (LAYOUT_FORMAT_VERSION < layoutFormatVersion ||
                LAYOUT_MIN_COMPAT_VERSION > layoutFormatVersion) {
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
            // ledger manager factory class
            String managerFactoryCls = parts[0];
            // ledger manager version
            int managerVersion = new Integer(parts[1]);
            return new LedgerLayout(managerFactoryCls, managerVersion, layoutFormatVersion);
        } catch (NumberFormatException e) {
            throw new IOException("Could not parse layout '" + layout + "'", e);
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
        return managerFactoryCls.equals(other.managerFactoryCls)
            && managerVersion == other.managerVersion;
    }

    @Override
    public int hashCode() {
        return (managerFactoryCls + managerVersion).hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LV").append(layoutFormatVersion).append(":")
            .append(",Type:").append(managerFactoryCls).append(":")
            .append(managerVersion);
        return sb.toString();
    }
}
