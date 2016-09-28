/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.bookie;

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Set;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.CookieFormat;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.protobuf.TextFormat;

/**
 * When a bookie starts for the first time it generates  a cookie, and stores
 * the cookie in zookeeper as well as in the each of the local filesystem
 * directories it uses. This cookie is used to ensure that for the life of the
 * bookie, its configuration stays the same. If any of the bookie directories
 * becomes unavailable, the bookie becomes unavailable. If the bookie changes
 * port, it must also reset all of its data.
 *
 * This is done to ensure data integrity. Without the cookie a bookie could
 * start with one of its ledger directories missing, so data would be missing,
 * but the bookie would be up, so the client would think that everything is ok
 * with the cluster. It's better to fail early and obviously.
 */
class Cookie {
    private final static Logger LOG = LoggerFactory.getLogger(Cookie.class);

    static final int CURRENT_COOKIE_LAYOUT_VERSION = 4;
    private final int layoutVersion;
    private final String bookieHost;
    private final String journalDir;
    private final String ledgerDirs;
    private final String instanceId;
    private static final String SEPARATOR = "\t";

    private Cookie(int layoutVersion, String bookieHost, String journalDir, String ledgerDirs, String instanceId) {
        this.layoutVersion = layoutVersion;
        this.bookieHost = bookieHost;
        this.journalDir = journalDir;
        this.ledgerDirs = ledgerDirs;
        this.instanceId = instanceId;
    }

    private static String encodeDirPaths(String[] dirs) {
        StringBuilder b = new StringBuilder();
        b.append(dirs.length);
        for (String d : dirs) {
            b.append(SEPARATOR).append(d);
        }
        return b.toString();
    }

    private static String[] decodeDirPathFromCookie(String s) {
        // the first part of the string contains a count of how many
        // directories are present; to skip it, we look for subString
        // from the first '/'
        return s.substring(s.indexOf(SEPARATOR)+SEPARATOR.length()).split(SEPARATOR);
    }

    String[] getLedgerDirPathsFromCookie() {
        return decodeDirPathFromCookie(ledgerDirs);
    }

    /**
     * Receives 2 String arrays, that each contain a list of directory paths,
     * and checks if first is a super set of the second.
     *
     * @param superSet
     * @param subSet
     * @return true if s1 is a superSet of s2; false otherwise
     */
    private boolean isSuperSet(String[] s1, String[] s2) {
        Set<String> superSet = Sets.newHashSet(s1);
        Set<String> subSet = Sets.newHashSet(s2);
        return superSet.containsAll(subSet);
    }

    private boolean verifyLedgerDirs(Cookie c, boolean checkIfSuperSet) {
        if (checkIfSuperSet == false) {
            return ledgerDirs.equals(c.ledgerDirs);
        } else {
            return isSuperSet(decodeDirPathFromCookie(ledgerDirs), decodeDirPathFromCookie(c.ledgerDirs));
        }
    }

    private void verifyInternal(Cookie c, boolean checkIfSuperSet) throws BookieException.InvalidCookieException {
        String errMsg;
        if (c.layoutVersion < 3 && c.layoutVersion != layoutVersion) {
            errMsg = "Cookie is of too old version " + c.layoutVersion;
            LOG.error(errMsg);
            throw new BookieException.InvalidCookieException(errMsg);
        } else if (!(c.layoutVersion >= 3 && c.bookieHost.equals(bookieHost)
            && c.journalDir.equals(journalDir) && verifyLedgerDirs(c, checkIfSuperSet))) {
            errMsg = "Cookie [" + this + "] is not matching with [" + c + "]";
            throw new BookieException.InvalidCookieException(errMsg);
        } else if ((instanceId == null && c.instanceId != null)
                || (instanceId != null && !instanceId.equals(c.instanceId))) {
            // instanceId should be same in both cookies
            errMsg = "instanceId " + instanceId
                    + " is not matching with " + c.instanceId;
            throw new BookieException.InvalidCookieException(errMsg);
        }
    }

    public void verify(Cookie c) throws BookieException.InvalidCookieException {
        verifyInternal(c, false);
    }

    public void verifyIsSuperSet(Cookie c) throws BookieException.InvalidCookieException {
        verifyInternal(c, true);
    }

    public String toString() {
        if (layoutVersion <= 3) {
            return toStringVersion3();
        }
        CookieFormat.Builder builder = CookieFormat.newBuilder();
        builder.setBookieHost(bookieHost);
        builder.setJournalDir(journalDir);
        builder.setLedgerDirs(ledgerDirs);
        if (null != instanceId) {
            builder.setInstanceId(instanceId);
        }
        StringBuilder b = new StringBuilder();
        b.append(CURRENT_COOKIE_LAYOUT_VERSION).append("\n");
        b.append(TextFormat.printToString(builder.build()));
        return b.toString();
    }

    private String toStringVersion3() {
        StringBuilder b = new StringBuilder();
        b.append(CURRENT_COOKIE_LAYOUT_VERSION).append("\n")
            .append(bookieHost).append("\n")
            .append(journalDir).append("\n")
            .append(ledgerDirs).append("\n");
        return b.toString();
    }

    private static Builder parse(BufferedReader reader) throws IOException {
        Builder cBuilder = Cookie.newBuilder();
        int layoutVersion = 0;
        String line = reader.readLine();
        if (null == line) {
            throw new EOFException("Exception in parsing cookie");
        }
        try {
            layoutVersion = Integer.parseInt(line.trim());
            cBuilder.setLayoutVersion(layoutVersion);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid string '" + line.trim()
                    + "', cannot parse cookie.");
        }
        if (layoutVersion == 3) {
            cBuilder.setBookieHost(reader.readLine());
            cBuilder.setJournalDir(reader.readLine());
            cBuilder.setLedgerDirs(reader.readLine());
        } else if (layoutVersion >= 4) {
            CookieFormat.Builder cfBuilder = CookieFormat.newBuilder();
            TextFormat.merge(reader, cfBuilder);
            CookieFormat data = cfBuilder.build();
            cBuilder.setBookieHost(data.getBookieHost());
            cBuilder.setJournalDir(data.getJournalDir());
            cBuilder.setLedgerDirs(data.getLedgerDirs());
            // Since InstanceId is optional
            if (null != data.getInstanceId() && !data.getInstanceId().isEmpty()) {
                cBuilder.setInstanceId(data.getInstanceId());
            }
        }
        return cBuilder;
    }

    void writeToDirectory(File directory) throws IOException {
        File versionFile = new File(directory,
                BookKeeperConstants.VERSION_FILENAME);

        FileOutputStream fos = new FileOutputStream(versionFile);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
            bw.write(toString());
        } finally {
            if (bw != null) {
                bw.close();
            }
            fos.close();
        }
    }

    /**
     * Writes cookie details to ZooKeeper
     *
     * @param zk
     *            ZooKeeper instance
     * @param conf
     *            configuration
     * @param version
     *            version
     *
     * @throws KeeperException
     * @throws InterruptedException
     * @throws UnknownHostException
     */
    void writeToZooKeeper(ZooKeeper zk, ServerConfiguration conf, Version version)
            throws KeeperException, InterruptedException, UnknownHostException {
        String bookieCookiePath = conf.getZkLedgersRootPath() + "/"
                + BookKeeperConstants.COOKIE_NODE;
        String zkPath = getZkPath(conf);
        byte[] data = toString().getBytes(UTF_8);
        if (Version.NEW == version) {
            if (zk.exists(bookieCookiePath, false) == null) {
                try {
                    zk.create(bookieCookiePath, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nne) {
                    LOG.info("More than one bookie tried to create {} at once. Safe to ignore",
                            bookieCookiePath);
                }
            }
            zk.create(zkPath, data,
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            if (!(version instanceof ZkVersion)) {
                throw new IllegalArgumentException("Invalid version type, expected ZkVersion type");
            }
            zk.setData(zkPath, data, ((ZkVersion) version).getZnodeVersion());
        }
    }

    /**
     * Deletes cookie from ZooKeeper and sets znode version to DEFAULT_COOKIE_ZNODE_VERSION
     *
     * @param zk
     *            ZooKeeper instance
     * @param conf
     *            configuration
     * @param version
     *            zookeeper version
     *
     * @throws KeeperException
     * @throws InterruptedException
     * @throws UnknownHostException
     */
    public void deleteFromZooKeeper(ZooKeeper zk, ServerConfiguration conf, Version version) throws KeeperException,
            InterruptedException, UnknownHostException {
        BookieSocketAddress address = Bookie.getBookieAddress(conf);
        deleteFromZooKeeper(zk, conf, address, version);
    }

    /**
     * Delete cookie from zookeeper
     *
     * @param zk zookeeper client
     * @param conf configuration instance
     * @param address bookie address
     * @param version cookie version
     * @throws KeeperException
     * @throws InterruptedException
     * @throws UnknownHostException
     */
    public void deleteFromZooKeeper(ZooKeeper zk, AbstractConfiguration conf,
                                    BookieSocketAddress address, Version version)
            throws KeeperException, InterruptedException, UnknownHostException {
        if (!(version instanceof ZkVersion)) {
            throw new IllegalArgumentException("Invalid version type, expected ZkVersion type");
        }

        String zkPath = getZkPath(conf, address);
        zk.delete(zkPath, ((ZkVersion)version).getZnodeVersion());
        LOG.info("Removed cookie from {} for bookie {}.", conf.getZkLedgersRootPath(), address);
    }

    /**
     * Generate cookie from the given configuration
     *
     * @param conf
     *            configuration
     *
     * @return cookie builder object
     *
     * @throws UnknownHostException
     */
    static Builder generateCookie(ServerConfiguration conf)
            throws UnknownHostException {
        Builder builder = Cookie.newBuilder();
        builder.setLayoutVersion(CURRENT_COOKIE_LAYOUT_VERSION);
        builder.setBookieHost(Bookie.getBookieAddress(conf).toString());
        builder.setJournalDir(conf.getJournalDirName());
        builder.setLedgerDirs(encodeDirPaths(conf.getLedgerDirNames()));
        return builder;
    }

    /**
     * Read cookie from ZooKeeper.
     *
     * @param zk
     *            ZooKeeper instance
     * @param conf
     *            configuration
     *
     * @return versioned cookie object
     *
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     * @throws UnknownHostException
     */
    static Versioned<Cookie> readFromZooKeeper(ZooKeeper zk, ServerConfiguration conf)
            throws KeeperException, InterruptedException, IOException, UnknownHostException {
        return readFromZooKeeper(zk, conf, Bookie.getBookieAddress(conf));
    }

    /**
     * Read cookie from zookeeper for a given bookie <i>address</i>
     *
     * @param zk zookeeper client
     * @param conf configuration instance
     * @param address bookie address
     * @return versioned cookie object
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     * @throws UnknownHostException
     */
    static Versioned<Cookie> readFromZooKeeper(ZooKeeper zk, AbstractConfiguration conf, BookieSocketAddress address)
            throws KeeperException, InterruptedException, IOException, UnknownHostException {
        String zkPath = getZkPath(conf, address);

        Stat stat = zk.exists(zkPath, false);
        byte[] data = zk.getData(zkPath, false, stat);
        BufferedReader reader = new BufferedReader(new StringReader(new String(data, UTF_8)));
        try {
            Builder builder = parse(reader);
            Cookie cookie = builder.build();
            // sets stat version from ZooKeeper
            ZkVersion version = new ZkVersion(stat.getVersion());
            return new Versioned<Cookie>(cookie, version);
        } finally {
            reader.close();
        }
    }

    /**
     * Returns cookie from the given directory
     *
     * @param directory
     *            directory
     *
     * @return cookie object
     *
     * @throws IOException
     */
    static Cookie readFromDirectory(File directory) throws IOException {
        File versionFile = new File(directory,
                BookKeeperConstants.VERSION_FILENAME);
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(versionFile), UTF_8));
        try {
            return parse(reader).build();
        } finally {
            reader.close();
        }
    }

    /**
     * Returns cookie path in zookeeper
     *
     * @param conf
     *            configuration
     *          
     * @return cookie zk path
     *
     * @throws UnknownHostException
     */
    static String getZkPath(ServerConfiguration conf)
            throws UnknownHostException {
        return getZkPath(conf, Bookie.getBookieAddress(conf));
    }

    /**
     * Return cookie path for a given bookie <i>address</i>
     *
     * @param conf configuration
     * @param address bookie address
     * @return cookie path for bookie
     */
    static String getZkPath(AbstractConfiguration conf, BookieSocketAddress address) {
        String bookieCookiePath = conf.getZkLedgersRootPath() + "/"
                + BookKeeperConstants.COOKIE_NODE;
        return bookieCookiePath + "/" + address;
    }

    /**
     * Check whether the 'bookieHost' was created using a hostname or an IP
     * address. Represent as 'hostname/IPaddress' if the InetSocketAddress was
     * created using hostname. Represent as '/IPaddress' if the
     * InetSocketAddress was created using an IPaddress
     * 
     * @return true if the 'bookieHost' was created using an IP address, false
     *         if the 'bookieHost' was created using a hostname
     */
    public boolean isBookieHostCreatedFromIp() throws IOException {
        String parts[] = bookieHost.split(":");
        if (parts.length != 2) {
            throw new IOException(bookieHost + " does not have the form host:port");
        }
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new IOException(bookieHost + " does not have the form host:port");
        }

        InetSocketAddress addr = new InetSocketAddress(parts[0], port);
        return addr.toString().startsWith("/");
    }

    /**
     * Cookie builder
     */
    public static class Builder {
        private int layoutVersion = 0;
        private String bookieHost = null;
        private String journalDir = null;
        private String ledgerDirs = null;
        private String instanceId = null;

        private Builder() {
        }

        private Builder(int layoutVersion, String bookieHost, String journalDir, String ledgerDirs, String instanceId) {
            this.layoutVersion = layoutVersion;
            this.bookieHost = bookieHost;
            this.journalDir = journalDir;
            this.ledgerDirs = ledgerDirs;
            this.instanceId = instanceId;
        }

        public Builder setLayoutVersion(int layoutVersion) {
            this.layoutVersion = layoutVersion;
            return this;
        }

        public Builder setBookieHost(String bookieHost) {
            this.bookieHost = bookieHost;
            return this;
        }

        public Builder setJournalDir(String journalDir) {
            this.journalDir = journalDir;
            return this;
        }

        public Builder setLedgerDirs(String ledgerDirs) {
            this.ledgerDirs = ledgerDirs;
            return this;
        }

        public Builder setInstanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Cookie build() {
            return new Cookie(layoutVersion, bookieHost, journalDir, ledgerDirs, instanceId);
        }
    }

    /**
     * Returns Cookie builder
     * 
     * @return cookie builder
     */
    static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Returns Cookie builder with the copy of given oldCookie
     *
     * @param oldCookie
     *            build new cookie from this cookie
     * @return cookie builder
     */
    static Builder newBuilder(Cookie oldCookie) {
        return new Builder(oldCookie.layoutVersion, oldCookie.bookieHost, oldCookie.journalDir, oldCookie.ledgerDirs,
                oldCookie.instanceId);
    }
}
