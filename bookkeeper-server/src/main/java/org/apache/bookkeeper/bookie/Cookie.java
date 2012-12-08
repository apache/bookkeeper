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

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;

import java.net.UnknownHostException;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.DataFormats.CookieFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    static Logger LOG = LoggerFactory.getLogger(Cookie.class);

    static final int CURRENT_COOKIE_LAYOUT_VERSION = 4;
    private int layoutVersion = 0;
    private String bookieHost = null;
    private String journalDir = null;
    private String ledgerDirs = null;
    private int znodeVersion = -1;
    private String instanceId = null;

    private Cookie() {
    }

    public void verify(Cookie c) throws BookieException.InvalidCookieException {
        String errMsg;
        if (c.layoutVersion < 3 && c.layoutVersion != layoutVersion) {
            errMsg = "Cookie is of too old version " + c.layoutVersion;
            LOG.error(errMsg);
            throw new BookieException.InvalidCookieException(errMsg);
        } else if (!(c.layoutVersion >= 3 && c.bookieHost.equals(bookieHost)
                && c.journalDir.equals(journalDir) && c.ledgerDirs
                    .equals(ledgerDirs))) {
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

    private static Cookie parse(BufferedReader reader) throws IOException {
        Cookie c = new Cookie();
        String line = reader.readLine();
        if (null == line) {
            throw new EOFException("Exception in parsing cookie");
        }
        try {
            c.layoutVersion = Integer.parseInt(line.trim());
        } catch (NumberFormatException e) {
            throw new IOException("Invalid string '" + line.trim()
                    + "', cannot parse cookie.");
        }
        if (c.layoutVersion == 3) {
            c.bookieHost = reader.readLine();
            c.journalDir = reader.readLine();
            c.ledgerDirs = reader.readLine();
        } else if (c.layoutVersion >= 4) {
            CookieFormat.Builder builder = CookieFormat.newBuilder();
            TextFormat.merge(reader, builder);
            CookieFormat data = builder.build();
            c.bookieHost = data.getBookieHost();
            c.journalDir = data.getJournalDir();
            c.ledgerDirs = data.getLedgerDirs();
            // Since InstanceId is optional
            if (null != data.getInstanceId() && !data.getInstanceId().isEmpty()) {
                c.instanceId = data.getInstanceId();
            }
        }
        return c;
    }

    void writeToDirectory(File directory) throws IOException {
        File versionFile = new File(directory,
                BookKeeperConstants.VERSION_FILENAME);

        FileOutputStream fos = new FileOutputStream(versionFile);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fos));
            bw.write(toString());
        } finally {
            if (bw != null) {
                bw.close();
            }
            fos.close();
        }
    }

    void writeToZooKeeper(ZooKeeper zk, ServerConfiguration conf)
            throws KeeperException, InterruptedException, UnknownHostException {
        String bookieCookiePath = conf.getZkLedgersRootPath() + "/"
                + BookKeeperConstants.COOKIE_NODE;
        String zkPath = getZkPath(conf);
        byte[] data = toString().getBytes();
        if (znodeVersion != -1) {
            zk.setData(zkPath, data, znodeVersion);
        } else {
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
            Stat stat = zk.exists(zkPath, false);
            this.znodeVersion = stat.getVersion();
        }
    }

    void deleteFromZooKeeper(ZooKeeper zk, ServerConfiguration conf)
            throws KeeperException, InterruptedException, UnknownHostException {
        String zkPath = getZkPath(conf);
        if (znodeVersion != -1) {
            zk.delete(zkPath, znodeVersion);
        }
        znodeVersion = -1;
    }

    static Cookie generateCookie(ServerConfiguration conf)
            throws UnknownHostException {
        Cookie c = new Cookie();
        c.layoutVersion = CURRENT_COOKIE_LAYOUT_VERSION;
        c.bookieHost = StringUtils.addrToString(Bookie.getBookieAddress(conf));
        c.journalDir = conf.getJournalDirName();
        StringBuilder b = new StringBuilder();
        String[] dirs = conf.getLedgerDirNames();
        b.append(dirs.length);
        for (String d : dirs) {
            b.append("\t").append(d);
        }
        c.ledgerDirs = b.toString();
        return c;
    }

    static Cookie readFromZooKeeper(ZooKeeper zk, ServerConfiguration conf)
            throws KeeperException, InterruptedException, IOException, UnknownHostException {
        String zkPath = getZkPath(conf);

        Stat stat = zk.exists(zkPath, false);
        byte[] data = zk.getData(zkPath, false, stat);
        BufferedReader reader = new BufferedReader(new StringReader(new String(
                data)));
        try {
            Cookie c = parse(reader);
            c.znodeVersion = stat.getVersion();
            return c;
        } finally {
            reader.close();
        }
    }

    static Cookie readFromDirectory(File directory) throws IOException {
        File versionFile = new File(directory,
                BookKeeperConstants.VERSION_FILENAME);
        BufferedReader reader = new BufferedReader(new FileReader(versionFile));
        try {
            return parse(reader);
        } finally {
            reader.close();
        }
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    private static String getZkPath(ServerConfiguration conf)
            throws UnknownHostException {
        String bookieCookiePath = conf.getZkLedgersRootPath() + "/"
                + BookKeeperConstants.COOKIE_NODE;
        return bookieCookiePath + "/" + StringUtils.addrToString(Bookie.getBookieAddress(conf));
    }
}
