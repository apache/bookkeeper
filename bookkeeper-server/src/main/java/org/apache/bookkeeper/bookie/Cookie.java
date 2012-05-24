/*
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Scanner;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.nio.ByteBuffer;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import org.apache.bookkeeper.conf.ServerConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    static final int CURRENT_COOKIE_LAYOUT_VERSION = 3;
    static final String COOKIE_NODE = "cookies";
    static final String VERSION_FILENAME = "VERSION";
    private int layoutVersion = 0;
    private String bookieHost = null;
    private String journalDir = null;
    private String ledgerDirs = null;
    private int znodeVersion = -1;

    private Cookie() {
    }

    public void verify(Cookie c)
            throws BookieException.InvalidCookieException {
        if (!(c.layoutVersion == layoutVersion
              && c.layoutVersion >= 3
              && c.bookieHost.equals(bookieHost)
              && c.journalDir.equals(journalDir)
              && c.ledgerDirs.equals(ledgerDirs))) {
            throw new BookieException.InvalidCookieException();
        }
    }

    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(CURRENT_COOKIE_LAYOUT_VERSION).append("\n")
            .append(bookieHost).append("\n")
            .append(journalDir).append("\n")
            .append(ledgerDirs).append("\n");
        return b.toString();
    }

    private static Cookie parse(Scanner s) throws IOException {
        Cookie c  = new Cookie();
        if (!s.hasNextInt()) {
            throw new IOException("Invalid string, cannot parse cookie.");
        }
        c.layoutVersion = s.nextInt();
        if (c.layoutVersion >= 3) {
            s.nextLine();
            c.bookieHost = s.nextLine();
            c.journalDir = s.nextLine();
            c.ledgerDirs = s.nextLine();
        }
        s.close();
        return c;
    }

    void writeToDirectory(File directory) throws IOException {
        File versionFile = new File(directory, VERSION_FILENAME);

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
        String bookieCookiePath = conf.getZkLedgersRootPath() + "/" + COOKIE_NODE;
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
        c.bookieHost = InetAddress.getLocalHost().getHostAddress() + ":" + conf.getBookiePort();
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
        Cookie c = parse(new Scanner(new String(data)));
        c.znodeVersion = stat.getVersion();
        return c;
    }

    static Cookie readFromDirectory(File directory) throws IOException {
        File versionFile = new File(directory, VERSION_FILENAME);
        return parse(new Scanner(versionFile));
    }

    private static String getZkPath(ServerConfiguration conf)
            throws UnknownHostException {
        String bookieCookiePath = conf.getZkLedgersRootPath() + "/" + COOKIE_NODE;
        return bookieCookiePath + "/" + InetAddress.getLocalHost().getHostAddress() + ":" + conf.getBookiePort();
    }
}
