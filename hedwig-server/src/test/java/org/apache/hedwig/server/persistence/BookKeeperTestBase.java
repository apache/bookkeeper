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
package org.apache.hedwig.server.persistence;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.test.PortManager;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hedwig.util.FileUtils;
import org.apache.hedwig.zookeeper.ZooKeeperTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a base class for any tests that require a BookKeeper client/server
 * setup.
 *
 */
public class BookKeeperTestBase extends ZooKeeperTestBase {
    private static Logger LOG = LoggerFactory.getLogger(BookKeeperTestBase.class);

    class TestBookie extends Bookie {
        final long readDelay;

        public TestBookie(ServerConfiguration conf, long readDelay)
            throws IOException, KeeperException, InterruptedException, BookieException {
            super(conf);
            this.readDelay = readDelay;
        }

        @Override
        public ByteBuffer readEntry(long ledgerId, long entryId)
            throws IOException, NoLedgerException {
            if (readDelay > 0) {
                try {
                    Thread.sleep(readDelay);
                } catch (InterruptedException ie) {
                }
            }
            return super.readEntry(ledgerId, entryId);
        }
    }

    class TestBookieServer extends BookieServer {
        public TestBookieServer(ServerConfiguration conf)
            throws IOException,
                KeeperException, InterruptedException, BookieException,
                UnavailableException, CompatibilityException {
            super(conf);
        }

        protected Bookie newBookie(ServerConfiguration conf)
            throws IOException, KeeperException, InterruptedException, BookieException {
            return new TestBookie(conf, readDelay);
        }
    }

    // BookKeeper Server variables
    private List<BookieServer> bookiesList;
    private List<ServerConfiguration> bkConfsList;

    // String constants used for creating the bookie server files.
    private static final String PREFIX = "bookie";
    private static final String SUFFIX = "test";

    // readDelay
    protected long readDelay;

    // Variable to decide how many bookie servers to set up.
    private final int numBookies;
    // BookKeeper client instance
    protected BookKeeper bk;

    protected ServerConfiguration baseConf = new ServerConfiguration();
    protected ClientConfiguration baseClientConf = new ClientConfiguration();

    // Constructor
    public BookKeeperTestBase(int numBookies) {
        this(numBookies, 0L);
    }

    public BookKeeperTestBase(int numBookies, long readDelay) {
        this.numBookies = numBookies;
        this.readDelay = readDelay;
    }

    public BookKeeperTestBase() {
        // By default, use 3 bookies.
        this(3);
    }

    // Getter for the ZooKeeper client instance that the parent class sets up.
    protected ZooKeeper getZooKeeperClient() {
        return zk;
    }

    // Give junit a fake test so that its happy
    @Test(timeout=60000)
    public void testNothing() throws Exception {

    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Initialize the zk client with values
        try {
            zk.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            LOG.error("Error setting up", e);
        } catch (InterruptedException e) {
            LOG.error("Error setting up", e);
        }

        // Create Bookie Servers
        bookiesList = new LinkedList<BookieServer>();
        bkConfsList = new LinkedList<ServerConfiguration>();

        for (int i = 0; i < numBookies; i++) {
            startUpNewBookieServer();
        }

        // Create the BookKeeper client
        bk = new BookKeeper(hostPort);
    }

    public String getZkHostPort() {
        return hostPort;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        // Shutdown all of the bookie servers
        for (BookieServer bs : bookiesList) {
            bs.shutdown();
        }
        // Close the BookKeeper client
        bk.close();
        super.tearDown();
    }

    public void stopAllBookieServers() throws Exception {
        for (BookieServer bs : bookiesList) {
            bs.shutdown();
        }
        bookiesList.clear();
    }

    public void startAllBookieServers() throws Exception {
        for (ServerConfiguration conf : bkConfsList) {
            bookiesList.add(startBookie(conf));
        }
    }

    public void suspendAllBookieServers() throws Exception {
        for (BookieServer bs : bookiesList) {
            bs.suspendProcessing();
        }
    }

    public void resumeAllBookieServers() throws Exception {
        for (BookieServer bs : bookiesList) {
            bs.resumeProcessing();
        }
    }

    public void tearDownOneBookieServer() throws Exception {
        Random r = new Random();
        int bi = r.nextInt(bookiesList.size());
        BookieServer bs = bookiesList.get(bi);
        bs.shutdown();
        bookiesList.remove(bi);
        bkConfsList.remove(bi);
    }
    
    public void startUpNewBookieServer() throws Exception {
        int port = PortManager.nextFreePort();
        File tmpDir = FileUtils.createTempDirectory(
                PREFIX + port, SUFFIX);
        ServerConfiguration conf = newServerConfiguration(
                port, hostPort, tmpDir, new File[] { tmpDir });
        bookiesList.add(startBookie(conf));
        bkConfsList.add(conf);
    }

    /**
     * Helper method to startup a bookie server using a configuration object
     *
     * @param conf
     *            Server Configuration Object
     *
     */
    private BookieServer startBookie(ServerConfiguration conf) throws Exception {
        BookieServer server = new TestBookieServer(conf);
        server.start();

        int port = conf.getBookiePort();
        while(zk.exists("/ledgers/available/" + InetAddress.getLocalHost().getHostAddress() + ":" + port, false) == null) {
            Thread.sleep(500);
        }

        return server;
    }

    protected ServerConfiguration newServerConfiguration(int port, String zkServers, File journalDir, File[] ledgerDirs) {
        ServerConfiguration conf = new ServerConfiguration(baseConf);
        conf.setBookiePort(port);
        conf.setZkServers(zkServers);
        conf.setJournalDirName(journalDir.getPath());
        String[] ledgerDirNames = new String[ledgerDirs.length];
        for (int i=0; i<ledgerDirs.length; i++) {
            ledgerDirNames[i] = ledgerDirs[i].getPath();
        }
        conf.setLedgerDirNames(ledgerDirNames);
        return conf;
    }

}
