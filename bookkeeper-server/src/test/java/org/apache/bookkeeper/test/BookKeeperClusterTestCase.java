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

package org.apache.bookkeeper.test;

import com.google.common.base.Stopwatch;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.metastore.InMemoryMetaStore;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.replication.Auditor;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class runs several bookie servers for testing.
 */
public abstract class BookKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(BookKeeperClusterTestCase.class);

    @Rule
    public final TestName runtime = new TestName();

    @Rule
    public final Timeout globalTimeout;

    // ZooKeeper related variables
    protected final ZooKeeperUtil zkUtil = new ZooKeeperUtil();
    protected ZooKeeper zkc;

    // BookKeeper related variables
    protected final List<File> tmpDirs = new LinkedList<File>();
    protected final List<BookieServer> bs = new LinkedList<BookieServer>();
    protected final List<ServerConfiguration> bsConfs = new LinkedList<ServerConfiguration>();
    protected int numBookies;
    protected BookKeeperTestClient bkc;

    protected final ServerConfiguration baseConf = TestBKConfiguration.newServerConfiguration();
    protected final ClientConfiguration baseClientConf = new ClientConfiguration();

    private final Map<BookieServer, AutoRecoveryMain> autoRecoveryProcesses = new HashMap<>();

    private boolean isAutoRecoveryEnabled;

    public BookKeeperClusterTestCase(int numBookies) {
        this(numBookies, 120);
    }

    public BookKeeperClusterTestCase(int numBookies, int testTimeoutSecs) {
        this.numBookies = numBookies;
        this.globalTimeout = Timeout.seconds(testTimeoutSecs);
        baseConf.setAllowLoopback(true);
    }

    @Before
    public void setUp() throws Exception {
        LOG.info("Setting up test {}", getClass());
        InMemoryMetaStore.reset();
        setMetastoreImplClass(baseConf);
        setMetastoreImplClass(baseClientConf);

        Stopwatch sw = Stopwatch.createStarted();
        try {
            // start zookeeper service
            startZKCluster();
            // start bookkeeper service
            startBKCluster();
            LOG.info("Setup testcase {} in {} ms.", runtime.getMethodName(), sw.elapsed(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            LOG.error("Error setting up", e);
            throw e;
        }
    }

    @After
    public void tearDown() throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        LOG.info("TearDown");
        // stop bookkeeper service
        stopBKCluster();
        // stop zookeeper service
        stopZKCluster();
        // cleanup temp dirs
        cleanupTempDirs();
        LOG.info("Tearing down test {} in {} ms.", runtime.getMethodName(), sw.elapsed(TimeUnit.MILLISECONDS));
    }

    protected File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tmpDirs.add(dir);
        return dir;
    }

    /**
     * Start zookeeper cluster
     *
     * @throws Exception
     */
    protected void startZKCluster() throws Exception {
        zkUtil.startServer();
        zkc = zkUtil.getZooKeeperClient();
    }

    /**
     * Stop zookeeper cluster
     *
     * @throws Exception
     */
    protected void stopZKCluster() throws Exception {
        zkUtil.killServer();
    }

    /**
     * Start cluster. Also, starts the auto recovery process for each bookie, if
     * isAutoRecoveryEnabled is true.
     *
     * @throws Exception
     */
    protected void startBKCluster() throws Exception {
        baseClientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        if (numBookies > 0) {
            bkc = new BookKeeperTestClient(baseClientConf);
        }

        // Create Bookie Servers (B1, B2, B3)
        for (int i = 0; i < numBookies; i++) {
            startNewBookie();
        }
    }

    /**
     * Stop cluster. Also, stops all the auto recovery processes for the bookie
     * cluster, if isAutoRecoveryEnabled is true.
     *
     * @throws Exception
     */
    protected void stopBKCluster() throws Exception {
        if (bkc != null) {
            bkc.close();
        }

        for (BookieServer server : bs) {
            server.shutdown();
            AutoRecoveryMain autoRecovery = autoRecoveryProcesses.get(server);
            if (autoRecovery != null && isAutoRecoveryEnabled()) {
                autoRecovery.shutdown();
                LOG.debug("Shutdown auto recovery for bookieserver:"
                        + server.getLocalAddress());
            }
        }
        bs.clear();
    }

    protected void cleanupTempDirs() throws Exception {
        for (File f : tmpDirs) {
            FileUtils.deleteDirectory(f);
        }
    }

    protected ServerConfiguration newServerConfiguration() throws Exception {
        File f = createTempDir("bookie", "test");

        int port;
        if (baseConf.isEnableLocalTransport() || !baseConf.getAllowEphemeralPorts()) {
            port = PortManager.nextFreePort();
        } else {
            port = 0;
        }
        return newServerConfiguration(port, zkUtil.getZooKeeperConnectString(),
                                      f, new File[] { f });
    }

    protected ClientConfiguration newClientConfiguration() {
        return new ClientConfiguration(baseConf);
    }

    protected ServerConfiguration newServerConfiguration(int port, String zkServers, File journalDir, File[] ledgerDirs) {
        ServerConfiguration conf = new ServerConfiguration(baseConf);
        conf.setBookiePort(port);
        conf.setZkServers(zkServers);
        conf.setAllowLoopback(true);
        conf.setJournalDirName(journalDir.getPath());
        String[] ledgerDirNames = new String[ledgerDirs.length];
        for (int i=0; i<ledgerDirs.length; i++) {
            ledgerDirNames[i] = ledgerDirs[i].getPath();
        }
        conf.setLedgerDirNames(ledgerDirNames);
        return conf;
    }

    protected void stopAllBookies() throws Exception {
        for (BookieServer server : bs) {
            server.shutdown();
        }
        bsConfs.clear();
        bs.clear();
        if (bkc != null) {
            bkc.close();
            bkc = null;
        }
    }

    protected void startAllBookies() throws Exception {
        for (ServerConfiguration conf : bsConfs) {
            bs.add(startBookie(conf));
        }
    }

    /**
     * Get bookie address for bookie at index
     */
    public BookieSocketAddress getBookie(int index) throws Exception {
        if (bs.size() <= index || index < 0) {
            throw new IllegalArgumentException("Invalid index, there are only " + bs.size()
                                               + " bookies. Asked for " + index);
        }
        return bs.get(index).getLocalAddress();
    }

    /**
     * Get bookie configuration for bookie
     */
    public ServerConfiguration getBkConf(BookieSocketAddress addr) throws Exception {
        int bkIndex = 0;
        for (BookieServer server : bs) {
            if (server.getLocalAddress().equals(addr)) {
                break;
            }
            ++bkIndex;
        }
        if (bkIndex < bs.size()) {
            return bsConfs.get(bkIndex);
        }
        return null;
    }

    /**
     * Kill a bookie by its socket address. Also, stops the autorecovery process
     * for the corresponding bookie server, if isAutoRecoveryEnabled is true.
     *
     * @param addr
     *            Socket Address
     * @return the configuration of killed bookie
     * @throws InterruptedException
     */
    public ServerConfiguration killBookie(BookieSocketAddress addr) throws Exception {
        BookieServer toRemove = null;
        int toRemoveIndex = 0;
        for (BookieServer server : bs) {
            if (server.getLocalAddress().equals(addr)) {
                server.shutdown();
                toRemove = server;
                break;
            }
            ++toRemoveIndex;
        }
        if (toRemove != null) {
            stopAutoRecoveryService(toRemove);
            bs.remove(toRemove);
            return bsConfs.remove(toRemoveIndex);
        }
        return null;
    }

    /**
     * Set the bookie identified by its socket address to readonly
     *
     * @param addr
     *          Socket Address
     * @return the configuration of killed bookie
     * @throws InterruptedException
     */
    public void setBookieToReadOnly(BookieSocketAddress addr) throws InterruptedException, UnknownHostException {
        for (BookieServer server : bs) {
            if (server.getLocalAddress().equals(addr)) {
                server.getBookie().doTransitionToReadOnlyMode();
                break;
            }
        }
    }

    /**
     * Kill a bookie by index. Also, stops the respective auto recovery process
     * for this bookie, if isAutoRecoveryEnabled is true.
     *
     * @param index
     *            Bookie Index
     * @return the configuration of killed bookie
     * @throws InterruptedException
     * @throws IOException
     */
    public ServerConfiguration killBookie(int index) throws Exception {
        if (index >= bs.size()) {
            throw new IOException("Bookie does not exist");
        }
        BookieServer server = bs.get(index);
        server.shutdown();
        stopAutoRecoveryService(server);
        bs.remove(server);
        return bsConfs.remove(index);
    }

    /**
     * Kill bookie by index and verify that it's stopped
     *
     * @param index index of bookie to kill
     *
     * @return configuration of killed bookie
     */
    public ServerConfiguration killBookieAndWaitForZK(int index) throws Exception {
        if (index >= bs.size()) {
            throw new IOException("Bookie does not exist");
        }
        BookieServer server = bs.get(index);
        ServerConfiguration ret = killBookie(index);
        while (zkc.exists(baseConf.getZkAvailableBookiesPath() + "/"
                + server.getLocalAddress().toString(), false) != null) {
            Thread.sleep(500);
        }
        return ret;
    }

    /**
     * Sleep a bookie
     *
     * @param addr
     *          Socket Address
     * @param seconds
     *          Sleep seconds
     * @return Count Down latch which will be counted down just after sleep begins
     * @throws InterruptedException
     * @throws IOException
     */
    public CountDownLatch sleepBookie(BookieSocketAddress addr, final int seconds)
            throws Exception {
        for (final BookieServer bookie : bs) {
            if (bookie.getLocalAddress().equals(addr)) {
                final CountDownLatch l = new CountDownLatch(1);
                Thread sleeper = new Thread() {
                        @Override
                        public void run() {
                            try {
                                bookie.suspendProcessing();
                                LOG.info("bookie {} is asleep", bookie.getLocalAddress());
                                l.countDown();
                                Thread.sleep(seconds*1000);
                                bookie.resumeProcessing();
                                LOG.info("bookie {} is awake", bookie.getLocalAddress());
                            } catch (Exception e) {
                                LOG.error("Error suspending bookie", e);
                            }
                        }
                    };
                sleeper.start();
                return l;
            }
        }
        throw new IOException("Bookie not found");
    }

    /**
     * Sleep a bookie until I count down the latch
     *
     * @param addr
     *          Socket Address
     * @param l
     *          Latch to wait on
     * @throws InterruptedException
     * @throws IOException
     */
    public void sleepBookie(BookieSocketAddress addr, final CountDownLatch l)
            throws InterruptedException, IOException {
        final CountDownLatch suspendLatch = new CountDownLatch(1);
        sleepBookie(addr, l, suspendLatch);
        suspendLatch.await();
    }

    public void sleepBookie(BookieSocketAddress addr, final CountDownLatch l, final CountDownLatch suspendLatch)
            throws InterruptedException, IOException {
        for (final BookieServer bookie : bs) {
            if (bookie.getLocalAddress().equals(addr)) {
                LOG.info("Sleep bookie {}.", addr);
                Thread sleeper = new Thread() {
                    @Override
                    public void run() {
                        try {
                            bookie.suspendProcessing();
                            if (null != suspendLatch) {
                                suspendLatch.countDown();
                            }
                            l.await();
                            bookie.resumeProcessing();
                        } catch (Exception e) {
                            LOG.error("Error suspending bookie", e);
                        }
                    }
                };
                sleeper.start();
                return;
            }
        }
        throw new IOException("Bookie not found");
    }

    /**
     * Restart bookie servers. Also restarts all the respective auto recovery
     * process, if isAutoRecoveryEnabled is true.
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws KeeperException
     * @throws BookieException
     */
    public void restartBookies()
            throws Exception {
        restartBookies(null);
    }

    /**
     * Restart a bookie. Also restart the respective auto recovery process,
     * if isAutoRecoveryEnabled is true.
     * 
     * @param addr
     * @throws InterruptedException
     * @throws IOException
     * @throws KeeperException
     * @throws BookieException
     */
    public void restartBookie(BookieSocketAddress addr) throws Exception {
        BookieServer toRemove = null;
        int toRemoveIndex = 0;
        for (BookieServer server : bs) {
            if (server.getLocalAddress().equals(addr)) {
                server.shutdown();
                toRemove = server;
                break;
            }
            ++toRemoveIndex;
        }
        if (toRemove != null) {
            stopAutoRecoveryService(toRemove);
            bs.remove(toRemove);
            ServerConfiguration newConfig = bsConfs.remove(toRemoveIndex);
            Thread.sleep(1000);
            bs.add(startBookie(newConfig));
            bsConfs.add(newConfig);
            return;
        }
        throw new IOException("Bookie not found");
    }

    /**
     * Restart bookie servers using new configuration settings. Also restart the
     * respective auto recovery process, if isAutoRecoveryEnabled is true.
     *
     * @param newConf
     *            New Configuration Settings
     * @throws InterruptedException
     * @throws IOException
     * @throws KeeperException
     * @throws BookieException
     */
    public void restartBookies(ServerConfiguration newConf)
            throws Exception {
        // shut down bookie server
        for (BookieServer server : bs) {
            server.shutdown();
            stopAutoRecoveryService(server);
        }
        bs.clear();
        Thread.sleep(1000);
        // restart them to ensure we can't
        for (ServerConfiguration conf : bsConfs) {
            // ensure the bookie port is loaded correctly
            int port = conf.getBookiePort();
            if (null != newConf) {
                conf.loadConf(newConf);
            }
            conf.setBookiePort(port);
            bs.add(startBookie(conf));
        }
    }

    /**
     * Helper method to startup a new bookie server with the indicated port
     * number. Also, starts the auto recovery process, if the
     * isAutoRecoveryEnabled is set true.
     *
     * @throws IOException
     */
    public int startNewBookie()
            throws Exception {
        ServerConfiguration conf = newServerConfiguration();
        bsConfs.add(conf);
        LOG.info("Starting new bookie on port: {}", conf.getBookiePort());
        bs.add(startBookie(conf));

        return conf.getBookiePort();
    }

    /**
     * Helper method to startup a bookie server using a configuration object.
     * Also, starts the auto recovery process if isAutoRecoveryEnabled is true.
     *
     * @param conf
     *            Server Configuration Object
     *
     */
    protected BookieServer startBookie(ServerConfiguration conf)
            throws Exception {
        BookieServer server = new BookieServer(conf);
        BookieSocketAddress address = Bookie.getBookieAddress(conf);
        if (bkc == null) {
            bkc = new BookKeeperTestClient(baseClientConf);
        }

        Future<?> waitForBookie = conf.isForceReadOnlyBookie()
            ? bkc.waitForReadOnlyBookie(address)
            : bkc.waitForWritableBookie(address);

        server.start();

        waitForBookie.get(30, TimeUnit.SECONDS);
        LOG.info("New bookie '{}' has been created.", address);

        try {
            startAutoRecovery(server, conf);
        } catch (CompatibilityException ce) {
            LOG.error("Exception while starting AutoRecovery!", ce);
        } catch (UnavailableException ue) {
            LOG.error("Exception while starting AutoRecovery!", ue);
        }
        return server;
    }

    /**
     * Start a bookie with the given bookie instance. Also, starts the auto
     * recovery for this bookie, if isAutoRecoveryEnabled is true.
     */
    protected BookieServer startBookie(ServerConfiguration conf, final Bookie b)
            throws Exception {
        BookieServer server = new BookieServer(conf) {
            @Override
            protected Bookie newBookie(ServerConfiguration conf) {
                return b;
            }
        };

        BookieSocketAddress address = Bookie.getBookieAddress(conf);
        if (bkc == null) {
            bkc = new BookKeeperTestClient(baseClientConf);
        }
        Future<?> waitForBookie = conf.isForceReadOnlyBookie()
            ? bkc.waitForReadOnlyBookie(address)
            : bkc.waitForWritableBookie(address);

        server.start();

        waitForBookie.get(30, TimeUnit.SECONDS);
        LOG.info("New bookie '{}' has been created.", address);

        try {
            startAutoRecovery(server, conf);
        } catch (CompatibilityException ce) {
            LOG.error("Exception while starting AutoRecovery!", ce);
        } catch (UnavailableException ue) {
            LOG.error("Exception while starting AutoRecovery!", ue);
        }
        return server;
    }

    public void setMetastoreImplClass(AbstractConfiguration conf) {
        conf.setMetastoreImplClass(InMemoryMetaStore.class.getName());
    }

    /**
     * Flags used to enable/disable the auto recovery process. If it is enabled,
     * starting the bookie server will starts the auto recovery process for that
     * bookie. Also, stopping bookie will stops the respective auto recovery
     * process.
     *
     * @param isAutoRecoveryEnabled
     *            Value true will enable the auto recovery process. Value false
     *            will disable the auto recovery process
     */
    public void setAutoRecoveryEnabled(boolean isAutoRecoveryEnabled) {
        this.isAutoRecoveryEnabled = isAutoRecoveryEnabled;
    }

    /**
     * Flag used to check whether auto recovery process is enabled/disabled. By
     * default the flag is false.
     *
     * @return true, if the auto recovery is enabled. Otherwise return false.
     */
    public boolean isAutoRecoveryEnabled() {
        return isAutoRecoveryEnabled;
    }

    private void startAutoRecovery(BookieServer bserver,
                                   ServerConfiguration conf) throws Exception {
        if (isAutoRecoveryEnabled()) {
            AutoRecoveryMain autoRecoveryProcess = new AutoRecoveryMain(conf);
            autoRecoveryProcess.start();
            autoRecoveryProcesses.put(bserver, autoRecoveryProcess);
            LOG.debug("Starting Auditor Recovery for the bookie:"
                    + bserver.getLocalAddress());
        }
    }

    private void stopAutoRecoveryService(BookieServer toRemove) throws Exception {
        AutoRecoveryMain autoRecoveryMain = autoRecoveryProcesses
                .remove(toRemove);
        if (null != autoRecoveryMain && isAutoRecoveryEnabled()) {
            autoRecoveryMain.shutdown();
            LOG.debug("Shutdown auto recovery for bookieserver:"
                    + toRemove.getLocalAddress());
        }
    }

    /**
     * Will starts the auto recovery process for the bookie servers. One auto
     * recovery process per each bookie server, if isAutoRecoveryEnabled is
     * enabled.
     */
    public void startReplicationService() throws Exception {
        int index = -1;
        for (BookieServer bserver : bs) {
            startAutoRecovery(bserver, bsConfs.get(++index));
        }
    }

    /**
     * Will stops all the auto recovery processes for the bookie cluster, if
     * isAutoRecoveryEnabled is true.
     */
    public void stopReplicationService() throws Exception{
        if(!isAutoRecoveryEnabled()){
            return;
        }
        for (Entry<BookieServer, AutoRecoveryMain> autoRecoveryProcess : autoRecoveryProcesses
                .entrySet()) {
            autoRecoveryProcess.getValue().shutdown();
            LOG.debug("Shutdown Auditor Recovery for the bookie:"
                    + autoRecoveryProcess.getKey().getLocalAddress());
        }
    }

    public Auditor getAuditor(int timeout, TimeUnit unit) throws Exception {
        final long timeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, unit);
        while (System.nanoTime() < timeoutAt) {
            for (AutoRecoveryMain p : autoRecoveryProcesses.values()) {
                Auditor a = p.getAuditor();
                if (a != null) {
                    return a;
                }
            }
            Thread.sleep(100);
        }
        throw new Exception("No auditor found");
    }

    /**
     * Check whether the InetSocketAddress was created using a hostname or an IP
     * address. Represent as 'hostname/IPaddress' if the InetSocketAddress was
     * created using hostname. Represent as '/IPaddress' if the
     * InetSocketAddress was created using an IPaddress
     *
     * @param addr
     *            inetaddress
     * @return true if the address was created using an IP address, false if the
     *         address was created using a hostname
     */
    public static boolean isCreatedFromIp(BookieSocketAddress addr) {
        return addr.getSocketAddress().toString().startsWith("/");
    }
}
