/*
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

package org.apache.bookkeeper.discover;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.util.BookKeeperConstants.COOKIE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.INSTANCEID;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.BookieIllegalOpException;
import org.apache.bookkeeper.bookie.BookieException.CookieNotFoundException;
import org.apache.bookkeeper.bookie.BookieException.MetadataStoreException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKInterruptedException;
import org.apache.bookkeeper.client.BKException.MetaStoreException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper Based {@link RegistrationManager}.
 */
@Slf4j
public class ZKRegistrationManager implements RegistrationManager {

    private static final Function<Throwable, BKException> EXCEPTION_FUNC = cause -> {
        if (cause instanceof BKException) {
            log.error("Failed to get bookie list : ", cause);
            return (BKException) cause;
        } else if (cause instanceof InterruptedException) {
            log.error("Interrupted reading bookie list : ", cause);
            return new BKInterruptedException();
        } else {
            return new MetaStoreException();
        }
    };

    private ServerConfiguration conf;
    private ZooKeeper zk;
    private List<ACL> zkAcls;
    private volatile boolean running = false;

    // cookie path
    private String cookiePath;
    // registration paths
    protected String bookieRegistrationPath;
    protected String bookieReadonlyRegistrationPath;
    // layout manager
    private LayoutManager layoutManager;

    private StatsLogger statsLogger;

    @Override
    public RegistrationManager initialize(ServerConfiguration conf,
                                          RegistrationListener listener,
                                          StatsLogger statsLogger)
            throws BookieException {
        if (null == conf.getZkServers()) {
            log.warn("No ZK servers passed to Bookie constructor so BookKeeper clients won't know about this server!");
            return null;
        }

        this.conf = conf;
        this.zkAcls = ZkUtils.getACLs(conf);
        this.statsLogger = statsLogger;

        this.cookiePath = conf.getZkLedgersRootPath() + "/" + COOKIE_NODE;
        this.bookieRegistrationPath = conf.getZkAvailableBookiesPath();
        this.bookieReadonlyRegistrationPath = this.bookieRegistrationPath + "/" + READONLY;

        try {
            this.zk = newZookeeper(conf, listener);
        } catch (InterruptedException | KeeperException | IOException e) {
            throw new MetadataStoreException(e);
        }

        this.layoutManager = new ZkLayoutManager(
            zk,
            conf.getZkLedgersRootPath(),
            zkAcls);

        return this;
    }

    @VisibleForTesting
    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    @VisibleForTesting
    public ZooKeeper getZk() {
        return this.zk;
    }

    /**
     * Create a new zookeeper client to zk cluster.
     *
     * <p>
     * Bookie Server just used zk client when syncing ledgers for garbage collection.
     * So when zk client is expired, it means this bookie server is not available in
     * bookie server list. The bookie client will be notified for its expiration. No
     * more bookie request will be sent to this server. So it's better to exit when zk
     * expired.
     * </p>
     * <p>
     * Since there are lots of bk operations cached in queue, so we wait for all the operations
     * are processed and quit. It is done by calling <b>shutdown</b>.
     * </p>
     *
     * @param conf server configuration
     *
     * @return zk client instance
     */
    private ZooKeeper newZookeeper(final ServerConfiguration conf, RegistrationListener listener)
        throws InterruptedException, KeeperException, IOException {
        Set<Watcher> watchers = new HashSet<Watcher>();
        watchers.add(event -> {
            if (!running) {
                // do nothing until first registration
                return;
            }
            // Check for expired connection.
            if (event.getType().equals(EventType.None) && event.getState().equals(KeeperState.Expired)) {
                listener.onRegistrationExpired();
            }
        });
        return ZooKeeperClient.newBuilder()
                .connectString(conf.getZkServers())
                .sessionTimeoutMs(conf.getZkTimeout())
                .watchers(watchers)
                .operationRetryPolicy(new BoundExponentialBackoffRetryPolicy(conf.getZkRetryBackoffStartMs(),
                        conf.getZkRetryBackoffMaxMs(), Integer.MAX_VALUE))
                .requestRateLimit(conf.getZkRequestRateLimit())
                .statsLogger(this.statsLogger.scope(BOOKIE_SCOPE))
                .build();
    }

    @Override
    public void close() {
        if (null != zk) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                log.warn("Interrupted on closing zookeeper client", e);
            }
        }
    }

    /**
     * Returns the CookiePath of the bookie in the ZooKeeper.
     *
     * @param bookieId bookie id
     * @return
     */
    public String getCookiePath(String bookieId) {
        return this.cookiePath + "/" + bookieId;
    }

    //
    // Registration Management
    //

    /**
     * Check existence of <i>regPath</i> and wait it expired if possible.
     *
     * @param regPath reg node path.
     * @return true if regPath exists, otherwise return false
     * @throws IOException if can't create reg path
     */
    protected boolean checkRegNodeAndWaitExpired(String regPath) throws IOException {
        final CountDownLatch prevNodeLatch = new CountDownLatch(1);
        Watcher zkPrevRegNodewatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // Check for prev znode deletion. Connection expiration is
                // not handling, since bookie has logic to shutdown.
                if (EventType.NodeDeleted == event.getType()) {
                    prevNodeLatch.countDown();
                }
            }
        };
        try {
            Stat stat = zk.exists(regPath, zkPrevRegNodewatcher);
            if (null != stat) {
                // if the ephemeral owner isn't current zookeeper client
                // wait for it to be expired.
                if (stat.getEphemeralOwner() != zk.getSessionId()) {
                    log.info("Previous bookie registration znode: {} exists, so waiting zk sessiontimeout:"
                            + " {} ms for znode deletion", regPath, conf.getZkTimeout());
                    // waiting for the previous bookie reg znode deletion
                    if (!prevNodeLatch.await(conf.getZkTimeout(), TimeUnit.MILLISECONDS)) {
                        throw new NodeExistsException(regPath);
                    } else {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        } catch (KeeperException ke) {
            log.error("ZK exception checking and wait ephemeral znode {} expired : ", regPath, ke);
            throw new IOException("ZK exception checking and wait ephemeral znode "
                    + regPath + " expired", ke);
        } catch (InterruptedException ie) {
            log.error("Interrupted checking and wait ephemeral znode {} expired : ", regPath, ie);
            throw new IOException("Interrupted checking and wait ephemeral znode "
                    + regPath + " expired", ie);
        }
    }

    @Override
    public void registerBookie(String bookieId, boolean readOnly) throws BookieException {
        if (!readOnly) {
            String regPath = bookieRegistrationPath + "/" + bookieId;
            doRegisterBookie(regPath);
        } else {
            doRegisterReadOnlyBookie(bookieId);
        }
    }

    private void doRegisterBookie(String regPath) throws BookieException {
        // ZK ephemeral node for this Bookie.
        try {
            if (!checkRegNodeAndWaitExpired(regPath)) {
                // Create the ZK ephemeral node for this Bookie.
                zk.create(regPath, new byte[0], zkAcls, CreateMode.EPHEMERAL);
            }
        } catch (KeeperException ke) {
            log.error("ZK exception registering ephemeral Znode for Bookie!", ke);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new MetadataStoreException(ke);
        } catch (InterruptedException ie) {
            log.error("Interrupted exception registering ephemeral Znode for Bookie!", ie);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new MetadataStoreException(ie);
        } catch (IOException e) {
            throw new MetadataStoreException(e);
        }
    }

    private void doRegisterReadOnlyBookie(String bookieId) throws BookieException {
        try {
            if (null == zk.exists(this.bookieReadonlyRegistrationPath, false)) {
                try {
                    zk.create(this.bookieReadonlyRegistrationPath, new byte[0],
                              zkAcls, CreateMode.PERSISTENT);
                } catch (NodeExistsException e) {
                    // this node is just now created by someone.
                }
            }

            String regPath = bookieReadonlyRegistrationPath + "/" + bookieId;
            doRegisterBookie(regPath);
            // clear the write state
            regPath = bookieRegistrationPath + "/" + bookieId;
            try {
                // Clear the current registered node
                zk.delete(regPath, -1);
            } catch (KeeperException.NoNodeException nne) {
                log.warn("No writable bookie registered node {} when transitioning to readonly",
                    regPath, nne);
            }
        } catch (KeeperException | InterruptedException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public void unregisterBookie(String bookieId, boolean readOnly) throws BookieException {
        String regPath;
        if (!readOnly) {
            regPath = bookieRegistrationPath + "/" + bookieId;
        } else {
            regPath = bookieReadonlyRegistrationPath + "/" + bookieId;
        }
        doUnregisterBookie(regPath);
    }

    private void doUnregisterBookie(String regPath) throws BookieException {
        try {
            zk.delete(regPath, -1);
        } catch (InterruptedException | KeeperException e) {
            throw new MetadataStoreException(e);
        }
    }

    //
    // Cookie Management
    //

    @Override
    public void writeCookie(String bookieId,
                            Versioned<byte[]> cookieData) throws BookieException {
        String zkPath = getCookiePath(bookieId);
        try {
            if (Version.NEW == cookieData.getVersion()) {
                if (zk.exists(cookiePath, false) == null) {
                    try {
                        zk.create(cookiePath, new byte[0], zkAcls, CreateMode.PERSISTENT);
                    } catch (NodeExistsException nne) {
                        log.info("More than one bookie tried to create {} at once. Safe to ignore.",
                            cookiePath);
                    }
                }
                zk.create(zkPath, cookieData.getValue(), zkAcls, CreateMode.PERSISTENT);
            } else {
                if (!(cookieData.getVersion() instanceof LongVersion)) {
                    throw new BookieIllegalOpException("Invalid version type, expected it to be LongVersion");
                }
                zk.setData(
                    zkPath,
                    cookieData.getValue(),
                    (int) ((LongVersion) cookieData.getVersion()).getLongVersion());
            }
        } catch (InterruptedException | KeeperException e) {
            throw new MetadataStoreException("Failed to write cookie for bookie " + bookieId);
        }
    }

    @Override
    public Versioned<byte[]> readCookie(String bookieId) throws BookieException {
        String zkPath = getCookiePath(bookieId);
        try {
            Stat stat = zk.exists(zkPath, false);
            byte[] data = zk.getData(zkPath, false, stat);
            // sets stat version from ZooKeeper
            LongVersion version = new LongVersion(stat.getVersion());
            return new Versioned<>(data, version);
        } catch (NoNodeException nne) {
            throw new CookieNotFoundException(bookieId);
        } catch (KeeperException | InterruptedException e) {
            throw new MetadataStoreException("Failed to read cookie for bookie " + bookieId);
        }
    }

    @Override
    public void removeCookie(String bookieId, Version version) throws BookieException {
        String zkPath = getCookiePath(bookieId);
        try {
            zk.delete(zkPath, (int) ((LongVersion) version).getLongVersion());
        } catch (NoNodeException e) {
            throw new CookieNotFoundException(bookieId);
        } catch (InterruptedException | KeeperException e) {
            throw new MetadataStoreException("Failed to delete cookie for bookie " + bookieId);
        }

        log.info("Removed cookie from {} for bookie {}.", cookiePath, bookieId);
    }


    @Override
    public String getClusterInstanceId() throws BookieException {
        String instanceId = null;
        try {
            if (zk.exists(conf.getZkLedgersRootPath(), null) == null) {
                log.error("BookKeeper metadata doesn't exist in zookeeper. "
                    + "Has the cluster been initialized? "
                    + "Try running bin/bookkeeper shell metaformat");
                throw new KeeperException.NoNodeException("BookKeeper metadata");
            }
            try {
                byte[] data = zk.getData(conf.getZkLedgersRootPath() + "/"
                    + INSTANCEID, false, null);
                instanceId = new String(data, UTF_8);
            } catch (KeeperException.NoNodeException e) {
                log.info("INSTANCEID not exists in zookeeper. Not considering it for data verification");
            }
        } catch (KeeperException | InterruptedException e) {
            throw new MetadataStoreException("Failed to get cluster instance id", e);
        }
        return instanceId;
    }

    @VisibleForTesting
    public void setLayoutManager(LayoutManager layoutManager) {
        this.layoutManager = layoutManager;
    }

    @Override
    public LayoutManager getLayoutManager(){
        return layoutManager;
    }

    @Override
    public boolean prepareFormat() throws Exception {
        boolean ledgerRootExists = null != zk.exists(conf.getZkLedgersRootPath(), false);
        boolean availableNodeExists = null != zk.exists(conf.getZkAvailableBookiesPath(), false);
        List<ACL> zkAcls = ZkUtils.getACLs(conf);
        // Create ledgers root node if not exists
        if (!ledgerRootExists) {
            zk.create(conf.getZkLedgersRootPath(), "".getBytes(Charsets.UTF_8), zkAcls, CreateMode.PERSISTENT);
        }
        // create available bookies node if not exists
        if (!availableNodeExists) {
            zk.create(conf.getZkAvailableBookiesPath(), "".getBytes(Charsets.UTF_8), zkAcls, CreateMode.PERSISTENT);
        }

        // create readonly bookies node if not exists
        if (null == zk.exists(conf.getZkAvailableBookiesPath() + "/" + READONLY, false)) {
            zk.create(conf.getZkAvailableBookiesPath() + "/" + READONLY, new byte[0], zkAcls, CreateMode.PERSISTENT);
        }

        return ledgerRootExists;
    }

    @Override
    public boolean initNewCluster() throws Exception {
        String zkLedgersRootPath = conf.getZkLedgersRootPath();
        String zkServers = conf.getZkServers();
        String zkAvailableBookiesPath = conf.getZkAvailableBookiesPath();
        log.info("Initializing ZooKeeper metadata for new cluster, ZKServers: {} ledger root path: {}", zkServers,
                zkLedgersRootPath);

        boolean ledgerRootExists = null != zk.exists(conf.getZkLedgersRootPath(), false);

        if (ledgerRootExists) {
            log.error("Ledger root path: {} already exists", conf.getZkLedgersRootPath());
            return false;
        }

        // Create ledgers root node
        zk.create(zkLedgersRootPath, "".getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // create available bookies node
        zk.create(zkAvailableBookiesPath, "".getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // creates the new layout and stores in zookeeper
        LedgerManagerFactory.newLedgerManagerFactory(conf, layoutManager);

        // create INSTANCEID
        String instanceId = UUID.randomUUID().toString();
        zk.create(conf.getZkLedgersRootPath() + "/" + BookKeeperConstants.INSTANCEID, instanceId.getBytes(UTF_8),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        log.info("Successfully initiated cluster. ZKServers: {} ledger root path: {} instanceId: {}", zkServers,
                zkLedgersRootPath, instanceId);
        return true;
    }

    @Override
    public boolean nukeExistingCluster() throws Exception {
        String zkLedgersRootPath = conf.getZkLedgersRootPath();
        String zkServers = conf.getZkServers();
        log.info("Nuking ZooKeeper metadata of existing cluster, ZKServers: {} ledger root path: {}",
                zkServers, zkLedgersRootPath);

        boolean ledgerRootExists = null != zk.exists(conf.getZkLedgersRootPath(), false);
        if (!ledgerRootExists) {
            log.info("There is no existing cluster with ledgersRootPath: {} in ZKServers: {}, "
                    + "so exiting nuke operation", zkLedgersRootPath, conf.getZkServers());
            return true;
        }

        String availableBookiesPath = conf.getZkAvailableBookiesPath();
        boolean availableNodeExists = null != zk.exists(availableBookiesPath, false);
        try (RegistrationClient regClient = new ZKRegistrationClient()) {
            regClient.initialize(new ClientConfiguration(conf), null, NullStatsLogger.INSTANCE, Optional.empty());
            if (availableNodeExists) {
                Collection<BookieSocketAddress> rwBookies = FutureUtils
                        .result(regClient.getWritableBookies(), EXCEPTION_FUNC).getValue();
                if (rwBookies != null && !rwBookies.isEmpty()) {
                    log.error("Bookies are still up and connected to this cluster, "
                            + "stop all bookies before nuking the cluster");
                    return false;
                }

                String readOnlyBookieRegPath = availableBookiesPath + "/" + BookKeeperConstants.READONLY;
                boolean readonlyNodeExists = null != zk.exists(readOnlyBookieRegPath, false);
                if (readonlyNodeExists) {
                    Collection<BookieSocketAddress> roBookies = FutureUtils
                            .result(regClient.getReadOnlyBookies(), EXCEPTION_FUNC).getValue();
                    if (roBookies != null && !roBookies.isEmpty()) {
                        log.error("Readonly Bookies are still up and connected to this cluster, "
                                + "stop all bookies before nuking the cluster");
                        return false;
                    }
                }
            }
        }

        LedgerManagerFactory ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(conf, layoutManager);
        return ledgerManagerFactory.validateAndNukeExistingCluster(conf, layoutManager);
    }

    @Override
    public boolean format() throws Exception {
        // Clear underreplicated ledgers
        try {
            ZKUtil.deleteRecursive(zk, ZkLedgerUnderreplicationManager.getBasePath(conf.getZkLedgersRootPath())
                    + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH);
        } catch (KeeperException.NoNodeException e) {
            if (log.isDebugEnabled()) {
                log.debug("underreplicated ledgers root path node not exists in zookeeper to delete");
            }
        }

        // Clear underreplicatedledger locks
        try {
            ZKUtil.deleteRecursive(zk, ZkLedgerUnderreplicationManager.getBasePath(conf.getZkLedgersRootPath()) + '/'
                    + BookKeeperConstants.UNDER_REPLICATION_LOCK);
        } catch (KeeperException.NoNodeException e) {
            if (log.isDebugEnabled()) {
                log.debug("underreplicatedledger locks node not exists in zookeeper to delete");
            }
        }

        // Clear the cookies
        try {
            ZKUtil.deleteRecursive(zk, conf.getZkLedgersRootPath() + "/cookies");
        } catch (KeeperException.NoNodeException e) {
            if (log.isDebugEnabled()) {
                log.debug("cookies node not exists in zookeeper to delete");
            }
        }

        // Clear the INSTANCEID
        try {
            zk.delete(conf.getZkLedgersRootPath() + "/" + BookKeeperConstants.INSTANCEID, -1);
        } catch (KeeperException.NoNodeException e) {
            if (log.isDebugEnabled()) {
                log.debug("INSTANCEID not exists in zookeeper to delete");
            }
        }

        // create INSTANCEID
        String instanceId = UUID.randomUUID().toString();
        zk.create(conf.getZkLedgersRootPath() + "/" + BookKeeperConstants.INSTANCEID,
                instanceId.getBytes(Charsets.UTF_8), ZkUtils.getACLs(conf), CreateMode.PERSISTENT);

        log.info("Successfully formatted BookKeeper metadata");
        return true;
    }

    @Override
    public boolean isBookieRegistered(String bookieId) throws BookieException {
        String regPath = bookieRegistrationPath + "/" + bookieId;
        String readonlyRegPath = bookieReadonlyRegistrationPath + "/" + bookieId;
        try {
            return ((null != zk.exists(regPath, false)) || (null != zk.exists(readonlyRegPath, false)));
        } catch (KeeperException e) {
            log.error("ZK exception while checking registration ephemeral znodes for BookieId: {}", bookieId, e);
            throw new MetadataStoreException(e);
        } catch (InterruptedException e) {
            log.error("InterruptedException while checking registration ephemeral znodes for BookieId: {}", bookieId,
                    e);
            throw new MetadataStoreException(e);
        }
    }
}
