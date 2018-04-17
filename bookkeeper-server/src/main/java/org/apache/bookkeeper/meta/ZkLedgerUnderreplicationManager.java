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

import static com.google.common.base.Charsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.protobuf.TextFormat;

import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.DataFormats.LedgerRereplicationLayoutFormat;
import org.apache.bookkeeper.proto.DataFormats.LockDataFormat;
import org.apache.bookkeeper.proto.DataFormats.UnderreplicatedLedgerFormat;
import org.apache.bookkeeper.replication.ReplicationEnableCb;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.SubTreeCache;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper implementation of underreplication manager.
 * This is implemented in a heirarchical fashion, so it'll work with
 * FlatLedgerManagerFactory and HierarchicalLedgerManagerFactory.
 *
 * <p>Layout is:
 * /root/underreplication/ LAYOUT
 *                         ledgers/(hierarchicalpath)/urL(ledgerId)
 *                         locks/(ledgerId)
 *
 * <p>The hierarchical path is created by splitting the ledger into 4 2byte
 * segments which are represented in hexidecimal.
 * e.g. For ledger id 0xcafebeef0000feed, the path is
 *  cafe/beef/0000/feed/
 */
public class ZkLedgerUnderreplicationManager implements LedgerUnderreplicationManager {
    static final Logger LOG = LoggerFactory.getLogger(ZkLedgerUnderreplicationManager.class);
    static final String LAYOUT = "BASIC";
    static final int LAYOUT_VERSION = 1;

    private static final byte[] LOCK_DATA = getLockData();

    private static class Lock {
        private final String lockZNode;
        private final int ledgerZNodeVersion;

        Lock(String lockZNode, int ledgerZNodeVersion) {
            this.lockZNode = lockZNode;
            this.ledgerZNodeVersion = ledgerZNodeVersion;
        }

        String getLockZNode() {
            return lockZNode;
        }

        int getLedgerZNodeVersion() {
            return ledgerZNodeVersion;
        }
    }
    private final Map<Long, Lock> heldLocks = new ConcurrentHashMap<Long, Lock>();
    private final Pattern idExtractionPattern;

    private final String basePath;
    private final String urLedgerPath;
    private final String urLockPath;
    private final String layoutZNode;
    private final AbstractConfiguration conf;
    private final String lostBookieRecoveryDelayZnode;
    private final ZooKeeper zkc;
    private final SubTreeCache subTreeCache;

    public ZkLedgerUnderreplicationManager(AbstractConfiguration conf, ZooKeeper zkc)
            throws KeeperException, InterruptedException, ReplicationException.CompatibilityException {
        this.conf = conf;
        basePath = getBasePath(ZKMetadataDriverBase.resolveZkLedgersRootPath(conf));
        layoutZNode = basePath + '/' + BookKeeperConstants.LAYOUT_ZNODE;
        urLedgerPath = basePath
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
        urLockPath = basePath + '/' + BookKeeperConstants.UNDER_REPLICATION_LOCK;
        lostBookieRecoveryDelayZnode = basePath + '/' + BookKeeperConstants.LOSTBOOKIERECOVERYDELAY_NODE;

        idExtractionPattern = Pattern.compile("urL(\\d+)$");
        this.zkc = zkc;
        this.subTreeCache = new SubTreeCache(new SubTreeCache.TreeProvider() {
            @Override
            public List<String> getChildren(String path, Watcher watcher) throws InterruptedException, KeeperException {
                return zkc.getChildren(path, watcher);
            }
        });

        checkLayout();
    }

    public static String getBasePath(String rootPath) {
        return String.format("%s/%s", rootPath, BookKeeperConstants.UNDER_REPLICATION_NODE);
    }

    public static String getUrLockPath(String rootPath) {
        return String.format("%s/%s", getBasePath(rootPath), BookKeeperConstants.UNDER_REPLICATION_LOCK);
    }

    public static byte[] getLockData() {
        LockDataFormat.Builder lockDataBuilder = LockDataFormat.newBuilder();
        try {
            lockDataBuilder.setBookieId(DNS.getDefaultHost("default"));
        } catch (UnknownHostException uhe) {
            // if we cant get the address, ignore. it's optional
            // in the data structure in any case
        }
        return TextFormat.printToString(lockDataBuilder.build()).getBytes(UTF_8);
    }

    private void checkLayout()
            throws KeeperException, InterruptedException, ReplicationException.CompatibilityException {
        List<ACL> zkAcls = ZkUtils.getACLs(conf);
        if (zkc.exists(basePath, false) == null) {
            try {
                zkc.create(basePath, new byte[0], zkAcls, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
        while (true) {
            if (zkc.exists(layoutZNode, false) == null) {
                LedgerRereplicationLayoutFormat.Builder builder = LedgerRereplicationLayoutFormat.newBuilder();
                builder.setType(LAYOUT).setVersion(LAYOUT_VERSION);
                try {
                    zkc.create(layoutZNode, TextFormat.printToString(builder.build()).getBytes(UTF_8),
                               zkAcls, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nne) {
                    // someone else managed to create it
                    continue;
                }
            } else {
                byte[] layoutData = zkc.getData(layoutZNode, false, null);

                LedgerRereplicationLayoutFormat.Builder builder = LedgerRereplicationLayoutFormat.newBuilder();

                try {
                    TextFormat.merge(new String(layoutData, UTF_8), builder);
                    LedgerRereplicationLayoutFormat layout = builder.build();
                    if (!layout.getType().equals(LAYOUT)
                            || layout.getVersion() != LAYOUT_VERSION) {
                        throw new ReplicationException.CompatibilityException(
                                "Incompatible layout found (" + LAYOUT + ":" + LAYOUT_VERSION + ")");
                    }
                } catch (TextFormat.ParseException pe) {
                    throw new ReplicationException.CompatibilityException(
                            "Invalid data found", pe);
                }
                break;
            }
        }
        if (zkc.exists(urLedgerPath, false) == null) {
            try {
                zkc.create(urLedgerPath, new byte[0], zkAcls, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
        if (zkc.exists(urLockPath, false) == null) {
            try {
                zkc.create(urLockPath, new byte[0], zkAcls, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
    }

    private long getLedgerId(String path) throws NumberFormatException {
        Matcher m = idExtractionPattern.matcher(path);
        if (m.find()) {
            return Long.parseLong(m.group(1));
        } else {
            throw new NumberFormatException("Couldn't find ledgerid in path");
        }
    }

    public static String getParentZnodePath(String base, long ledgerId) {
        String subdir1 = String.format("%04x", ledgerId >> 48 & 0xffff);
        String subdir2 = String.format("%04x", ledgerId >> 32 & 0xffff);
        String subdir3 = String.format("%04x", ledgerId >> 16 & 0xffff);
        String subdir4 = String.format("%04x", ledgerId & 0xffff);

        return String.format("%s/%s/%s/%s/%s",
                             base, subdir1, subdir2, subdir3, subdir4);
    }

    public static String getUrLedgerZnode(String base, long ledgerId) {
        return String.format("%s/urL%010d", getParentZnodePath(base, ledgerId), ledgerId);
    }

    public static String getUrLedgerLockZnode(String base, long ledgerId) {
        return String.format("%s/urL%010d", base, ledgerId);
    }

    private String getUrLedgerZnode(long ledgerId) {
        return getUrLedgerZnode(urLedgerPath, ledgerId);
    }

    @VisibleForTesting
    public UnderreplicatedLedgerFormat getLedgerUnreplicationInfo(long ledgerId)
            throws KeeperException, TextFormat.ParseException, InterruptedException {
        String znode = getUrLedgerZnode(ledgerId);
        UnderreplicatedLedgerFormat.Builder builder = UnderreplicatedLedgerFormat.newBuilder();
        byte[] data = zkc.getData(znode, false, null);
        TextFormat.merge(new String(data, UTF_8), builder);
        return builder.build();
    }

    @Override
    public void markLedgerUnderreplicated(long ledgerId, String missingReplica)
            throws ReplicationException.UnavailableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("markLedgerUnderreplicated(ledgerId={}, missingReplica={})", ledgerId, missingReplica);
        }
        try {
            List<ACL> zkAcls = ZkUtils.getACLs(conf);
            String znode = getUrLedgerZnode(ledgerId);
            while (true) {
                UnderreplicatedLedgerFormat.Builder builder = UnderreplicatedLedgerFormat.newBuilder();
                try {
                    builder.addReplica(missingReplica);
                    ZkUtils.createFullPathOptimistic(zkc, znode, TextFormat
                            .printToString(builder.build()).getBytes(UTF_8),
                            zkAcls, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nee) {
                    Stat s = zkc.exists(znode, false);
                    if (s == null) {
                        continue;
                    }
                    try {
                        byte[] bytes = zkc.getData(znode, false, s);
                        builder.clear();
                        TextFormat.merge(new String(bytes, UTF_8), builder);
                        UnderreplicatedLedgerFormat data = builder.build();
                        if (data.getReplicaList().contains(missingReplica)) {
                            return; // nothing to add
                        }
                        builder.addReplica(missingReplica);
                        zkc.setData(znode,
                                    TextFormat.printToString(builder.build()).getBytes(UTF_8),
                                    s.getVersion());
                    } catch (KeeperException.NoNodeException nne) {
                        continue;
                    } catch (KeeperException.BadVersionException bve) {
                        continue;
                    } catch (TextFormat.ParseException pe) {
                        throw new ReplicationException.UnavailableException(
                                "Invalid data found", pe);
                    }
                }
                break;
            }
        } catch (KeeperException ke) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public void markLedgerReplicated(long ledgerId) throws ReplicationException.UnavailableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("markLedgerReplicated(ledgerId={})", ledgerId);
        }
        try {
            Lock l = heldLocks.get(ledgerId);
            if (l != null) {
                zkc.delete(getUrLedgerZnode(ledgerId), l.getLedgerZNodeVersion());

                try {
                    // clean up the hierarchy
                    String parts[] = getUrLedgerZnode(ledgerId).split("/");
                    for (int i = 1; i <= 4; i++) {
                        String p[] = Arrays.copyOf(parts, parts.length - i);
                        String path = Joiner.on("/").join(p);
                        Stat s = zkc.exists(path, null);
                        if (s != null) {
                            zkc.delete(path, s.getVersion());
                        }
                    }
                } catch (KeeperException.NotEmptyException nee) {
                    // This can happen when cleaning up the hierarchy.
                    // It's safe to ignore, it simply means another
                    // ledger in the same hierarchy has been marked as
                    // underreplicated.
                }
            }
        } catch (KeeperException.NoNodeException nne) {
            // this is ok
        } catch (KeeperException.BadVersionException bve) {
            // if this is the case, some has marked the ledger
            // for rereplication again. Leave the underreplicated
            // znode in place, so the ledger is checked.
        } catch (KeeperException ke) {
            LOG.error("Error deleting underreplicated ledger znode", ke);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        } finally {
            releaseUnderreplicatedLedger(ledgerId);
        }
    }

    /**
     * Get a list of all the ledgers which have been
     * marked for rereplication, filtered by the predicate on the replicas list.
     *
     * <p>Replicas list of an underreplicated ledger is the list of the bookies which are part of
     * the ensemble of this ledger and are currently unavailable/down.
     *
     * <p>If filtering is not needed then it is suggested to pass null for predicate,
     * otherwise it will read the content of the ZNode to decide on filtering.
     *
     * @param predicate filter to use while listing under replicated ledgers. 'null' if filtering is not required.
     * @param includeReplicaList whether to include missing replicalist in the output.
     * @return an iterator which returns ledger ids
     */
    @Override
    public Iterator<Map.Entry<Long, List<String>>> listLedgersToRereplicate(final Predicate<List<String>> predicate,
            boolean includeReplicaList) {
        final Queue<String> queue = new LinkedList<String>();
        queue.add(urLedgerPath);

        return new Iterator<Map.Entry<Long, List<String>>>() {
            final Queue<Map.Entry<Long, List<String>>> curBatch = new LinkedList<Map.Entry<Long, List<String>>>();

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                if (curBatch.size() > 0) {
                    return true;
                }

                while (queue.size() > 0 && curBatch.size() == 0) {
                    String parent = queue.remove();
                    try {
                        for (String c : zkc.getChildren(parent, false)) {
                            try {
                                String child = parent + "/" + c;
                                if (c.startsWith("urL")) {
                                    long ledgerId = getLedgerId(child);
                                    List<String> replicaList = getLedgerUnreplicationInfo(ledgerId).getReplicaList();
                                    if ((predicate == null)
                                            || predicate.test(replicaList)) {
                                        curBatch.add(new AbstractMap.SimpleImmutableEntry<Long, List<String>>(ledgerId,
                                                ((includeReplicaList) ? replicaList : null)));
                                    }
                                } else {
                                    queue.add(child);
                                }
                            } catch (KeeperException.NoNodeException nne) {
                                // ignore
                            }
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    } catch (KeeperException.NoNodeException nne) {
                        // ignore
                    } catch (Exception e) {
                        throw new RuntimeException("Error reading list", e);
                    }
                }
                return curBatch.size() > 0;
            }

            @Override
            public Map.Entry<Long, List<String>> next() {
                assert curBatch.size() > 0;
                return curBatch.remove();
            }
        };
    }

    private long getLedgerToRereplicateFromHierarchy(String parent, long depth)
            throws KeeperException, InterruptedException {
        if (depth == 4) {
            List<String> children;
            try {
                children = subTreeCache.getChildren(parent);
            } catch (KeeperException.NoNodeException nne) {
                // can occur if another underreplicated ledger's
                // hierarchy is being cleaned up
                return -1;
            }

            Collections.shuffle(children);
            List<ACL> zkAcls = ZkUtils.getACLs(conf);
            while (children.size() > 0) {
                String tryChild = children.get(0);
                try {
                    List<String> locks = subTreeCache.getChildren(urLockPath);
                    if (locks.contains(tryChild)) {
                        children.remove(tryChild);
                        continue;
                    }

                    Stat stat = zkc.exists(parent + "/" + tryChild, false);
                    if (stat == null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("{}/{} doesn't exist", parent, tryChild);
                        }
                        children.remove(tryChild);
                        continue;
                    }

                    String lockPath = urLockPath + "/" + tryChild;
                    long ledgerId = getLedgerId(tryChild);
                    zkc.create(lockPath, LOCK_DATA, zkAcls, CreateMode.EPHEMERAL);
                    heldLocks.put(ledgerId, new Lock(lockPath, stat.getVersion()));
                    return ledgerId;
                } catch (KeeperException.NodeExistsException nee) {
                    children.remove(tryChild);
                } catch (NumberFormatException nfe) {
                    children.remove(tryChild);
                }
            }
            return -1;
        }

        List<String> children;
        try {
            children = subTreeCache.getChildren(parent);
        } catch (KeeperException.NoNodeException nne) {
            // can occur if another underreplicated ledger's
            // hierarchy is being cleaned up
            return -1;
        }

        Collections.shuffle(children);

        while (children.size() > 0) {
            String tryChild = children.get(0);
            String tryPath = parent + "/" + tryChild;
            long ledger = getLedgerToRereplicateFromHierarchy(tryPath, depth + 1);
            if (ledger != -1) {
                return ledger;
            }
            children.remove(tryChild);
        }
        return -1;
    }


    @Override
    public long pollLedgerToRereplicate() throws ReplicationException.UnavailableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("pollLedgerToRereplicate()");
        }
        try {
            return getLedgerToRereplicateFromHierarchy(urLedgerPath, 0);
        } catch (KeeperException ke) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting zookeeper", ie);
        }
    }

    @Override
    public long getLedgerToRereplicate() throws ReplicationException.UnavailableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getLedgerToRereplicate()");
        }
        while (true) {
            final CountDownLatch changedLatch = new CountDownLatch(1);
            Watcher w = new Watcher() {
                public void process(WatchedEvent e) {
                    if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged
                            || e.getType() == Watcher.Event.EventType.NodeDeleted
                            || e.getType() == Watcher.Event.EventType.NodeCreated
                            || e.getState() == Watcher.Event.KeeperState.Expired
                            || e.getState() == Watcher.Event.KeeperState.Disconnected) {
                        changedLatch.countDown();
                    }
                }
            };
            try (SubTreeCache.WatchGuard wg = subTreeCache.registerWatcherWithGuard(w)) {
                waitIfLedgerReplicationDisabled();
                long ledger = getLedgerToRereplicateFromHierarchy(urLedgerPath, 0);
                if (ledger != -1) {
                    return ledger;
                }
                // nothing found, wait for a watcher to trigger
                changedLatch.await();
            } catch (KeeperException ke) {
                throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new ReplicationException.UnavailableException("Interrupted while connecting zookeeper", ie);
            }
        }
    }

    private void waitIfLedgerReplicationDisabled() throws UnavailableException,
            InterruptedException {
        ReplicationEnableCb cb = new ReplicationEnableCb();
        if (!this.isLedgerReplicationEnabled()) {
            this.notifyLedgerReplicationEnabled(cb);
            cb.await();
        }
    }

    @Override
    public void releaseUnderreplicatedLedger(long ledgerId) throws ReplicationException.UnavailableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("releaseLedger(ledgerId={})", ledgerId);
        }
        try {
            Lock l = heldLocks.remove(ledgerId);
            if (l != null) {
                zkc.delete(l.getLockZNode(), -1);
            }
        } catch (KeeperException.NoNodeException nne) {
            // this is ok
        } catch (KeeperException ke) {
            LOG.error("Error deleting underreplicated ledger lock", ke);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting zookeeper", ie);
        }
    }

    @Override
    public void close() throws ReplicationException.UnavailableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("close()");
        }
        try {
            for (Map.Entry<Long, Lock> e : heldLocks.entrySet()) {
                zkc.delete(e.getValue().getLockZNode(), -1);
            }
        } catch (KeeperException.NoNodeException nne) {
            // this is ok
        } catch (KeeperException ke) {
            LOG.error("Error deleting underreplicated ledger lock", ke);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting zookeeper", ie);
        }
    }

    @Override
    public void disableLedgerReplication()
            throws ReplicationException.UnavailableException {
        List<ACL> zkAcls = ZkUtils.getACLs(conf);
        if (LOG.isDebugEnabled()) {
            LOG.debug("disableLedegerReplication()");
        }
        try {
            String znode = basePath + '/' + BookKeeperConstants.DISABLE_NODE;
            zkc.create(znode, "".getBytes(UTF_8), zkAcls, CreateMode.PERSISTENT);
            LOG.info("Auto ledger re-replication is disabled!");
        } catch (KeeperException.NodeExistsException ke) {
            LOG.warn("AutoRecovery is already disabled!", ke);
            throw new ReplicationException.UnavailableException(
                    "AutoRecovery is already disabled!", ke);
        } catch (KeeperException ke) {
            LOG.error("Exception while stopping auto ledger re-replication", ke);
            throw new ReplicationException.UnavailableException(
                    "Exception while stopping auto ledger re-replication", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while stopping auto ledger re-replication", ie);
        }
    }

    @Override
    public void enableLedgerReplication()
            throws ReplicationException.UnavailableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("enableLedegerReplication()");
        }
        try {
            zkc.delete(basePath + '/' + BookKeeperConstants.DISABLE_NODE, -1);
            LOG.info("Resuming automatic ledger re-replication");
        } catch (KeeperException.NoNodeException ke) {
            LOG.warn("AutoRecovery is already enabled!", ke);
            throw new ReplicationException.UnavailableException(
                    "AutoRecovery is already enabled!", ke);
        } catch (KeeperException ke) {
            LOG.error("Exception while resuming ledger replication", ke);
            throw new ReplicationException.UnavailableException(
                    "Exception while resuming auto ledger re-replication", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while resuming auto ledger re-replication", ie);
        }
    }

    @Override
    public boolean isLedgerReplicationEnabled()
            throws ReplicationException.UnavailableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("isLedgerReplicationEnabled()");
        }
        try {
            if (null != zkc.exists(basePath + '/'
                    + BookKeeperConstants.DISABLE_NODE, false)) {
                return false;
            }
            return true;
        } catch (KeeperException ke) {
            LOG.error("Error while checking the state of "
                    + "ledger re-replication", ke);
            throw new ReplicationException.UnavailableException(
                    "Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public void notifyLedgerReplicationEnabled(final GenericCallback<Void> cb)
            throws ReplicationException.UnavailableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("notifyLedgerReplicationEnabled()");
        }
        Watcher w = new Watcher() {
            public void process(WatchedEvent e) {
                if (e.getType() == Watcher.Event.EventType.NodeDeleted) {
                    LOG.info("LedgerReplication is enabled externally through Zookeeper, "
                            + "since DISABLE_NODE ZNode is deleted");
                    cb.operationComplete(0, null);
                }
            }
        };
        try {
            if (null == zkc.exists(basePath + '/'
                    + BookKeeperConstants.DISABLE_NODE, w)) {
                LOG.info("LedgerReplication is enabled externally through Zookeeper, "
                        + "since DISABLE_NODE ZNode is deleted");
                cb.operationComplete(0, null);
                return;
            }
        } catch (KeeperException ke) {
            LOG.error("Error while checking the state of "
                    + "ledger re-replication", ke);
            throw new ReplicationException.UnavailableException(
                    "Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while contacting zookeeper", ie);
        }
    }

    /**
     * Check whether the ledger is being replicated by any bookie.
     */
    public static boolean isLedgerBeingReplicated(ZooKeeper zkc, String zkLedgersRootPath, long ledgerId)
            throws KeeperException,
            InterruptedException {
        return zkc.exists(getUrLedgerLockZnode(getUrLockPath(zkLedgersRootPath), ledgerId), false) != null;
    }

    /**
     * Acquire the underreplicated ledger lock.
     */
    public static void acquireUnderreplicatedLedgerLock(ZooKeeper zkc, String zkLedgersRootPath,
        long ledgerId, List<ACL> zkAcls)
            throws KeeperException, InterruptedException {
        ZkUtils.createFullPathOptimistic(zkc, getUrLedgerLockZnode(getUrLockPath(zkLedgersRootPath), ledgerId),
                LOCK_DATA, zkAcls, CreateMode.EPHEMERAL);
    }

    /**
     * Release the underreplicated ledger lock if it exists.
     */
    public static void releaseUnderreplicatedLedgerLock(ZooKeeper zkc, String zkLedgersRootPath, long ledgerId)
            throws InterruptedException, KeeperException {
        if (isLedgerBeingReplicated(zkc, zkLedgersRootPath, ledgerId)) {
            zkc.delete(getUrLedgerLockZnode(getUrLockPath(zkLedgersRootPath), ledgerId), -1);
        }
    }

    @Override
    public boolean initializeLostBookieRecoveryDelay(int lostBookieRecoveryDelay) throws UnavailableException {
        LOG.debug("initializeLostBookieRecoveryDelay()");
        try {
            zkc.create(lostBookieRecoveryDelayZnode, Integer.toString(lostBookieRecoveryDelay).getBytes(UTF_8),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ke) {
            LOG.info("lostBookieRecoveryDelay Znode is already present, so using "
                    + "existing lostBookieRecoveryDelay Znode value");
            return false;
        } catch (KeeperException ke) {
            LOG.error("Error while initializing LostBookieRecoveryDelay", ke);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
        return true;
    }

    @Override
    public void setLostBookieRecoveryDelay(int lostBookieRecoveryDelay) throws UnavailableException {
        LOG.debug("setLostBookieRecoveryDelay()");
        try {
            if (zkc.exists(lostBookieRecoveryDelayZnode, false) != null) {
                zkc.setData(lostBookieRecoveryDelayZnode, Integer.toString(lostBookieRecoveryDelay).getBytes(UTF_8),
                        -1);
            } else {
                zkc.create(lostBookieRecoveryDelayZnode, Integer.toString(lostBookieRecoveryDelay).getBytes(UTF_8),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException ke) {
            LOG.error("Error while setting LostBookieRecoveryDelay ", ke);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public int getLostBookieRecoveryDelay() throws UnavailableException {
        LOG.debug("getLostBookieRecoveryDelay()");
        try {
            byte[] data = zkc.getData(lostBookieRecoveryDelayZnode, false, null);
            return Integer.parseInt(new String(data, UTF_8));
        } catch (KeeperException ke) {
            LOG.error("Error while getting LostBookieRecoveryDelay ", ke);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public void notifyLostBookieRecoveryDelayChanged(GenericCallback<Void> cb) throws UnavailableException {
        LOG.debug("notifyLostBookieRecoveryDelayChanged()");
        Watcher w = new Watcher() {
            public void process(WatchedEvent e) {
                if (e.getType() == Watcher.Event.EventType.NodeDataChanged) {
                    cb.operationComplete(0, null);
                }
            }
        };
        try {
            if (null == zkc.exists(lostBookieRecoveryDelayZnode, w)) {
                cb.operationComplete(0, null);
                return;
            }
        } catch (KeeperException ke) {
            LOG.error("Error while checking the state of lostBookieRecoveryDelay", ke);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }
}
