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

import org.apache.bookkeeper.replication.ReplicationEnableCb;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.net.DNS;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.DataFormats.LedgerRereplicationLayoutFormat;
import org.apache.bookkeeper.proto.DataFormats.UnderreplicatedLedgerFormat;
import org.apache.bookkeeper.proto.DataFormats.LockDataFormat;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;

import com.google.protobuf.TextFormat;
import com.google.common.base.Joiner;
import static com.google.common.base.Charsets.UTF_8;

import java.net.UnknownHostException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.util.Arrays;


import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper implementation of underreplication manager.
 * This is implemented in a heirarchical fashion, so it'll work with
 * FlatLedgerManagerFactory and HierarchicalLedgerManagerFactory.
 *
 * Layout is:
 * /root/underreplication/ LAYOUT
 *                         ledgers/(hierarchicalpath)/urL(ledgerId)
 *                         locks/(ledgerId)
 *
 * The hierarchical path is created by splitting the ledger into 4 2byte
 * segments which are represented in hexidecimal.
 * e.g. For ledger id 0xcafebeef0000feed, the path is
 *  cafe/beef/0000/feed/
 */
public class ZkLedgerUnderreplicationManager implements LedgerUnderreplicationManager {
    static final Logger LOG = LoggerFactory.getLogger(ZkLedgerUnderreplicationManager.class);
    static final String LAYOUT="BASIC";
    static final int LAYOUT_VERSION=1;

    private static class Lock {
        private final String lockZNode;
        private final int ledgerZNodeVersion;

        Lock(String lockZNode, int ledgerZNodeVersion) {
            this.lockZNode = lockZNode;
            this.ledgerZNodeVersion = ledgerZNodeVersion;
        }

        String getLockZNode() { return lockZNode; }
        int getLedgerZNodeVersion() { return ledgerZNodeVersion; }
    };
    private final Map<Long, Lock> heldLocks = new ConcurrentHashMap<Long, Lock>();
    private final Pattern idExtractionPattern;

    private final String basePath;
    private final String urLedgerPath;
    private final String urLockPath;
    private final String layoutZNode;
    private final LockDataFormat lockData;

    private final ZooKeeper zkc;

    public ZkLedgerUnderreplicationManager(AbstractConfiguration conf, ZooKeeper zkc)
            throws KeeperException, InterruptedException, ReplicationException.CompatibilityException {
        basePath = conf.getZkLedgersRootPath() + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE;
        layoutZNode = basePath + '/' + BookKeeperConstants.LAYOUT_ZNODE;
        urLedgerPath = basePath
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
        urLockPath = basePath + "/locks";

        idExtractionPattern = Pattern.compile("urL(\\d+)$");
        this.zkc = zkc;

        LockDataFormat.Builder lockDataBuilder = LockDataFormat.newBuilder();
        try {
            lockDataBuilder.setBookieId(DNS.getDefaultHost("default"));
        } catch (UnknownHostException uhe) {
            // if we cant get the address, ignore. it's optional
            // in the data structure in any case
        }
        lockData = lockDataBuilder.build();

        checkLayout();
    }

    private void checkLayout()
            throws KeeperException, InterruptedException, ReplicationException.CompatibilityException {
        if (zkc.exists(basePath, false) == null) {
            try {
                zkc.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
        while (true) {
            if (zkc.exists(layoutZNode, false) == null) {
                LedgerRereplicationLayoutFormat.Builder builder
                    = LedgerRereplicationLayoutFormat.newBuilder();
                builder.setType(LAYOUT).setVersion(LAYOUT_VERSION);
                try {
                    zkc.create(layoutZNode, TextFormat.printToString(builder.build()).getBytes(UTF_8),
                               Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nne) {
                    // someone else managed to create it
                    continue;
                }
            } else {
                byte[] layoutData = zkc.getData(layoutZNode, false, null);

                LedgerRereplicationLayoutFormat.Builder builder
                    = LedgerRereplicationLayoutFormat.newBuilder();

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
                zkc.create(urLedgerPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
        if (zkc.exists(urLockPath, false) == null) {
            try {
                zkc.create(urLockPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
    }

    private long getLedgerId(String path) throws NumberFormatException {
        Matcher m = idExtractionPattern.matcher(path);
        if (m.find()) {
            return Long.valueOf(m.group(1));
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

    private String getUrLedgerZnode(long ledgerId) {
        return getUrLedgerZnode(urLedgerPath, ledgerId);
    }


    @Override
    public void markLedgerUnderreplicated(long ledgerId, String missingReplica)
            throws ReplicationException.UnavailableException {
        LOG.debug("markLedgerUnderreplicated(ledgerId={}, missingReplica={})", ledgerId, missingReplica);
        try {
            String znode = getUrLedgerZnode(ledgerId);
            while (true) {
                UnderreplicatedLedgerFormat.Builder builder = UnderreplicatedLedgerFormat.newBuilder();
                try {
                    builder.addReplica(missingReplica);
                    ZkUtils.createFullPathOptimistic(zkc, znode, TextFormat
                            .printToString(builder.build()).getBytes(UTF_8),
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
        LOG.debug("markLedgerReplicated(ledgerId={})", ledgerId);
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

    private long getLedgerToRereplicateFromHierarchy(String parent, long depth, Watcher w)
            throws KeeperException, InterruptedException {
        if (depth == 4) {
            List<String> children;
            try {
                children = zkc.getChildren(parent, w);
            } catch (KeeperException.NoNodeException nne) {
                // can occur if another underreplicated ledger's
                // hierarchy is being cleaned up
                return -1;
            }

            Collections.shuffle(children);

            while (children.size() > 0) {
                String tryChild = children.get(0);
                try {
                    String lockPath = urLockPath + "/" + tryChild;
                    if (zkc.exists(lockPath, w) != null) {
                        children.remove(tryChild);
                        continue;
                    }

                    Stat stat = zkc.exists(parent + "/" + tryChild, false);
                    if (stat == null) {
                        LOG.debug("{}/{} doesn't exist", parent, tryChild);
                        children.remove(tryChild);
                        continue;
                    }

                    long ledgerId = getLedgerId(tryChild);
                    zkc.create(lockPath, TextFormat.printToString(lockData).getBytes(UTF_8),
                               Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
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
            children = zkc.getChildren(parent, w);
        } catch (KeeperException.NoNodeException nne) {
            // can occur if another underreplicated ledger's
            // hierarchy is being cleaned up
            return -1;
        }

        Collections.shuffle(children);

        while (children.size() > 0) {
            String tryChild = children.get(0);
            String tryPath = parent + "/" + tryChild;
            long ledger = getLedgerToRereplicateFromHierarchy(tryPath, depth + 1, w);
            if (ledger != -1) {
                return ledger;
            }
            children.remove(tryChild);
        }
        return -1;
    }


    @Override
    public long pollLedgerToRereplicate() throws ReplicationException.UnavailableException {
        LOG.debug("pollLedgerToRereplicate()");
        try {
            Watcher w = new Watcher() {
                    public void process(WatchedEvent e) { // do nothing
                    }
                };
            return getLedgerToRereplicateFromHierarchy(urLedgerPath, 0, w);
        } catch (KeeperException ke) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting zookeeper", ie);
        }
    }

    @Override
    public long getLedgerToRereplicate() throws ReplicationException.UnavailableException {
        LOG.debug("getLedgerToRereplicate()");
        try {
            while (true) {
                waitIfLedgerReplicationDisabled();
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
                long ledger = getLedgerToRereplicateFromHierarchy(urLedgerPath, 0, w);
                if (ledger != -1) {
                    return ledger;
                }
                // nothing found, wait for a watcher to trigger
                changedLatch.await();
            }
        } catch (KeeperException ke) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting zookeeper", ie);
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
        LOG.debug("releaseLedger(ledgerId={})", ledgerId);
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
        LOG.debug("close()");
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
        LOG.debug("disableLedegerReplication()");
        try {
            String znode = basePath + '/' + BookKeeperConstants.DISABLE_NODE;
            zkc.create(znode, "".getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
        LOG.debug("enableLedegerReplication()");
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
        LOG.debug("isLedgerReplicationEnabled()");
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
        LOG.debug("notifyLedgerReplicationEnabled()");
        Watcher w = new Watcher() {
            public void process(WatchedEvent e) {
                if (e.getType() == Watcher.Event.EventType.NodeDeleted) {
                    cb.operationComplete(0, null);
                }
            }
        };
        try {
            if (null == zkc.exists(basePath + '/'
                    + BookKeeperConstants.DISABLE_NODE, w)) {
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
}
