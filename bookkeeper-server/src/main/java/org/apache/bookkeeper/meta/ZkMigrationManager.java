/*
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
 */
package org.apache.bookkeeper.meta;

import static org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager.getParentZnodePath;

import com.google.common.collect.Lists;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of MigrationManager to manage ledger replicas to be migrated.
 * */
public class ZkMigrationManager implements MigrationManager {
    private static final Logger LOG = LoggerFactory.getLogger(ZkMigrationManager.class);
    private final String rootPath;
    private final String basePath;
    private final String migrationReplicasPath;
    private final String lock = "locked";
    private final ZooKeeper zkc;
    private final List<ACL> zkAcls;
    public ZkMigrationManager(AbstractConfiguration conf, ZooKeeper zkc) {
        rootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf);
        basePath = getBasePath(rootPath);
        migrationReplicasPath = basePath
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
        this.zkc = zkc;
        zkAcls = ZkUtils.getACLs(conf);
    }

    private static String getBasePath(String rootPath) {
        return String.format("%s/%s", rootPath, BookKeeperConstants.MIGRATION_REPLICAS);
    }

    private String getLedgerForMigrationReplicasPath(long ledgerId) {
        return String.format("%s/urL%010d", getParentZnodePath(migrationReplicasPath, ledgerId), ledgerId);
    }

    private String getLockForMigrationReplicasPath(long ledgerId) {
        return String.format("%s/%s", getLedgerForMigrationReplicasPath(ledgerId), lock);
    }

    @Override
    public void submitToMigrateReplicas(Map<Long, Set<BookieId>> toMigratedLedgerAndBookieMap)
            throws InterruptedException, KeeperException {
        List<Op> multiOps = Lists.newArrayListWithExpectedSize(toMigratedLedgerAndBookieMap.size());
        for (Map.Entry<Long, Set<BookieId>> entry : toMigratedLedgerAndBookieMap.entrySet()) {
            Long ledgerId = entry.getKey();
            String bookies = StringUtils.join(entry.getValue(), ",");
            multiOps.add(Op.create(getLedgerForMigrationReplicasPath(ledgerId),
                    bookies.getBytes(StandardCharsets.UTF_8), zkAcls, CreateMode.PERSISTENT));
        }
        zkc.multi(multiOps);
    }

    @Override
    public List<String> listLedgersOfMigrationReplicas()
            throws InterruptedException, KeeperException {
        try {
            return zkc.getChildren(migrationReplicasPath, false);
        } catch (KeeperException.NoNodeException e) {
            zkc.create(migrationReplicasPath, BookKeeperConstants.EMPTY_BYTE_ARRAY, zkAcls, CreateMode.PERSISTENT);
            return new ArrayList<>();
        }
    }

    @Override
    public void lockMigrationReplicas(long ledgerId, String advertisedAddress)
            throws InterruptedException, KeeperException, UnsupportedOperationException{
        zkc.create(getLockForMigrationReplicasPath(ledgerId),
                advertisedAddress.getBytes(StandardCharsets.UTF_8), zkAcls, CreateMode.EPHEMERAL);
    }

    @Override
    public String getOwnerBookiesMigrationReplicas(long ledgerId)
            throws InterruptedException, KeeperException, UnsupportedEncodingException {
        byte[] data = zkc.getData(getLedgerForMigrationReplicasPath(ledgerId), null, null);
        return data == null ? "" : new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public void deleteZkPath(String path)
            throws InterruptedException, KeeperException, UnsupportedOperationException {
        zkc.delete(path, -1);
    }

    @Override
    public void deleteMigrationLedgerPath(long ledgerId)
            throws InterruptedException, KeeperException, UnsupportedOperationException{
        deleteZkPath(getLockForMigrationReplicasPath(ledgerId));
    }

    @Override
    public void releaseLock(long ledgerId) {
        try {
            deleteZkPath(getLockForMigrationReplicasPath(ledgerId));
            LOG.info(String.format("Release lock for ledgerId %s success!", ledgerId));
        } catch (KeeperException.NoNodeException e) {
            // do nothing,already release lock
        } catch (Exception e) {
            LOG.error(String.format("Release lock for ledgerId %s failed!", ledgerId));
        }
    }

    @Override
    public boolean exists(long ledgerId) throws InterruptedException, KeeperException {
        return zkc.exists(getLedgerForMigrationReplicasPath(ledgerId), null) != null;
    }

    @Override
    public void close(){
        try {
            zkc.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
