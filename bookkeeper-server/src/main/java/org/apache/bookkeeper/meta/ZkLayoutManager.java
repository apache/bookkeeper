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

import static org.apache.bookkeeper.util.BookKeeperConstants.LAYOUT_ZNODE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * Provide utils for writing/reading/deleting layout in zookeeper.
 */
public class ZkLayoutManager implements LayoutManager {

    private final ZooKeeper zk;
    private final String ledgersLayout;
    private final List<ACL> acls;

    public ZkLayoutManager(ZooKeeper zk,
                           String ledgersRoot,
                           List<ACL> acls) {
        this.zk = zk;
        this.ledgersLayout = ledgersRoot + "/" + LAYOUT_ZNODE;
        this.acls = acls;
    }

    @VisibleForTesting
    public ZooKeeper getZk() {
        return zk;
    }

    /**
     * Read ledger layout from zookeeper.
     *
     * @return ledger layout, or null if none set in zookeeper
     */
    @Override
    public LedgerLayout readLedgerLayout() throws IOException {
        try {
            byte[] layoutData = zk.getData(ledgersLayout, false, null);
            return LedgerLayout.parseLayout(layoutData);
        } catch (NoNodeException nne) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (KeeperException e) {
            throw new IOException(e);
        }
    }

    /**
     * Store the ledger layout to zookeeper.
     */
    @Override
    public void storeLedgerLayout(LedgerLayout layout) throws IOException {
        try {
            zk.create(ledgersLayout, layout.serialize(), acls,
                CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            throw new LedgerLayoutExistsException(e);
        } catch (KeeperException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    /**
     * Delete the ledger layout from zookeeper.
     */
    @Override
    public void deleteLedgerLayout() throws IOException {
        try {
            zk.delete(ledgersLayout, -1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (KeeperException e) {
            throw new IOException(e);
        }
    }
}
