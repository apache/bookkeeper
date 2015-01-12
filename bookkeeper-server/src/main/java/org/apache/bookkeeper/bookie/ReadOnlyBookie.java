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

import java.io.IOException;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implements a read only bookie.
 * 
 * ReadOnlyBookie is force started as readonly, and will not change to writable.
 *
 */
public class ReadOnlyBookie extends Bookie {

    private final static Logger LOG = LoggerFactory.getLogger(ReadOnlyBookie.class);

    public ReadOnlyBookie(ServerConfiguration conf, StatsLogger statsLogger)
            throws IOException, KeeperException, InterruptedException, BookieException {
        super(conf, statsLogger);
        if (conf.isReadOnlyModeEnabled()) {
            readOnly.set(true);
        } else {
            String err = "Try to init ReadOnly Bookie, while ReadOnly mode is not enabled";
            LOG.error(err);
            throw new IOException(err);
        }
        LOG.info("successed call ReadOnlyBookie constructor");
    }

    /**
     * Register as a read only bookie
     */
    @Override
    protected void registerBookie(ServerConfiguration conf) throws IOException {
        if (null == zk) {
            // zookeeper instance is null, means not register itself to zk
            return;
        }

        // ZK node for this ReadOnly Bookie.
        try{
            if (null == zk.exists(this.bookieRegistrationPath
                        + BookKeeperConstants.READONLY, false)) {
                try {
                    zk.create(this.bookieRegistrationPath
                            + BookKeeperConstants.READONLY + "/", new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOG.debug("successed create ReadOnlyBookie parent zk node");
                } catch (NodeExistsException e) {
                    // this node is just now created by someone.
                }
            }

            if (!checkRegNodeAndWaitExpired(zkBookieReadOnlyPath)) {
                // Create the ZK node for this RO Bookie.
                zk.create(zkBookieReadOnlyPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                LOG.debug("successed create ReadOnlyBookie zk node");
            }
        } catch (KeeperException ke) {
            LOG.error("ZK exception registering Znode for ReadOnly Bookie!", ke);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new IOException(ke);
        } catch (InterruptedException ie) {
            LOG.error("Interruptted exception registering Znode for ReadOnly Bookie!",
                    ie);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new IOException(ie);
        }
    }

    @VisibleForTesting
    @Override
    public void transitionToWritableMode() {
        LOG.info("Skip transition to writable mode for readonly bookie");
    }


    @VisibleForTesting
    @Override
    public void transitionToReadOnlyMode() {
        LOG.warn("Skip transition to readonly mode for readonly bookie");
    }

}
