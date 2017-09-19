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

import java.io.IOException;
import java.util.List;

import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkRegistrationManager implements RegistrationManager {

    private static Logger LOG = LoggerFactory
            .getLogger(ZkRegistrationManager.class);

    private ZooKeeper zk;

    private static final int CUR_VERSION = 1;

    private AbstractConfiguration conf;

    private ManagerListener mgrListener;

    @Override
    public void init(AbstractConfiguration conf, ZooKeeper zk,
            int managerVersion) throws IOException {
        if (CUR_VERSION != managerVersion) {
            throw new IOException("Incompatible manager version found : "
                    + managerVersion);
        }
        this.conf = conf;
        this.zk = zk;
    }

    @Override
    public int getCurrentVersion() {
        return CUR_VERSION;
    }

    @Override
    public BookieRegistrationManager getBookieRegistrationManager() {
        return new BookieRegistrationManager() {

            @Override
            public void registerBookie(String bookieId, GenericCallback<Void> cb) {

            }

            @Override
            public void markAsReadOnlyBookie(String bookieId,
                    GenericCallback<Void> cb) {

            }

            @Override
            public void getBookieInstanceId(GenericCallback<String> cb) {

            }

            @Override
            public void writeCookieData(byte[] data, GenericCallback<Void> cb) {

            }

            @Override
            public void removeCookieData(GenericCallback<Void> cb) {

            }

            @Override
            public void readCookieData(GenericCallback<byte[]> cb) {

            }

            @Override
            public void createBookieInstanceId(GenericCallback<String> cb) {
                // TODO Auto-generated method stub

            }

        };
    }

    @Override
    public ClientRegistrationManager getClientRegistrationManager() {
        return new ClientRegistrationManager() {

            @Override
            public void getAvailableBookies(GenericCallback<List<String>> cb) {
                // TODO Auto-generated method stub

            }
        };
    }

    @Override
    public void registerListener(ManagerListener listener) {
        if (listener == null) {
            // TODO: handle invalid param
            return;
        }
        this.mgrListener = listener;

        // add watcher for receiving zookeeper server connection events.
        zk.register(new ZooKeeperWatcherBase(zk.getSessionTimeout()) {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType().equals(Watcher.Event.EventType.None)) {
                    if (event.getState().equals(
                            Watcher.Event.KeeperState.Disconnected)) {
                        // TODO:
                        mgrListener.onSuspend();
                    } else if (event.getState().equals(
                            Watcher.Event.KeeperState.SyncConnected)) {
                        // TODO:
                        mgrListener.onResume();
                    }
                }
                if (event.getState().equals(Watcher.Event.KeeperState.Expired)) {
                    // TODO:
                    mgrListener.onShutdown();
                }
            }
        });
    }
}
