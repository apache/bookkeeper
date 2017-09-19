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

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.zookeeper.ZooKeeper;

/**
 * RegistrationManager manages the bookie server and client registration
 * processes
 */
public interface RegistrationManager {

    /**
     * Initialise registration manager with configuration.
     *
     * @param conf
     *            configuration
     * @param zk
     *            zookeeper instance
     * @param manager
     *            version
     */
    void init(AbstractConfiguration conf, ZooKeeper zk, int managerVersion)
            throws IOException;

    /**
     * Listening the server errors
     */
    static interface ManagerListener {
        /**
         * Suspended when encountering some transient errors.
         * {@link #onResume()} would be called if those errors could be fixed.
         * {@link #onShutdown()} would be called if those errors could not be
         * fixed.
         */
        public void onSuspend();

        /**
         * Manager is resumed after fixing the transient errors.
         */
        void onResume();

        /**
         * Manager had to shutdown due to unrecoverable errors.
         */
        public void onShutdown();
    }

    /**
     * Register a listener to listen events of manager
     *
     * @param listener
     *            Server Manager Listener
     */
    public void registerListener(ManagerListener listener);

    /**
     * Return current manager version.
     *
     * @return current version used by manager.
     */
    int getCurrentVersion();

    /**
     * Registration manager interface, which the Bookie server will use to do
     * the registration process
     */
    interface BookieRegistrationManager {
        /**
         * Registering bookie server
         *
         * @param bookieId
         *            unique bookie identifier. This identifier is used by the
         *            bookie client to know the available bookie information
         * @param cb
         *            Callback when registering bookie.
         *            {@link BKException.Code.ZKException} return code when
         *            can't register the bookie
         */
        void registerBookie(String bookieId, GenericCallback<Void> cb);

        /**
         * Marks bookie server as readonly
         *
         * @param bookieId
         *            unique bookie identifier.
         * @param cb
         *            Callback when marking bookie as r-o mode.
         *            {@link BKException.Code.ZKException} return code when
         *            can't mark r-o bookie
         */
        void markAsReadOnlyBookie(String bookieId, GenericCallback<Void> cb);

        /**
         * Writes cookie data, which will be used for verifying the integrity of
         * the bookie environment
         *
         * @param data
         *            data in bytes
         * @param cb
         *            Callback when writing the cookie data.
         *            {@link BKException.Code.ZKException} return code when
         *            can't write the cookie data
         */
        void writeCookieData(byte[] data, GenericCallback<Void> cb);

        /**
         * Reads the cookie data
         *
         * @param cb
         *            Callback when reading cookie data.
         *            {@link BKException.Code.ZKException} return code when
         *            can't read the cookie data
         */
        void readCookieData(GenericCallback<byte[]> cb);

        /**
         * Removing the cookie data
         *
         * @param cb
         *            Callback when removing the cookie data.
         *            {@link BKException.Code.ZKException} return code when
         *            can't delete the cookie data
         */
        void removeCookieData(GenericCallback<Void> cb);

        /**
         * Unique Instance Id of the bookie, which will be created during bookie
         * format
         *
         * @param cb
         *            Callback when getting the bookie instance id.
         *            {@link BKException.Code.ZKException} return code when
         *            can't get the unique bookie instance id
         */
        void getBookieInstanceId(GenericCallback<String> cb);

        /**
         * Creates unique bookie instance id, which will be created during the
         * bookie format. If an instance already exists, it will delete and
         * creates a new one.
         *
         * @param cb
         *            Callback when creating the bookie instance id.
         *            {@link BKException.Code.ZKException} return code when
         *            can't creates the unique bookie instance id
         */
        void createBookieInstanceId(GenericCallback<String> cb);

    }

    /**
     * Registration manager interface, which the Bookie client will use to do
     * the registration process
     */
    interface ClientRegistrationManager {

        /**
         * Get the list of available bookie identifiers
         *
         * @param cb
         *            Callback when getting the registered bookies.
         *            {@link BKException.Code.ZKException} return code when
         *            can't get the list of bookies
         */
        void getAvailableBookies(GenericCallback<List<String>> cb);

    }

    /**
     * Gets the bookie registration manager
     *
     * @return bookie registration manager
     */
    BookieRegistrationManager getBookieRegistrationManager();

    /**
     * Gets the bookie client registration manager
     *
     * @return client registration manager
     */
    ClientRegistrationManager getClientRegistrationManager();
}
