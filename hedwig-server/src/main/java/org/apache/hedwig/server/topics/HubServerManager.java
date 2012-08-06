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
package org.apache.hedwig.server.topics;

import java.io.IOException;

import org.apache.hedwig.util.Callback;

/**
 * The HubServerManager class manages info about hub servers.
 */
interface HubServerManager {

    static interface ManagerListener {

        /**
         * Server manager is suspended if encountering some transient errors.
         * {@link #onResume()} would be called if those errors could be fixed.
         * {@link #onShutdown()} would be called if those errors could not be fixed.
         */
        public void onSuspend();

        /**
         * Server manager is resumed after fixing some transient errors.
         */
        public void onResume();

        /**
         * Server manager had to shutdown due to unrecoverable errors.
         */
        public void onShutdown();
    }

    /**
     * Register a listener to listen events of server manager
     *
     * @param listener
     *          Server Manager Listener
     */
    public void registerListener(ManagerListener listener);

    /**
     * Register itself to the cluster.
     *
     * @param selfLoad
     *          Self load data
     * @param callback
     *          Callback when itself registered.
     * @param ctx
     *          Callback context.
     */
    public void registerSelf(HubLoad selfLoad, Callback<HubInfo> callback, Object ctx);

    /**
     * Unregister itself from the cluster.
     */
    public void unregisterSelf() throws IOException;

    /**
     * Uploading self server load data.
     *
     * It is an asynchrounous call which should not block other operations.
     * Currently we don't need to care about whether it succeed or not.
     *
     * @param selfLoad
     *          Hub server load data.
     */
    public void uploadSelfLoadData(HubLoad selfLoad);

    /**
     * Check whether a hub server is alive as the id
     *
     * @param hub
     *          Hub id to identify a lifecycle of a hub server
     * @param callback
     *          Callback of check result. If the hub server is still
     *          alive as the provided id <code>hub</code>, return true.
     *          Otherwise return false.
     * @param ctx
     *          Callback context
     */
    public void isHubAlive(HubInfo hub, Callback<Boolean> callback, Object ctx);

    /**
     * Choose a least loaded hub server from available hub servers.
     *
     * @param callback
     *          Callback to return least loaded hub server.
     * @param ctx
     *          Callback context.
     */
    public void chooseLeastLoadedHub(Callback<HubInfo> callback, Object ctx);
}
