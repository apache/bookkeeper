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

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.LimitedPrivate;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Registration manager interface, which a bookie server will use to do the registration process.
 */
@LimitedPrivate
@Evolving
public interface RegistrationManager extends AutoCloseable {

    /**
     * Registration Listener on listening the registration state.
     */
    @FunctionalInterface
    interface RegistrationListener {

        /**
         * Signal when registration is expired.
         */
        void onRegistrationExpired();

    }

    @Override
    void close();

    /**
     * Return the cluster instance id.
     *
     * @return the cluster instance id.
     */
    String getClusterInstanceId() throws BookieException;

    /**
     * Registering the bookie server as <i>bookieId</i>.
     *
     * @param bookieId bookie id
     * @param readOnly whether to register it as writable or readonly
     * @param serviceInfo information about services exposed by the Bookie
     * @throws BookieException when fail to register a bookie.
     */
    void registerBookie(String bookieId, boolean readOnly, BookieServiceInfo serviceInfo) throws BookieException;

    /**
     * Unregistering the bookie server as <i>bookieId</i>.
     *
     * @param bookieId bookie id
     * @param readOnly whether to register it as writable or readonly
     * @throws BookieException when fail to unregister a bookie.
     */
    void unregisterBookie(String bookieId, boolean readOnly) throws BookieException;

    /**
     * Checks if Bookie with the given BookieId is registered as readwrite or
     * readonly bookie.
     *
     * @param bookieId bookie id
     * @return returns true if a bookie with bookieid is currently registered as
     *          readwrite or readonly bookie.
     * @throws BookieException
     */
    boolean isBookieRegistered(String bookieId) throws BookieException;

    /**
     * Write the cookie data, which will be used for verifying the integrity of the bookie environment.
     *
     * @param bookieId bookie id
     * @param cookieData cookie data
     * @throws BookieException when fail to write cookie
     */
    void writeCookie(String bookieId, Versioned<byte[]> cookieData) throws BookieException;

    /**
     * Read the cookie data, which will be used for verifying the integrity of the bookie environment.
     *
     * @param bookieId bookie id
     * @return versioned cookie data
     * @throws BookieException when fail to read cookie
     */
    Versioned<byte[]> readCookie(String bookieId) throws BookieException;

    /**
     * Remove the cookie data.
     *
     * @param bookieId bookie id
     * @param version version of the cookie data
     * @throws BookieException when fail to remove cookie
     */
    void removeCookie(String bookieId, Version version) throws BookieException;

    /**
     * Prepare ledgers root node, availableNode, readonly node..
     *
     * @return Returns true if old data exists, false if not.
     */
    boolean prepareFormat() throws Exception;

    /**
     * Initializes new cluster by creating required znodes for the cluster. If
     * ledgersrootpath is already existing then it will error out.
     *
     * @return returns true if new cluster is successfully created or false if it failed to initialize.
     * @throws Exception
     */
    boolean initNewCluster() throws Exception;

    /**
     * Do format boolean.
     *
     * @return Returns true if success do format, false if not.
     */
    boolean format() throws Exception;

    /**
     * Nukes existing cluster metadata.
     *
     * @return returns true if cluster metadata is successfully nuked
     *          or false if it failed to nuke the cluster metadata.
     * @throws Exception
     */
    boolean nukeExistingCluster() throws Exception;
}
