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
package org.apache.bookkeeper.metadata.etcd;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Etcd registration manager.
 */
class EtcdRegistrationManager implements RegistrationManager {
    @Override
    public RegistrationManager initialize(ServerConfiguration conf, RegistrationListener listener, StatsLogger statsLogger) throws BookieException {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public String getClusterInstanceId() throws BookieException {
        return null;
    }

    @Override
    public void registerBookie(String bookieId, boolean readOnly) throws BookieException {

    }

    @Override
    public void unregisterBookie(String bookieId, boolean readOnly) throws BookieException {

    }

    @Override
    public boolean isBookieRegistered(String bookieId) throws BookieException {
        return false;
    }

    @Override
    public void writeCookie(String bookieId, Versioned<byte[]> cookieData) throws BookieException {

    }

    @Override
    public Versioned<byte[]> readCookie(String bookieId) throws BookieException {
        return null;
    }

    @Override
    public void removeCookie(String bookieId, Version version) throws BookieException {

    }

    @Override
    public LayoutManager getLayoutManager() {
        return null;
    }

    @Override
    public boolean prepareFormat() throws Exception {
        return false;
    }

    @Override
    public boolean initNewCluster() throws Exception {
        return false;
    }

    @Override
    public boolean format() throws Exception {
        return false;
    }

    @Override
    public boolean nukeExistingCluster() throws Exception {
        return false;
    }
}
