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

package org.apache.bookkeeper.discover;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Mock implementation of RegistrationManager.
 */
public class MockRegistrationManager implements RegistrationManager {
    private final ConcurrentHashMap<BookieId, Versioned<byte[]>> cookies = new ConcurrentHashMap<>();

    @Override
    public void close() {}

    @Override
    public String getClusterInstanceId() throws BookieException {
        return "mock-cluster";
    }

    @Override
    public void registerBookie(BookieId bookieId, boolean readOnly,
                               BookieServiceInfo serviceInfo) throws BookieException {
        throw new UnsupportedOperationException("Not implemented in mock. Implement if you need it");
    }

    @Override
    public void unregisterBookie(BookieId bookieId, boolean readOnly) throws BookieException {
        throw new UnsupportedOperationException("Not implemented in mock. Implement if you need it");
    }

    @Override
    public boolean isBookieRegistered(BookieId bookieId) throws BookieException {
        throw new UnsupportedOperationException("Not implemented in mock. Implement if you need it");
    }

    @Override
    public void writeCookie(BookieId bookieId, Versioned<byte[]> cookieData) throws BookieException {
        try {
            cookies.compute(bookieId, (bookieId1, current) -> {
                    if (cookieData.getVersion() == Version.NEW) {
                        if (current == null) {
                            return new Versioned<byte[]>(cookieData.getValue(), new LongVersion(1));
                        } else {
                            throw new RuntimeException(new BookieException.CookieExistException(bookieId.getId()));
                        }
                    } else {
                        if (current != null
                            && cookieData.getVersion().equals(current.getVersion())) {
                            LongVersion oldVersion = (LongVersion) current.getVersion();
                            LongVersion newVersion = new LongVersion(oldVersion.getLongVersion() + 1);
                            return new Versioned<byte[]>(cookieData.getValue(), newVersion);
                        } else {
                            throw new RuntimeException(new BookieException.CookieExistException(bookieId.getId()));
                        }
                    }
                });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof BookieException) {
                throw (BookieException) e.getCause();
            }
        }
    }

    @Override
    public Versioned<byte[]> readCookie(BookieId bookieId) throws BookieException {
        Versioned<byte[]> cookie = cookies.get(bookieId);
        if (cookie == null) {
            throw new BookieException.CookieNotFoundException(bookieId.toString());
        }
        return cookie;
    }

    @Override
    public void removeCookie(BookieId bookieId, Version version) throws BookieException {
        try {
            cookies.compute(bookieId, (bookieId1, current) -> {
                    if (current == null) {
                        throw new RuntimeException(new BookieException.CookieNotFoundException(bookieId.toString()));
                    } else if (current.getVersion().equals(version)) {
                        return null;
                    } else {
                        throw new RuntimeException(new BookieException.MetadataStoreException("Bad version"));
                    }
                });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof BookieException) {
                throw (BookieException) e.getCause();
            }
        }

    }

    @Override
    public boolean prepareFormat() throws Exception {
        throw new UnsupportedOperationException("Not implemented in mock. Implement if you need it");
    }

    @Override
    public boolean initNewCluster() throws Exception {
        throw new UnsupportedOperationException("Not implemented in mock. Implement if you need it");
    }

    @Override
    public boolean format() throws Exception {
        throw new UnsupportedOperationException("Not implemented in mock. Implement if you need it");
    }

    @Override
    public boolean nukeExistingCluster() throws Exception {
        throw new UnsupportedOperationException("Not implemented in mock. Implement if you need it");
    }

    @Override
    public void addRegistrationListener(RegistrationListener listener) {
        throw new UnsupportedOperationException("Not implemented in mock. Implement if you need it");
    }
}
