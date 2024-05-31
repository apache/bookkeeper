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

package org.apache.bookkeeper.bookie.datainteg;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.bookie.CookieValidation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the CookieValidation interface that allows for auto-stamping
 * cookies when configured and used in conjunction with the data integrity service.
 * Because the data integrity service can heal a bookie with lost data due to a disk
 * failure, a bookie can auto stamp new cookies as part of the healing process.
 */
public class DataIntegrityCookieValidation implements CookieValidation {
    private static final Logger log = LoggerFactory.getLogger(DataIntegrityCookieValidation.class);
    private final ServerConfiguration conf;
    private final BookieId bookieId;
    private final RegistrationManager registrationManager;
    private final DataIntegrityCheck dataIntegCheck;

    public DataIntegrityCookieValidation(ServerConfiguration conf,
                                         RegistrationManager registrationManager,
                                         DataIntegrityCheck dataIntegCheck)
            throws UnknownHostException {
        this.conf = conf;
        this.registrationManager = registrationManager;
        this.bookieId = BookieImpl.getBookieId(conf);
        this.dataIntegCheck = dataIntegCheck;
    }

    private Optional<Versioned<Cookie>> getRegManagerCookie() throws BookieException {
        try {
            return Optional.of(Cookie.readFromRegistrationManager(registrationManager, bookieId));
        } catch (BookieException.CookieNotFoundException noCookieException) {
            return Optional.empty();
        }
    }

    private List<Optional<Cookie>> collectDirectoryCookies(List<File> directories) throws BookieException {
        List<Optional<Cookie>> cookies = new ArrayList<>();
        for (File d : directories) {
            try {
                cookies.add(Optional.of(Cookie.readFromDirectory(d)));
            } catch (FileNotFoundException fnfe) {
                cookies.add(Optional.empty());
            } catch (IOException ioe) {
                throw new BookieException.InvalidCookieException(ioe);
            }
        }
        return cookies;
    }

    private void stampCookie(Cookie masterCookie, Version expectedVersion, List<File> directories)
            throws BookieException {
        // stamp to ZK first as it's the authoritative cookie. If this fails part way through
        // stamping the directories, then a data integrity check will occur.
        log.info("Stamping cookie to ZK");
        masterCookie.writeToRegistrationManager(registrationManager, conf, expectedVersion);
        for (File d : directories) {
            try {
                log.info("Stamping cookie to directory {}", d);
                masterCookie.writeToDirectory(d);
            } catch (IOException ioe) {
                log.error("Exception writing cookie", ioe);
                throw new BookieException.InvalidCookieException(ioe);
            }
        }
    }

    @Override
    public void checkCookies(List<File> directories)
            throws BookieException, InterruptedException {
        String instanceId = registrationManager.getClusterInstanceId();
        if (instanceId == null) {
            throw new BookieException.InvalidCookieException("Cluster instance ID unavailable");
        }
        Cookie masterCookie;
        try {
            masterCookie = Cookie.generateCookie(conf).setInstanceId(instanceId).build();
        } catch (UnknownHostException uhe) {
            throw new BookieException.InvalidCookieException(uhe);
        }

        // collect existing cookies
        Optional<Versioned<Cookie>> regManagerCookie = getRegManagerCookie();
        List<Optional<Cookie>> directoryCookies = collectDirectoryCookies(directories);

        // if master is empty, everything must be empty, otherwise the cluster is messed up
        if (!regManagerCookie.isPresent()) {
            // if everything is empty, it's a new install, just stamp the cookies
            if (directoryCookies.stream().noneMatch(Optional::isPresent)) {
                log.info("New environment found. Stamping cookies");
                stampCookie(masterCookie, Version.NEW, directories);
            } else {
                String errorMsg =
                    "Cookie missing from ZK. Either it was manually deleted, "
                    + "or the bookie was started pointing to a different ZK cluster "
                    + "than the one it was originally started with. "
                    + "This requires manual intervention to fix";
                log.error(errorMsg);
                throw new BookieException.InvalidCookieException(errorMsg);
            }
        } else if (!regManagerCookie.get().getValue().equals(masterCookie)
                   || !directoryCookies.stream().allMatch(c -> c.map(masterCookie::equals).orElse(false))) {
            if (conf.isDataIntegrityStampMissingCookiesEnabled()) {
                log.warn("ZK cookie({}) or directory cookies({}) do not match master cookie ({}), running check",
                        regManagerCookie, directoryCookies, masterCookie);
                try {
                    dataIntegCheck.runPreBootCheck("INVALID_COOKIE").get();
                } catch (ExecutionException ee) {
                    if (ee.getCause() instanceof BookieException) {
                        throw (BookieException) ee.getCause();
                    } else {
                        throw new BookieException.InvalidCookieException(ee.getCause());
                    }
                }
                log.info("Environment should be in a sane state. Stamp new cookies");
                stampCookie(masterCookie, regManagerCookie.get().getVersion(), directories);
            } else {
                String errorMsg = MessageFormat.format(
                        "ZK cookie({0}) or directory cookies({1}) do not match master cookie ({2})"
                                + " and missing cookie stamping is disabled.",
                        regManagerCookie, directoryCookies, masterCookie);
                log.error(errorMsg);
                throw new BookieException.InvalidCookieException(errorMsg);
            }
        } // else all cookies match the masterCookie, meaning nothing has changed in the configuration
    }
}
