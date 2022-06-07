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
package org.apache.bookkeeper.bookie;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Legacy implementation of CookieValidation.
 */
public class LegacyCookieValidation implements CookieValidation {
    private static final Logger log = LoggerFactory.getLogger(LegacyCookieValidation.class);

    private final ServerConfiguration conf;
    private final RegistrationManager registrationManager;

    public LegacyCookieValidation(ServerConfiguration conf,
                                  RegistrationManager registrationManager) {
        this.conf = conf;
        this.registrationManager = registrationManager;
    }

    @Override
    public void checkCookies(List<File> directories) throws BookieException {
        try {
            // 1. retrieve the instance id
            String instanceId = registrationManager.getClusterInstanceId();

            // 2. build the master cookie from the configuration
            Cookie.Builder builder = Cookie.generateCookie(conf);
            if (null != instanceId) {
                builder.setInstanceId(instanceId);
            }
            Cookie masterCookie = builder.build();
            boolean allowExpansion = conf.getAllowStorageExpansion();

            // 3. read the cookie from registration manager. it is the `source-of-truth` of a given bookie.
            //    if it doesn't exist in registration manager, this bookie is a new bookie, otherwise it is
            //    an old bookie.
            List<BookieId> possibleBookieIds = possibleBookieIds(conf);
            final Versioned<Cookie> rmCookie = readAndVerifyCookieFromRegistrationManager(
                    masterCookie, registrationManager, possibleBookieIds, allowExpansion);

            // 4. check if the cookie appear in all the directories.
            List<File> missedCookieDirs = new ArrayList<>();
            List<Cookie> existingCookies = Lists.newArrayList();
            if (null != rmCookie) {
                existingCookies.add(rmCookie.getValue());
            }

            // 4.1 verify the cookies in journal directories
            Pair<List<File>, List<Cookie>> result =
                    verifyAndGetMissingDirs(masterCookie,
                            allowExpansion, directories);
            missedCookieDirs.addAll(result.getLeft());
            existingCookies.addAll(result.getRight());

            // 5. if there are directories missing cookies,
            //    this is either a:
            //    - new environment
            //    - a directory is being added
            //    - a directory has been corrupted/wiped, which is an error
            if (!missedCookieDirs.isEmpty()) {
                if (rmCookie == null) {
                    // 5.1 new environment: all directories should be empty
                    verifyDirsForNewEnvironment(missedCookieDirs);
                    stampNewCookie(conf, masterCookie, registrationManager,
                            Version.NEW, directories);
                } else if (allowExpansion) {
                    // 5.2 storage is expanding
                    Set<File> knownDirs = getKnownDirs(existingCookies);
                    verifyDirsForStorageExpansion(missedCookieDirs, knownDirs);
                    stampNewCookie(conf, masterCookie, registrationManager,
                            rmCookie.getVersion(), directories);
                } else {
                    // 5.3 Cookie-less directories and
                    //     we can't do anything with them
                    log.error("There are directories without a cookie,"
                            + " and this is neither a new environment,"
                            + " nor is storage expansion enabled. "
                            + "Empty directories are {}", missedCookieDirs);
                    throw new BookieException.InvalidCookieException();
                }
            } else {
                if (rmCookie == null) {
                    // No corresponding cookie found in registration manager. The bookie should fail to come up.
                    log.error("Cookie for this bookie is not stored in metadata store. Bookie failing to come up");
                    throw new BookieException.InvalidCookieException();
                }
            }
        } catch (IOException ioe) {
            log.error("Error accessing cookie on disks", ioe);
            throw new BookieException.InvalidCookieException(ioe);
        }
    }

    private static List<BookieId> possibleBookieIds(ServerConfiguration conf)
            throws BookieException {
        // we need to loop through all possible bookie identifiers to ensure it is treated as a new environment
        // just because of bad configuration
        List<BookieId> addresses = Lists.newArrayListWithExpectedSize(3);
        // we are checking all possibilities here, so we don't need to fail if we can only get
        // loopback address. it will fail anyway when the bookie attempts to listen on loopback address.
        try {
            if (null != conf.getBookieId()) {
                // If BookieID is configured, it takes precedence over default network information used as id.
                addresses.add(BookieImpl.getBookieId(conf));
            } else {
                // ip address
                addresses.add(BookieImpl.getBookieAddress(
                        new ServerConfiguration(conf)
                                .setUseHostNameAsBookieID(false)
                                .setAdvertisedAddress(null)
                                .setAllowLoopback(true)
                ).toBookieId());
                // host name
                addresses.add(BookieImpl.getBookieAddress(
                        new ServerConfiguration(conf)
                                .setUseHostNameAsBookieID(true)
                                .setAdvertisedAddress(null)
                                .setAllowLoopback(true)
                ).toBookieId());
                // advertised address
                if (null != conf.getAdvertisedAddress()) {
                    addresses.add(BookieImpl.getBookieAddress(conf).toBookieId());
                }
            }
        } catch (UnknownHostException e) {
            throw new BookieException.UnknownBookieIdException(e);
        }
        return addresses;
    }

    private static Versioned<Cookie> readAndVerifyCookieFromRegistrationManager(
            Cookie masterCookie, RegistrationManager rm,
            List<BookieId> addresses, boolean allowExpansion)
            throws BookieException {
        Versioned<Cookie> rmCookie = null;
        for (BookieId address : addresses) {
            try {
                rmCookie = Cookie.readFromRegistrationManager(rm, address);
                // If allowStorageExpansion option is set, we should
                // make sure that the new set of ledger/index dirs
                // is a super set of the old; else, we fail the cookie check
                if (allowExpansion) {
                    masterCookie.verifyIsSuperSet(rmCookie.getValue());
                } else {
                    masterCookie.verify(rmCookie.getValue());
                }
            } catch (BookieException.CookieNotFoundException e) {
                continue;
            }
        }
        return rmCookie;
    }

    private static Pair<List<File>, List<Cookie>> verifyAndGetMissingDirs(
            Cookie masterCookie, boolean allowExpansion, List<File> dirs)
            throws BookieException.InvalidCookieException, IOException {
        List<File> missingDirs = Lists.newArrayList();
        List<Cookie> existedCookies = Lists.newArrayList();
        for (File dir : dirs) {
            try {
                Cookie c = Cookie.readFromDirectory(dir);
                if (allowExpansion) {
                    masterCookie.verifyIsSuperSet(c);
                } else {
                    masterCookie.verify(c);
                }
                existedCookies.add(c);
            } catch (FileNotFoundException fnf) {
                missingDirs.add(dir);
            }
        }
        return Pair.of(missingDirs, existedCookies);
    }

    private static void verifyDirsForNewEnvironment(List<File> missedCookieDirs)
            throws BookieException.InvalidCookieException {
        List<File> nonEmptyDirs = new ArrayList<>();
        for (File dir : missedCookieDirs) {
            String[] content = dir.list();
            if (content != null && content.length != 0) {
                nonEmptyDirs.add(dir);
            }
        }
        if (!nonEmptyDirs.isEmpty()) {
            log.error("Not all the new directories are empty. New directories that are not empty are: " + nonEmptyDirs);
            throw new BookieException.InvalidCookieException();
        }
    }

    private static void stampNewCookie(ServerConfiguration conf,
                                       Cookie masterCookie,
                                       RegistrationManager rm,
                                       Version version,
                                       List<File> dirs)
            throws BookieException, IOException {
        // backfill all the directories that miss cookies (for storage expansion)
        log.info("Stamping new cookies on all dirs {}", dirs);
        for (File dir : dirs) {
            masterCookie.writeToDirectory(dir);
        }
        masterCookie.writeToRegistrationManager(rm, conf, version);
    }

    private static Set<File> getKnownDirs(List<Cookie> cookies) {
        return cookies.stream()
                .flatMap((c) -> Arrays.stream(c.getLedgerDirPathsFromCookie()))
                .map((s) -> new File(s))
                .collect(Collectors.toSet());
    }

    private static void verifyDirsForStorageExpansion(
            List<File> missedCookieDirs,
            Set<File> existingLedgerDirs) throws BookieException.InvalidCookieException {

        List<File> dirsMissingData = new ArrayList<File>();
        List<File> nonEmptyDirs = new ArrayList<File>();
        for (File dir : missedCookieDirs) {
            if (existingLedgerDirs.contains(dir.getParentFile())) {
                // if one of the existing ledger dirs doesn't have cookie,
                // let us not proceed further
                dirsMissingData.add(dir);
                continue;
            }
            String[] content = dir.list();
            if (content != null && content.length != 0) {
                nonEmptyDirs.add(dir);
            }
        }
        if (dirsMissingData.size() > 0 || nonEmptyDirs.size() > 0) {
            log.error("Either not all local directories have cookies or directories being added "
                    + " newly are not empty. "
                    + "Directories missing cookie file are: " + dirsMissingData
                    + " New directories that are not empty are: " + nonEmptyDirs);
            throw new BookieException.InvalidCookieException();
        }
    }
}