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
package org.apache.bookkeeper.bookie.environment.checker;

import com.google.common.collect.Lists;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class BookieEnvironmentChecker {
    private static final Logger LOG = LoggerFactory.getLogger(BookieEnvironmentChecker.class);
    private final ServerConfiguration conf;

    public BookieEnvironmentChecker(ServerConfiguration conf) {
        this.conf = conf;
    }

    /**
     * Check that the environment for the bookie is correct.
     * This means that the configuration has stayed the same as the
     * first run and the filesystem structure is up to date.
     */
    public void checkEnvironment(MetadataBookieDriver metadataDriver,
                                 List<File> journalDirectories,
                                 LedgerDirsManager ledgerDirsManager,
                                 LedgerDirsManager indexDirsManager)
            throws BookieException, IOException {
        List<File> allLedgerDirs = new ArrayList<File>(ledgerDirsManager.getAllLedgerDirs().size()
                + indexDirsManager.getAllLedgerDirs().size());
        allLedgerDirs.addAll(ledgerDirsManager.getAllLedgerDirs());
        if (indexDirsManager != ledgerDirsManager) {
            allLedgerDirs.addAll(indexDirsManager.getAllLedgerDirs());
        }
        if (metadataDriver == null) { // exists only for testing, just make sure directories are correct

            for (File journalDirectory : journalDirectories) {
                checkDirectoryStructure(journalDirectory);
            }

            for (File dir : allLedgerDirs) {
                checkDirectoryStructure(dir);
            }
            return;
        }

        checkEnvironmentWithStorageExpansion(conf, metadataDriver, journalDirectories, allLedgerDirs);

        checkIfDirsOnSameDiskPartition(allLedgerDirs);
        checkIfDirsOnSameDiskPartition(journalDirectories);
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
            LOG.error("Not all the new directories are empty. New directories that are not empty are: " + nonEmptyDirs);
            throw new BookieException.InvalidCookieException();
        }
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
            LOG.error("Either not all local directories have cookies or directories being added "
                    + " newly are not empty. "
                    + "Directories missing cookie file are: " + dirsMissingData
                    + " New directories that are not empty are: " + nonEmptyDirs);
            throw new BookieException.InvalidCookieException();
        }
    }

    public static void checkEnvironmentWithStorageExpansion(
            ServerConfiguration conf,
            MetadataBookieDriver metadataDriver,
            List<File> journalDirectories,
            List<File> allLedgerDirs) throws BookieException {
        RegistrationManager rm = metadataDriver.getRegistrationManager();
        try {
            // 1. retrieve the instance id
            String instanceId = rm.getClusterInstanceId();

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
                    masterCookie, rm, possibleBookieIds, allowExpansion);

            // 4. check if the cookie appear in all the directories.
            List<File> missedCookieDirs = new ArrayList<>();
            List<Cookie> existingCookies = Lists.newArrayList();
            if (null != rmCookie) {
                existingCookies.add(rmCookie.getValue());
            }

            // 4.1 verify the cookies in journal directories
            Pair<List<File>, List<Cookie>> journalResult =
                    verifyAndGetMissingDirs(masterCookie,
                            allowExpansion, journalDirectories);
            missedCookieDirs.addAll(journalResult.getLeft());
            existingCookies.addAll(journalResult.getRight());
            // 4.2. verify the cookies in ledger directories
            Pair<List<File>, List<Cookie>> ledgerResult =
                    verifyAndGetMissingDirs(masterCookie,
                            allowExpansion, allLedgerDirs);
            missedCookieDirs.addAll(ledgerResult.getLeft());
            existingCookies.addAll(ledgerResult.getRight());

            // 5. if there are directories missing cookies,
            //    this is either a:
            //    - new environment
            //    - a directory is being added
            //    - a directory has been corrupted/wiped, which is an error
            if (!missedCookieDirs.isEmpty()) {
                if (rmCookie == null) {
                    // 5.1 new environment: all directories should be empty
                    verifyDirsForNewEnvironment(missedCookieDirs);
                    stampNewCookie(conf, masterCookie, rm, Version.NEW,
                            journalDirectories, allLedgerDirs);
                } else if (allowExpansion) {
                    // 5.2 storage is expanding
                    Set<File> knownDirs = getKnownDirs(existingCookies);
                    verifyDirsForStorageExpansion(missedCookieDirs, knownDirs);
                    stampNewCookie(conf, masterCookie,
                            rm, rmCookie.getVersion(),
                            journalDirectories, allLedgerDirs);
                } else {
                    // 5.3 Cookie-less directories and
                    //     we can't do anything with them
                    LOG.error("There are directories without a cookie,"
                            + " and this is neither a new environment,"
                            + " nor is storage expansion enabled. "
                            + "Empty directories are {}", missedCookieDirs);
                    throw new BookieException.InvalidCookieException();
                }
            } else {
                if (rmCookie == null) {
                    // No corresponding cookie found in registration manager. The bookie should fail to come up.
                    LOG.error("Cookie for this bookie is not stored in metadata store. Bookie failing to come up");
                    throw new BookieException.InvalidCookieException();
                }
            }
        } catch (IOException ioe) {
            LOG.error("Error accessing cookie on disks", ioe);
            throw new BookieException.InvalidCookieException(ioe);
        }
    }


    static List<BookieId> possibleBookieIds(ServerConfiguration conf)
            throws BookieException {
        // we need to loop through all possible bookie identifiers to ensure it is treated as a new environment
        // just because of bad configuration
        List<BookieId> addresses = Lists.newArrayListWithExpectedSize(3);
        // we are checking all possibilities here, so we don't need to fail if we can only get
        // loopback address. it will fail anyway when the bookie attempts to listen on loopback address.
        try {
            // ip address
            addresses.add(Bookie.getBookieAddress(
                    new ServerConfiguration(conf)
                            .setUseHostNameAsBookieID(false)
                            .setAdvertisedAddress(null)
                            .setAllowLoopback(true)
            ).toBookieId());
            // host name
            addresses.add(Bookie.getBookieAddress(
                    new ServerConfiguration(conf)
                            .setUseHostNameAsBookieID(true)
                            .setAdvertisedAddress(null)
                            .setAllowLoopback(true)
            ).toBookieId());
            // advertised address
            if (null != conf.getAdvertisedAddress()) {
                addresses.add(Bookie.getBookieAddress(conf).toBookieId());
            }
            if (null != conf.getBookieId()) {
                addresses.add(BookieId.parse(conf.getBookieId()));
            }
        } catch (UnknownHostException e) {
            throw new BookieException.UnknownBookieIdException(e);
        }
        return addresses;
    }

    static Versioned<Cookie> readAndVerifyCookieFromRegistrationManager(
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
            checkDirectoryStructure(dir);
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

    private static void stampNewCookie(ServerConfiguration conf,
                                       Cookie masterCookie,
                                       RegistrationManager rm,
                                       Version version,
                                       List<File> journalDirectories,
                                       List<File> allLedgerDirs)
            throws BookieException, IOException {
        // backfill all the directories that miss cookies (for storage expansion)
        LOG.info("Stamping new cookies on all dirs {} {}",
                journalDirectories, allLedgerDirs);
        for (File journalDirectory : journalDirectories) {
            masterCookie.writeToDirectory(journalDirectory);
        }
        for (File dir : allLedgerDirs) {
            masterCookie.writeToDirectory(dir);
        }
        masterCookie.writeToRegistrationManager(rm, conf, version);
    }







    public static void checkDirectoryStructure(File dir) throws IOException {
        if (!dir.exists()) {
            File parent = dir.getParentFile();
            File preV3versionFile = new File(dir.getParent(),
                    BookKeeperConstants.VERSION_FILENAME);

            final AtomicBoolean oldDataExists = new AtomicBoolean(false);
            parent.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.endsWith(".txn") || name.endsWith(".idx") || name.endsWith(".log")) {
                        oldDataExists.set(true);
                    }
                    return true;
                }
            });
            if (preV3versionFile.exists() || oldDataExists.get()) {
                String err = "Directory layout version is less than 3, upgrade needed";
                LOG.error(err);
                throw new IOException(err);
            }
            if (!dir.mkdirs()) {
                String err = "Unable to create directory " + dir;
                LOG.error(err);
                throw new IOException(err);
            }
        }
    }

    /**
     * Checks if multiple directories are in same diskpartition/filesystem/device.
     * If ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION config parameter is not enabled, and
     * if it is found that there are multiple directories in the same DiskPartition then
     * it will throw DiskPartitionDuplicationException.
     *
     * @param dirs dirs to validate
     * @throws IOException
     */
    private void checkIfDirsOnSameDiskPartition(List<File> dirs) throws BookieException.DiskPartitionDuplicationException {
        boolean allowDiskPartitionDuplication = conf.isAllowMultipleDirsUnderSameDiskPartition();
        final MutableBoolean isDuplicationFoundAndNotAllowed = new MutableBoolean(false);
        Map<FileStore, List<File>> fileStoreDirsMap = new HashMap<FileStore, List<File>>();
        for (File dir : dirs) {
            FileStore fileStore;
            try {
                fileStore = Files.getFileStore(dir.toPath());
            } catch (IOException e) {
                LOG.error("Got IOException while trying to FileStore of {}", dir);
                throw new BookieException.DiskPartitionDuplicationException(e);
            }
            if (fileStoreDirsMap.containsKey(fileStore)) {
                fileStoreDirsMap.get(fileStore).add(dir);
            } else {
                List<File> dirsList = new ArrayList<File>();
                dirsList.add(dir);
                fileStoreDirsMap.put(fileStore, dirsList);
            }
        }

        fileStoreDirsMap.forEach((fileStore, dirsList) -> {
            if (dirsList.size() > 1) {
                if (allowDiskPartitionDuplication) {
                    LOG.warn("Dirs: {} are in same DiskPartition/FileSystem: {}", dirsList, fileStore);
                } else {
                    LOG.error("Dirs: {} are in same DiskPartition/FileSystem: {}", dirsList, fileStore);
                    isDuplicationFoundAndNotAllowed.setValue(true);
                }
            }
        });
        if (isDuplicationFoundAndNotAllowed.getValue()) {
            throw new BookieException.DiskPartitionDuplicationException();
        }
    }
}
