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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_ADD_ENTRY_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_READ_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_READ_ENTRY_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_RECOVERY_ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SERVER_STATUS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WRITE_BYTES;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.bookkeeper.bookie.BookieException.BookieIllegalOpException;
import org.apache.bookkeeper.bookie.BookieException.CookieNotFoundException;
import org.apache.bookkeeper.bookie.BookieException.DiskPartitionDuplicationException;
import org.apache.bookkeeper.bookie.BookieException.InvalidCookieException;
import org.apache.bookkeeper.bookie.BookieException.MetadataStoreException;
import org.apache.bookkeeper.bookie.BookieException.UnknownBookieIdException;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a bookie.
 */
public class Bookie extends BookieCriticalThread {

    private static final Logger LOG = LoggerFactory.getLogger(Bookie.class);

    final List<File> journalDirectories;
    final ServerConfiguration conf;

    final SyncThread syncThread;
    final LedgerManagerFactory ledgerManagerFactory;
    final LedgerManager ledgerManager;
    final LedgerStorage ledgerStorage;
    final List<Journal> journals;

    final HandleFactory handles;

    static final long METAENTRY_ID_LEDGER_KEY = -0x1000;
    static final long METAENTRY_ID_FENCE_KEY  = -0x2000;

    private final LedgerDirsManager ledgerDirsManager;
    private LedgerDirsManager indexDirsManager;

    LedgerDirsMonitor ledgerMonitor;
    LedgerDirsMonitor idxMonitor;

    // Registration Manager for managing registration
    RegistrationManager registrationManager;

    // Running flag
    private volatile boolean running = false;
    // Flag identify whether it is in shutting down progress
    private volatile boolean shuttingdown = false;
    // Bookie status
    private final BookieStatus bookieStatus = new BookieStatus();

    private int exitCode = ExitCode.OK;

    private final ConcurrentLongHashMap<byte[]> masterKeyCache = new ConcurrentLongHashMap<>();

    protected final String bookieId;

    private final AtomicBoolean rmRegistered = new AtomicBoolean(false);
    protected final AtomicBoolean forceReadOnly = new AtomicBoolean(false);
    // executor to manage the state changes for a bookie.
    final ExecutorService stateService = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("BookieStateService-%d").build());

    // Expose Stats
    private final StatsLogger statsLogger;
    private final Counter writeBytes;
    private final Counter readBytes;
    // Bookie Operation Latency Stats
    private final OpStatsLogger addEntryStats;
    private final OpStatsLogger recoveryAddEntryStats;
    private final OpStatsLogger readEntryStats;
    // Bookie Operation Bytes Stats
    private final OpStatsLogger addBytesStats;
    private final OpStatsLogger readBytesStats;

    /**
     * Exception is thrown when no such a ledger is found in this bookie.
     */
    public static class NoLedgerException extends IOException {
        private static final long serialVersionUID = 1L;
        private final long ledgerId;
        public NoLedgerException(long ledgerId) {
            super("Ledger " + ledgerId + " not found");
            this.ledgerId = ledgerId;
        }
        public long getLedgerId() {
            return ledgerId;
        }
    }

    /**
     * Exception is thrown when no such an entry is found in this bookie.
     */
    public static class NoEntryException extends IOException {
        private static final long serialVersionUID = 1L;
        private final long ledgerId;
        private final long entryId;
        public NoEntryException(long ledgerId, long entryId) {
            this("Entry " + entryId + " not found in " + ledgerId, ledgerId, entryId);
        }

        public NoEntryException(String msg, long ledgerId, long entryId) {
            super(msg);
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        public long getLedger() {
            return ledgerId;
        }
        public long getEntry() {
            return entryId;
        }
    }

    // Write Callback do nothing
    static class NopWriteCallback implements WriteCallback {
        @Override
        public void writeComplete(int rc, long ledgerId, long entryId,
                                  BookieSocketAddress addr, Object ctx) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Finished writing entry {} @ ledger {} for {} : {}",
                        entryId, ledgerId, addr, rc);
            }
        }
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

    @VisibleForTesting
    public void setRegistrationManager(RegistrationManager rm) {
        this.registrationManager = rm;
    }

    @VisibleForTesting
    public RegistrationManager getRegistrationManager() {
        return this.registrationManager;
    }

    /**
     * Check that the environment for the bookie is correct.
     * This means that the configuration has stayed the same as the
     * first run and the filesystem structure is up to date.
     */
    private void checkEnvironment(RegistrationManager rm) throws BookieException, IOException {
        List<File> allLedgerDirs = new ArrayList<File>(ledgerDirsManager.getAllLedgerDirs().size()
                                                     + indexDirsManager.getAllLedgerDirs().size());
        allLedgerDirs.addAll(ledgerDirsManager.getAllLedgerDirs());
        if (indexDirsManager != ledgerDirsManager) {
            allLedgerDirs.addAll(indexDirsManager.getAllLedgerDirs());
        }
        if (rm == null) { // exists only for testing, just make sure directories are correct

            for (File journalDirectory : journalDirectories) {
                checkDirectoryStructure(journalDirectory);
            }

            for (File dir : allLedgerDirs) {
                checkDirectoryStructure(dir);
            }
            return;
        }

        checkEnvironmentWithStorageExpansion(conf, rm, journalDirectories, allLedgerDirs);

        checkIfDirsOnSameDiskPartition(allLedgerDirs);
        checkIfDirsOnSameDiskPartition(journalDirectories);
    }

    /**
     * Checks if multiple directories are in same diskpartition/filesystem/device.
     * If ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION config parameter is not enabled, and
     * if it is found that there are multiple directories in the same DiskPartition then
     * it will throw DiskPartitionDuplicationException.
     *
     * @param dirs dirs to validate
     *
     * @throws IOException
     */
    private void checkIfDirsOnSameDiskPartition(List<File> dirs) throws DiskPartitionDuplicationException {
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

    static List<BookieSocketAddress> possibleBookieIds(ServerConfiguration conf)
            throws BookieException {
        // we need to loop through all possible bookie identifiers to ensure it is treated as a new environment
        // just because of bad configuration
        List<BookieSocketAddress> addresses = Lists.newArrayListWithExpectedSize(3);
        try {
            // ip address
            addresses.add(getBookieAddress(
                new ServerConfiguration(conf).setUseHostNameAsBookieID(false).setAdvertisedAddress(null)));
            // host name
            addresses.add(getBookieAddress(
                new ServerConfiguration(conf).setUseHostNameAsBookieID(true).setAdvertisedAddress(null)));
            // advertised address
            if (null != conf.getAdvertisedAddress()) {
                addresses.add(getBookieAddress(conf));
            }
        } catch (UnknownHostException e) {
            throw new UnknownBookieIdException(e);
        }
        return addresses;
    }

    static Versioned<Cookie> readAndVerifyCookieFromRegistrationManager(
            Cookie masterCookie, RegistrationManager rm,
            List<BookieSocketAddress> addresses, boolean allowExpansion)
            throws BookieException {
        Versioned<Cookie> rmCookie = null;
        for (BookieSocketAddress address : addresses) {
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
            } catch (CookieNotFoundException e) {
                continue;
            }
        }
        return rmCookie;
    }

    private static Pair<List<File>, List<Cookie>> verifyAndGetMissingDirs(
            Cookie masterCookie, boolean allowExpansion, List<File> dirs)
            throws InvalidCookieException, IOException {
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

    public static void checkEnvironmentWithStorageExpansion(
            ServerConfiguration conf,
            RegistrationManager rm,
            List<File> journalDirectories,
            List<File> allLedgerDirs) throws BookieException {
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
            List<BookieSocketAddress> possibleBookieIds = possibleBookieIds(conf);
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
                    throw new InvalidCookieException();
                }
            }
        } catch (IOException ioe) {
            LOG.error("Error accessing cookie on disks", ioe);
            throw new BookieException.InvalidCookieException(ioe);
        }
    }

    private static void verifyDirsForNewEnvironment(List<File> missedCookieDirs)
            throws InvalidCookieException {
        List<File> nonEmptyDirs = new ArrayList<>();
        for (File dir : missedCookieDirs) {
            String[] content = dir.list();
            if (content != null && content.length != 0) {
                nonEmptyDirs.add(dir);
            }
        }
        if (!nonEmptyDirs.isEmpty()) {
            LOG.error("Not all the new directories are empty. New directories that are not empty are: " + nonEmptyDirs);
            throw new InvalidCookieException();
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
            Set<File> existingLedgerDirs) throws InvalidCookieException {

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
            throw new InvalidCookieException();
        }
    }

    /**
     * Return the configured address of the bookie.
     */
    public static BookieSocketAddress getBookieAddress(ServerConfiguration conf)
            throws UnknownHostException {
        // Advertised address takes precedence over the listening interface and the
        // useHostNameAsBookieID settings
        if (conf.getAdvertisedAddress() != null && conf.getAdvertisedAddress().trim().length() > 0) {
            String hostAddress = conf.getAdvertisedAddress().trim();
            return new BookieSocketAddress(hostAddress, conf.getBookiePort());
        }

        String iface = conf.getListeningInterface();
        if (iface == null) {
            iface = "default";
        }
        String hostName = DNS.getDefaultHost(iface);
        InetSocketAddress inetAddr = new InetSocketAddress(hostName, conf.getBookiePort());
        if (inetAddr.isUnresolved()) {
            throw new UnknownHostException("Unable to resolve default hostname: "
                    + hostName + " for interface: " + iface);
        }
        String hostAddress = inetAddr.getAddress().getHostAddress();
        if (conf.getUseHostNameAsBookieID()) {
            hostAddress = inetAddr.getAddress().getCanonicalHostName();
        }

        BookieSocketAddress addr =
                new BookieSocketAddress(hostAddress, conf.getBookiePort());
        if (addr.getSocketAddress().getAddress().isLoopbackAddress()
            && !conf.getAllowLoopback()) {
            throw new UnknownHostException("Trying to listen on loopback address, "
                    + addr + " but this is forbidden by default "
                    + "(see ServerConfiguration#getAllowLoopback())");
        }
        return addr;
    }

    public LedgerDirsManager getLedgerDirsManager() {
        return ledgerDirsManager;
    }

    LedgerDirsManager getIndexDirsManager() {
        return indexDirsManager;
    }

    public long getTotalDiskSpace() throws IOException {
        return getLedgerDirsManager().getTotalDiskSpace(ledgerDirsManager.getAllLedgerDirs());
    }

    public long getTotalFreeSpace() throws IOException {
        return getLedgerDirsManager().getTotalFreeSpace(ledgerDirsManager.getAllLedgerDirs());
    }

    public static File getCurrentDirectory(File dir) {
        return new File(dir, BookKeeperConstants.CURRENT_DIR);
    }

    public static File[] getCurrentDirectories(File[] dirs) {
        File[] currentDirs = new File[dirs.length];
        for (int i = 0; i < dirs.length; i++) {
            currentDirs[i] = getCurrentDirectory(dirs[i]);
        }
        return currentDirs;
    }

    public Bookie(ServerConfiguration conf)
            throws IOException, InterruptedException, BookieException {
        this(conf, NullStatsLogger.INSTANCE);
    }

    public Bookie(ServerConfiguration conf, StatsLogger statsLogger)
            throws IOException, InterruptedException, BookieException {
        super("Bookie-" + conf.getBookiePort());
        this.statsLogger = statsLogger;
        this.conf = conf;
        this.journalDirectories = Lists.newArrayList();
        for (File journalDirectory : conf.getJournalDirs()) {
            this.journalDirectories.add(getCurrentDirectory(journalDirectory));
        }
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        this.ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(), diskChecker,
                statsLogger.scope(LD_LEDGER_SCOPE));

        File[] idxDirs = conf.getIndexDirs();
        if (null == idxDirs) {
            this.indexDirsManager = this.ledgerDirsManager;
        } else {
            this.indexDirsManager = new LedgerDirsManager(conf, idxDirs, diskChecker,
                    statsLogger.scope(LD_INDEX_SCOPE));
        }

        // instantiate zookeeper client to initialize ledger manager
        this.registrationManager = instantiateRegistrationManager(conf);
        checkEnvironment(this.registrationManager);
        try {
            ZooKeeper zooKeeper = null;  // ZooKeeper is null existing only for testing
            if (registrationManager != null) {
                zooKeeper = ((ZKRegistrationManager) this.registrationManager).getZk();
                // current the registration manager is zookeeper only
                ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(
                    conf,
                    zooKeeper);
                LOG.info("instantiate ledger manager {}", ledgerManagerFactory.getClass().getName());
                ledgerManager = ledgerManagerFactory.newLedgerManager();
            } else {
                ledgerManagerFactory = null;
                ledgerManager = null;
            }
        } catch (KeeperException e) {
            throw new MetadataStoreException("Failed to initialize ledger manager", e);
        }

        // Initialise ledgerDirMonitor. This would look through all the
        // configured directories. When disk errors or all the ledger
        // directories are full, would throws exception and fail bookie startup.
        this.ledgerMonitor = new LedgerDirsMonitor(conf, diskChecker, ledgerDirsManager);
        try {
            this.ledgerMonitor.init();
        } catch (NoWritableLedgerDirException nle) {
            // start in read-only mode if no writable dirs and read-only allowed
            if (!conf.isReadOnlyModeEnabled()) {
                throw nle;
            } else {
                this.transitionToReadOnlyMode();
            }
        }

        if (null == idxDirs) {
            this.idxMonitor = this.ledgerMonitor;
        } else {
            this.idxMonitor = new LedgerDirsMonitor(conf, diskChecker, indexDirsManager);
            try {
                this.idxMonitor.init();
            } catch (NoWritableLedgerDirException nle) {
                // start in read-only mode if no writable dirs and read-only allowed
                if (!conf.isReadOnlyModeEnabled()) {
                    throw nle;
                } else {
                    this.transitionToReadOnlyMode();
                }
            }
        }

        // ZK ephemeral node for this Bookie.
        this.bookieId = getMyId();

        // instantiate the journals
        journals = Lists.newArrayList();
        for (int i = 0; i < journalDirectories.size(); i++) {
            journals.add(new Journal(journalDirectories.get(i),
                         conf, ledgerDirsManager, statsLogger.scope(JOURNAL_SCOPE + "_" + i)));
        }

        CheckpointSource checkpointSource = new CheckpointSourceList(journals);

        // Instantiate the ledger storage implementation
        String ledgerStorageClass = conf.getLedgerStorageClass();
        LOG.info("Using ledger storage: {}", ledgerStorageClass);
        ledgerStorage = LedgerStorageFactory.createLedgerStorage(ledgerStorageClass);
        syncThread = new SyncThread(conf, getLedgerDirsListener(), ledgerStorage, checkpointSource);

        ledgerStorage.initialize(
            conf,
            ledgerManager,
            ledgerDirsManager,
            indexDirsManager,
            checkpointSource,
            syncThread,
            statsLogger);


        handles = new HandleFactoryImpl(ledgerStorage);

        // Expose Stats
        writeBytes = statsLogger.getCounter(WRITE_BYTES);
        readBytes = statsLogger.getCounter(READ_BYTES);
        addEntryStats = statsLogger.getOpStatsLogger(BOOKIE_ADD_ENTRY);
        recoveryAddEntryStats = statsLogger.getOpStatsLogger(BOOKIE_RECOVERY_ADD_ENTRY);
        readEntryStats = statsLogger.getOpStatsLogger(BOOKIE_READ_ENTRY);
        addBytesStats = statsLogger.getOpStatsLogger(BOOKIE_ADD_ENTRY_BYTES);
        readBytesStats = statsLogger.getOpStatsLogger(BOOKIE_READ_ENTRY_BYTES);
        // 1 : up, 0 : readonly, -1 : unregistered
        statsLogger.registerGauge(SERVER_STATUS, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                if (!rmRegistered.get()){
                    return -1;
                } else if (forceReadOnly.get() || bookieStatus.isInReadOnlyMode()) {
                    return 0;
                } else {
                    return 1;
                }
            }
        });
    }

    private String getMyId() throws UnknownHostException {
        return Bookie.getBookieAddress(conf).toString();
    }

    void readJournal() throws IOException, BookieException {
        long startTs = MathUtils.now();
        JournalScanner scanner = new JournalScanner() {
            @Override
            public void process(int journalVersion, long offset, ByteBuffer recBuff) throws IOException {
                long ledgerId = recBuff.getLong();
                long entryId = recBuff.getLong();
                try {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Replay journal - ledger id : {}, entry id : {}.", ledgerId, entryId);
                    }
                    if (entryId == METAENTRY_ID_LEDGER_KEY) {
                        if (journalVersion >= JournalChannel.V3) {
                            int masterKeyLen = recBuff.getInt();
                            byte[] masterKey = new byte[masterKeyLen];

                            recBuff.get(masterKey);
                            masterKeyCache.put(ledgerId, masterKey);
                        } else {
                            throw new IOException("Invalid journal. Contains journalKey "
                                    + " but layout version (" + journalVersion
                                    + ") is too old to hold this");
                        }
                    } else if (entryId == METAENTRY_ID_FENCE_KEY) {
                        if (journalVersion >= JournalChannel.V4) {
                            byte[] key = masterKeyCache.get(ledgerId);
                            if (key == null) {
                                key = ledgerStorage.readMasterKey(ledgerId);
                            }
                            LedgerDescriptor handle = handles.getHandle(ledgerId, key);
                            handle.setFenced();
                        } else {
                            throw new IOException("Invalid journal. Contains fenceKey "
                                    + " but layout version (" + journalVersion
                                    + ") is too old to hold this");
                        }
                    } else {
                        byte[] key = masterKeyCache.get(ledgerId);
                        if (key == null) {
                            key = ledgerStorage.readMasterKey(ledgerId);
                        }
                        LedgerDescriptor handle = handles.getHandle(ledgerId, key);

                        recBuff.rewind();
                        handle.addEntry(Unpooled.wrappedBuffer(recBuff));
                    }
                } catch (NoLedgerException nsle) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Skip replaying entries of ledger {} since it was deleted.", ledgerId);
                    }
                } catch (BookieException be) {
                    throw new IOException(be);
                }
            }
        };

        for (Journal journal : journals) {
            journal.replay(scanner);
        }
        long elapsedTs = MathUtils.now() - startTs;
        LOG.info("Finished replaying journal in {} ms.", elapsedTs);
    }

    @Override
    public synchronized void start() {
        setDaemon(true);
        if (LOG.isDebugEnabled()) {
            LOG.debug("I'm starting a bookie with journal directories {}",
                    journalDirectories.stream().map(File::getName).collect(Collectors.joining(", ")));
        }
        //Start DiskChecker thread
        ledgerMonitor.start();
        if (indexDirsManager != ledgerDirsManager) {
            idxMonitor.start();
        }

        // replay journals
        try {
            readJournal();
        } catch (IOException ioe) {
            LOG.error("Exception while replaying journals, shutting down", ioe);
            shutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        } catch (BookieException be) {
            LOG.error("Exception while replaying journals, shutting down", be);
            shutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        }

        // Do a fully flush after journal replay
        try {
            syncThread.requestFlush().get();
        } catch (InterruptedException e) {
            LOG.warn("Interrupting the fully flush after replaying journals : ", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.error("Error on executing a fully flush after replaying journals.");
            shutdown(ExitCode.BOOKIE_EXCEPTION);
        }
        LOG.info("Finished reading journal, starting bookie");

        // start bookie thread
        super.start();

        // After successful bookie startup, register listener for disk
        // error/full notifications.
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        if (indexDirsManager != ledgerDirsManager) {
            indexDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        }

        ledgerStorage.start();

        // check the bookie status to start with
        if (forceReadOnly.get()) {
            this.bookieStatus.setToReadOnlyMode();
        } else if (conf.isPersistBookieStatusEnabled()) {
            this.bookieStatus.readFromDirectories(ledgerDirsManager.getAllLedgerDirs());
        }

        // set running here.
        // since bookie server use running as a flag to tell bookie server whether it is alive
        // if setting it in bookie thread, the watcher might run before bookie thread.
        running = true;
        try {
            registerBookie(true).get();
        } catch (Exception e) {
            LOG.error("Couldn't register bookie with zookeeper, shutting down : ", e);
            shutdown(ExitCode.ZK_REG_FAIL);
        }
    }

    /*
     * Get the DiskFailure listener for the bookie
     */
    private LedgerDirsListener getLedgerDirsListener() {

        return new LedgerDirsListener() {

            @Override
            public void diskFull(File disk) {
                // Nothing needs to be handled here.
            }

            @Override
            public void diskAlmostFull(File disk) {
                // Nothing needs to be handled here.
            }

            @Override
            public void diskFailed(File disk) {
                // Shutdown the bookie on disk failure.
                triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            }

            @Override
            public void allDisksFull() {
                // Transition to readOnly mode on all disks full
                transitionToReadOnlyMode();
            }

            @Override
            public void fatalError() {
                LOG.error("Fatal error reported by ledgerDirsManager");
                triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            }

            @Override
            public void diskWritable(File disk) {
                // Transition to writable mode when a disk becomes writable again.
                transitionToWritableMode();
            }

            @Override
            public void diskJustWritable(File disk) {
                // Transition to writable mode when a disk becomes writable again.
                transitionToWritableMode();
            }
        };
    }

    /**
     * Instantiate the registration manager for the Bookie.
     */
    private RegistrationManager instantiateRegistrationManager(ServerConfiguration conf) throws BookieException {
        // Create the registration manager instance
        Class<? extends RegistrationManager> managerCls;
        try {
            managerCls = conf.getRegistrationManagerClass();
        } catch (ConfigurationException e) {
            throw new BookieIllegalOpException(e);
        }

        RegistrationManager manager = ReflectionUtils.newInstance(managerCls);
        return manager.initialize(conf, () -> {
            rmRegistered.set(false);
            // schedule a re-register operation
            registerBookie(false);
        }, statsLogger);
    }

    /**
     * Register as an available bookie.
     */
    protected Future<Void> registerBookie(final boolean throwException) {
        return stateService.submit(new Callable<Void>() {
            @Override
            public Void call() throws IOException {
                try {
                    doRegisterBookie();
                } catch (IOException ioe) {
                    if (throwException) {
                        throw ioe;
                    } else {
                        LOG.error("Couldn't register bookie with zookeeper, shutting down : ", ioe);
                        triggerBookieShutdown(ExitCode.ZK_REG_FAIL);
                    }
                }
                return (Void) null;
            }
        });
    }

    protected void doRegisterBookie() throws IOException {
        doRegisterBookie(forceReadOnly.get() || bookieStatus.isInReadOnlyMode());
    }

    private void doRegisterBookie(boolean isReadOnly) throws IOException {
        if (null == registrationManager || ((ZKRegistrationManager) this.registrationManager).getZk() == null) {
            // registration manager is null, means not register itself to zk.
            // ZooKeeper is null existing only for testing.
            LOG.info("null zk while do register");
            return;
        }

        rmRegistered.set(false);
        try {
            registrationManager.registerBookie(bookieId, isReadOnly);
            rmRegistered.set(true);
        } catch (BookieException e) {
            throw new IOException(e);
        }
    }


    /**
     * Transition the bookie from readOnly mode to writable.
     */
    private Future<Void> transitionToWritableMode() {
        return stateService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                doTransitionToWritableMode();
                return null;
            }
        });
    }

    @VisibleForTesting
    public void doTransitionToWritableMode() {
        if (shuttingdown || forceReadOnly.get()) {
            return;
        }

        if (!bookieStatus.setToWritableMode()) {
            // do nothing if already in writable mode
            return;
        }
        LOG.info("Transitioning Bookie to Writable mode and will serve read/write requests.");
        if (conf.isPersistBookieStatusEnabled()) {
            bookieStatus.writeToDirectories(ledgerDirsManager.getAllLedgerDirs());
        }
        // change zookeeper state only when using zookeeper
        if (null == registrationManager) {
            return;
        }
        try {
            doRegisterBookie(false);
        } catch (IOException e) {
            LOG.warn("Error in transitioning back to writable mode : ", e);
            transitionToReadOnlyMode();
            return;
        }
        // clear the readonly state
        try {
            registrationManager.unregisterBookie(bookieId, true);
        } catch (BookieException e) {
            // if we failed when deleting the readonly flag in zookeeper, it is OK since client would
            // already see the bookie in writable list. so just log the exception
            LOG.warn("Failed to delete bookie readonly state in zookeeper : ", e);
            return;
        }
    }

    /**
     * Transition the bookie to readOnly mode.
     */
    private Future<Void> transitionToReadOnlyMode() {
        return stateService.submit(new Callable<Void>() {
            @Override
            public Void call() {
                doTransitionToReadOnlyMode();
                return (Void) null;
            }
        });
    }

    @VisibleForTesting
    public void doTransitionToReadOnlyMode() {
        if (shuttingdown) {
            return;
        }
        if (!bookieStatus.setToReadOnlyMode()) {
            return;
        }
        if (!conf.isReadOnlyModeEnabled()) {
            LOG.warn("ReadOnly mode is not enabled. "
                    + "Can be enabled by configuring "
                    + "'readOnlyModeEnabled=true' in configuration."
                    + "Shutting down bookie");
            triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        }
        LOG.info("Transitioning Bookie to ReadOnly mode,"
                + " and will serve only read requests from clients!");
        // persist the bookie status if we enable this
        if (conf.isPersistBookieStatusEnabled()) {
            this.bookieStatus.writeToDirectories(ledgerDirsManager.getAllLedgerDirs());
        }
        // change zookeeper state only when using zookeeper
        if (null == registrationManager) {
            return;
        }
        try {
            registrationManager.registerBookie(bookieId, true);
        } catch (BookieException e) {
            LOG.error("Error in transition to ReadOnly Mode."
                    + " Shutting down", e);
            triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        }
    }

    /*
     * Check whether Bookie is writable
     */
    public boolean isReadOnly() {
        return forceReadOnly.get() || bookieStatus.isInReadOnlyMode();
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void run() {
        // bookie thread wait for journal thread
        try {
            // start journals
            for (Journal journal: journals) {
                journal.start();
            }

            // wait until journal quits
            for (Journal journal: journals) {

                journal.join();
            }
            LOG.info("Journal thread(s) quit.");
        } catch (InterruptedException ie) {
            LOG.warn("Interrupted on running journal thread : ", ie);
        }
        // if the journal thread quits due to shutting down, it is ok
        if (!shuttingdown) {
            // some error found in journal thread and it quits
            // following add operations to it would hang unit client timeout
            // so we should let bookie server exists
            LOG.error("Journal manager quits unexpectedly.");
            triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
        }
    }

    // Triggering the Bookie shutdown in its own thread,
    // because shutdown can be called from sync thread which would be
    // interrupted by shutdown call.
    AtomicBoolean shutdownTriggered = new AtomicBoolean(false);
    void triggerBookieShutdown(final int exitCode) {
        if (!shutdownTriggered.compareAndSet(false, true)) {
            return;
        }
        LOG.info("Triggering shutdown of Bookie-{} with exitCode {}",
                 conf.getBookiePort(), exitCode);
        BookieThread th = new BookieThread("BookieShutdownTrigger") {
            @Override
            public void run() {
                Bookie.this.shutdown(exitCode);
            }
        };
        th.start();
    }

    // provided a public shutdown method for other caller
    // to shut down bookie gracefully
    public int shutdown() {
        return shutdown(ExitCode.OK);
    }

    // internal shutdown method to let shutdown bookie gracefully
    // when encountering exception
    synchronized int shutdown(int exitCode) {
        try {
            if (running) { // avoid shutdown twice
                // the exitCode only set when first shutdown usually due to exception found
                LOG.info("Shutting down Bookie-{} with exitCode {}",
                         conf.getBookiePort(), exitCode);
                if (this.exitCode == ExitCode.OK) {
                    this.exitCode = exitCode;
                }
                // mark bookie as in shutting down progress
                shuttingdown = true;

                // turn bookie to read only during shutting down process
                LOG.info("Turning bookie to read only during shut down");
                this.forceReadOnly.set(true);

                // Shutdown Sync thread
                syncThread.shutdown();

                // Shutdown journals
                for (Journal journal : journals) {
                    journal.shutdown();
                }
                this.join();

                // Shutdown the EntryLogger which has the GarbageCollector Thread running
                ledgerStorage.shutdown();

                // close Ledger Manager
                try {
                    if (null != ledgerManager) {
                        ledgerManager.close();
                    }
                    if (null != ledgerManagerFactory) {
                        ledgerManagerFactory.uninitialize();
                    }
                } catch (IOException ie) {
                    LOG.error("Failed to close active ledger manager : ", ie);
                }

                //Shutdown disk checker
                ledgerMonitor.shutdown();
                if (indexDirsManager != ledgerDirsManager) {
                    idxMonitor.shutdown();
                }

                // Shutdown the state service
                stateService.shutdown();
            }
            // Shutdown the ZK client
            if (registrationManager != null) {
                registrationManager.close();
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted during shutting down bookie : ", ie);
        } finally {
            // setting running to false here, so watch thread
            // in bookie server know it only after bookie shut down
            running = false;
        }
        return this.exitCode;
    }

    /**
     * Retrieve the ledger descriptor for the ledger which entry should be added to.
     * The LedgerDescriptor returned from this method should be eventually freed with
     * #putHandle().
     *
     * @throws BookieException if masterKey does not match the master key of the ledger
     */
    private LedgerDescriptor getLedgerForEntry(ByteBuf entry, final byte[] masterKey)
            throws IOException, BookieException {
        final long ledgerId = entry.getLong(entry.readerIndex());

        LedgerDescriptor l = handles.getHandle(ledgerId, masterKey);
        if (masterKeyCache.get(ledgerId) == null) {
            // Force the load into masterKey cache
            byte[] oldValue = masterKeyCache.putIfAbsent(ledgerId, masterKey);
            if (oldValue == null) {
                // new handle, we should add the key to journal ensure we can rebuild
                ByteBuffer bb = ByteBuffer.allocate(8 + 8 + 4 + masterKey.length);
                bb.putLong(ledgerId);
                bb.putLong(METAENTRY_ID_LEDGER_KEY);
                bb.putInt(masterKey.length);
                bb.put(masterKey);
                bb.flip();

                getJournal(ledgerId).logAddEntry(bb, new NopWriteCallback(), null);
            }
        }

        return l;
    }

    private Journal getJournal(long ledgerId) {
        return journals.get(MathUtils.signSafeMod(ledgerId, journals.size()));
    }

    /**
     * Add an entry to a ledger as specified by handle.
     */
    private void addEntryInternal(LedgerDescriptor handle, ByteBuf entry, WriteCallback cb, Object ctx)
            throws IOException, BookieException {
        long ledgerId = handle.getLedgerId();
        long entryId = handle.addEntry(entry);

        writeBytes.add(entry.readableBytes());

        if (LOG.isTraceEnabled()) {
            LOG.trace("Adding {}@{}", entryId, ledgerId);
        }
        getJournal(ledgerId).logAddEntry(entry, cb, ctx);
    }

    /**
     * Add entry to a ledger, even if the ledger has previous been fenced. This should only
     * happen in bookie recovery or ledger recovery cases, where entries are being replicates
     * so that they exist on a quorum of bookies. The corresponding client side call for this
     * is not exposed to users.
     */
    public void recoveryAddEntry(ByteBuf entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        long requestNanos = MathUtils.nowInNano();
        boolean success = false;
        int entrySize = 0;
        try {
            LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);
            synchronized (handle) {
                entrySize = entry.readableBytes();
                addEntryInternal(handle, entry, cb, ctx);
            }
            success = true;
        } catch (NoWritableLedgerDirException e) {
            transitionToReadOnlyMode();
            throw new IOException(e);
        } finally {
            long elapsedNanos = MathUtils.elapsedNanos(requestNanos);
            if (success) {
                recoveryAddEntryStats.registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                addBytesStats.registerSuccessfulValue(entrySize);
            } else {
                recoveryAddEntryStats.registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                addBytesStats.registerFailedValue(entrySize);
            }

            entry.release();
        }
    }

    public void setExplicitLac(ByteBuf entry, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        try {
            long ledgerId = entry.getLong(entry.readerIndex());
            LedgerDescriptor handle = handles.getHandle(ledgerId, masterKey);
            synchronized (handle) {
                handle.setExplicitLac(entry);
            }
        } catch (NoWritableLedgerDirException e) {
            transitionToReadOnlyMode();
            throw new IOException(e);
        }
    }

    public ByteBuf getExplicitLac(long ledgerId) throws IOException, Bookie.NoLedgerException {
        ByteBuf lac;
        LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
        synchronized (handle) {
            lac = handle.getExplicitLac();
        }
        return lac;
    }

    /**
     * Add entry to a ledger.
     * @throws BookieException.LedgerFencedException if the ledger is fenced
     */
    public void addEntry(ByteBuf entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException.LedgerFencedException, BookieException {
        long requestNanos = MathUtils.nowInNano();
        boolean success = false;
        int entrySize = 0;
        try {
            LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);
            synchronized (handle) {
                if (handle.isFenced()) {
                    throw BookieException
                            .create(BookieException.Code.LedgerFencedException);
                }
                entrySize = entry.readableBytes();
                addEntryInternal(handle, entry, cb, ctx);
            }
            success = true;
        } catch (NoWritableLedgerDirException e) {
            transitionToReadOnlyMode();
            throw new IOException(e);
        } finally {
            long elapsedNanos = MathUtils.elapsedNanos(requestNanos);
            if (success) {
                addEntryStats.registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                addBytesStats.registerSuccessfulValue(entrySize);
            } else {
                addEntryStats.registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                addBytesStats.registerFailedValue(entrySize);
            }

            entry.release();
        }
    }

    static class FutureWriteCallback implements WriteCallback {

        SettableFuture<Boolean> result = SettableFuture.create();

        @Override
        public void writeComplete(int rc, long ledgerId, long entryId,
                                  BookieSocketAddress addr, Object ctx) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Finished writing entry {} @ ledger {} for {} : {}",
                        entryId, ledgerId, addr, rc);
            }

            result.set(0 == rc);
        }

        public SettableFuture<Boolean> getResult() {
            return result;
        }
    }

    /**
     * Fences a ledger. From this point on, clients will be unable to
     * write to this ledger. Only recoveryAddEntry will be
     * able to add entries to the ledger.
     * This method is idempotent. Once a ledger is fenced, it can
     * never be unfenced. Fencing a fenced ledger has no effect.
     */
    public SettableFuture<Boolean> fenceLedger(long ledgerId, byte[] masterKey) throws IOException, BookieException {
        LedgerDescriptor handle = handles.getHandle(ledgerId, masterKey);
        return handle.fenceAndLogInJournal(getJournal(ledgerId));
    }

    public ByteBuf readEntry(long ledgerId, long entryId)
            throws IOException, NoLedgerException {
        long requestNanos = MathUtils.nowInNano();
        boolean success = false;
        int entrySize = 0;
        try {
            LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Reading {}@{}", entryId, ledgerId);
            }
            ByteBuf entry = handle.readEntry(entryId);
            readBytes.add(entry.readableBytes());
            success = true;
            return entry;
        } finally {
            long elapsedNanos = MathUtils.elapsedNanos(requestNanos);
            if (success) {
                readEntryStats.registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                readBytesStats.registerSuccessfulValue(entrySize);
            } else {
                readEntryStats.registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                readBytesStats.registerFailedValue(entrySize);
            }
        }
    }

    public long readLastAddConfirmed(long ledgerId) throws IOException {
        LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
        return handle.getLastAddConfirmed();
    }

    public Observable waitForLastAddConfirmedUpdate(long ledgerId, long previoisLAC, Observer observer)
            throws IOException {
        LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
        return handle.waitForLastAddConfirmedUpdate(previoisLAC, observer);
    }

    // The rest of the code is test stuff
    static class CounterCallback implements WriteCallback {
        int count;

        @Override
        public synchronized void writeComplete(int rc, long l, long e, BookieSocketAddress addr, Object ctx) {
            count--;
            if (count == 0) {
                notifyAll();
            }
        }

        public synchronized void incCount() {
            count++;
        }

        public synchronized void waitZero() throws InterruptedException {
            while (count > 0) {
                wait();
            }
        }
    }

    /**
     * Format the bookie server data.
     *
     * @param conf ServerConfiguration
     * @param isInteractive Whether format should ask prompt for confirmation if old data exists or not.
     * @param force If non interactive and force is true, then old data will be removed without confirm prompt.
     * @return Returns true if the format is success else returns false
     */
    public static boolean format(ServerConfiguration conf,
            boolean isInteractive, boolean force) {
        for (File journalDir : conf.getJournalDirs()) {
            String[] journalDirFiles =
                    journalDir.exists() && journalDir.isDirectory() ? journalDir.list() : null;
            if (journalDirFiles != null && journalDirFiles.length != 0) {
                try {
                    boolean confirm = false;
                    if (!isInteractive) {
                        // If non interactive and force is set, then delete old
                        // data.
                        if (force) {
                            confirm = true;
                        } else {
                            confirm = false;
                        }
                    } else {
                        confirm = IOUtils
                                .confirmPrompt("Are you sure to format Bookie data..?");
                    }

                    if (!confirm) {
                        LOG.error("Bookie format aborted!!");
                        return false;
                    }
                } catch (IOException e) {
                    LOG.error("Error during bookie format", e);
                    return false;
                }
            }
            if (!cleanDir(journalDir)) {
                LOG.error("Formatting journal directory failed");
                return false;
            }

            File[] ledgerDirs = conf.getLedgerDirs();
            for (File dir : ledgerDirs) {
                if (!cleanDir(dir)) {
                    LOG.error("Formatting ledger directory " + dir + " failed");
                    return false;
                }
            }

            // Clean up index directories if they are separate from the ledger dirs
            File[] indexDirs = conf.getIndexDirs();
            if (null != indexDirs) {
                for (File dir : indexDirs) {
                    if (!cleanDir(dir)) {
                        LOG.error("Formatting ledger directory " + dir + " failed");
                        return false;
                    }
                }
            }
        }

        LOG.info("Bookie format completed successfully");
        return true;
    }

    private static boolean cleanDir(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File child : files) {
                    boolean delete = FileUtils.deleteQuietly(child);
                    if (!delete) {
                        LOG.error("Not able to delete " + child);
                        return false;
                    }
                }
            }
        } else if (!dir.mkdirs()) {
            LOG.error("Not able to create the directory " + dir);
            return false;
        }
        return true;
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args)
            throws IOException, InterruptedException, BookieException, KeeperException {
        Bookie b = new Bookie(new ServerConfiguration());
        b.start();
        CounterCallback cb = new CounterCallback();
        long start = MathUtils.now();
        for (int i = 0; i < 100000; i++) {
            ByteBuf buff = Unpooled.buffer(1024);
            buff.writeLong(1);
            buff.writeLong(i);
            cb.incCount();
            b.addEntry(buff, cb, null, new byte[0]);
        }
        cb.waitZero();
        long end = MathUtils.now();
        System.out.println("Took " + (end - start) + "ms");
    }

    /**
     * Returns exit code - cause of failure.
     *
     * @return {@link ExitCode}
     */
    public int getExitCode() {
        return exitCode;
    }

}
