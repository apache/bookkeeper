/*
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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.bookkeeper.bookie.BookieException.DiskPartitionDuplicationException;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.bookie.stats.BookieStats;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.BookieRequestHandler;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.ThreadRegistry;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a bookie.
 */
public class BookieImpl extends BookieCriticalThread implements Bookie {

    private static final Logger LOG = LoggerFactory.getLogger(Bookie.class);

    final List<File> journalDirectories;
    final ServerConfiguration conf;

    final SyncThread syncThread;
    final LedgerStorage ledgerStorage;
    final RegistrationManager registrationManager;
    final List<Journal> journals;

    final HandleFactory handles;
    final boolean entryLogPerLedgerEnabled;

    public static final long METAENTRY_ID_LEDGER_KEY = -0x1000;
    public static final long METAENTRY_ID_FENCE_KEY  = -0x2000;
    public static final long METAENTRY_ID_FORCE_LEDGER  = -0x4000;
    static final long METAENTRY_ID_LEDGER_EXPLICITLAC  = -0x8000;

    private final LedgerDirsManager ledgerDirsManager;
    protected final Supplier<BookieServiceInfo> bookieServiceInfoProvider;
    private final LedgerDirsManager indexDirsManager;
    LedgerDirsMonitor dirsMonitor;

    private int exitCode = ExitCode.OK;

    private final ConcurrentLongHashMap<byte[]> masterKeyCache =
            ConcurrentLongHashMap.<byte[]>newBuilder().autoShrink(true).build();

    protected StateManager stateManager;

    // Expose Stats
    final StatsLogger statsLogger;
    private final BookieStats bookieStats;

    private final ByteBufAllocator allocator;

    private final boolean writeDataToJournal;

    // Write Callback do nothing
    static class NopWriteCallback implements WriteCallback {
        @Override
        public void writeComplete(int rc, long ledgerId, long entryId,
                                  BookieId addr, Object ctx) {
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

    /**
     * Check that the environment for the bookie is correct.
     * This means that the configuration has stayed the same as the
     * first run and the filesystem structure is up to date.
     */
    private void checkEnvironment()
            throws BookieException, IOException, InterruptedException {
        List<File> allLedgerDirs = new ArrayList<File>(ledgerDirsManager.getAllLedgerDirs().size()
                + indexDirsManager.getAllLedgerDirs().size());
        allLedgerDirs.addAll(ledgerDirsManager.getAllLedgerDirs());
        if (indexDirsManager != ledgerDirsManager) {
            allLedgerDirs.addAll(indexDirsManager.getAllLedgerDirs());
        }

        for (File journalDirectory : journalDirectories) {
            checkDirectoryStructure(journalDirectory);
        }

        for (File dir : allLedgerDirs) {
            checkDirectoryStructure(dir);
        }

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

    public static BookieId getBookieId(ServerConfiguration conf) throws UnknownHostException {
        String customBookieId = conf.getBookieId();
        if (customBookieId != null) {
            return BookieId.parse(customBookieId);
        }
        return getBookieAddress(conf).toBookieId();
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
        String hostAddress = null;
        InetAddress iAddress = inetAddr.getAddress();
        if (conf.getUseHostNameAsBookieID()) {
            hostAddress = iAddress.getCanonicalHostName();
            if (conf.getUseShortHostName()) {
                /*
                 * if short hostname is used, then FQDN is not used. Short
                 * hostname is the hostname cut at the first dot.
                 */
                hostAddress = hostAddress.split("\\.", 2)[0];
            }
        } else {
            hostAddress = iAddress.getHostAddress();
        }

        BookieSocketAddress addr =
                new BookieSocketAddress(hostAddress, conf.getBookiePort());
        if (addr.getSocketAddress().getAddress().isLoopbackAddress()
            && !conf.getAllowLoopback()) {
            throw new UnknownHostException("Trying to listen on loopback address, "
                    + addr + " but this is forbidden by default "
                    + "(see ServerConfiguration#getAllowLoopback()).\n"
                    + "If this happen, you can consider specifying the network interface"
                    + " to listen on (e.g. listeningInterface=eth0) or specifying the"
                    + " advertised address (e.g. advertisedAddress=172.x.y.z)");
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

    /**
     * Initialize LedgerStorage instance without checkpointing for use within the shell
     * and other RO users.  ledgerStorage must not have already been initialized.
     *
     * <p>The caller is responsible for disposing of the ledgerStorage object.
     *
     * @param conf Bookie config.
     * @param ledgerStorage Instance to initialize.
     * @return Passed ledgerStorage instance
     * @throws IOException
     */
    public static LedgerStorage mountLedgerStorageOffline(ServerConfiguration conf, LedgerStorage ledgerStorage)
            throws IOException {
        StatsLogger statsLogger = NullStatsLogger.INSTANCE;
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());

        LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
                conf, diskChecker, statsLogger.scope(LD_LEDGER_SCOPE));
        LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(
                conf, diskChecker, statsLogger.scope(LD_INDEX_SCOPE), ledgerDirsManager);

        if (null == ledgerStorage) {
            ledgerStorage = BookieResources.createLedgerStorage(conf, null,
                                                                ledgerDirsManager,
                                                                indexDirsManager,
                                                                statsLogger,
                                                                UnpooledByteBufAllocator.DEFAULT);
        } else {
            ledgerStorage.initialize(
                conf,
                null,
                ledgerDirsManager,
                indexDirsManager,
                statsLogger,
                UnpooledByteBufAllocator.DEFAULT);
        }

        ledgerStorage.setCheckpointSource(new CheckpointSource() {
                @Override
                public Checkpoint newCheckpoint() {
                    return Checkpoint.MIN;
                }

                @Override
                public void checkpointComplete(Checkpoint checkpoint, boolean compact)
                        throws IOException {
                }
            });
        ledgerStorage.setCheckpointer(Checkpointer.NULL);
        return ledgerStorage;
    }

    public BookieImpl(ServerConfiguration conf,
                      RegistrationManager registrationManager,
                      LedgerStorage storage,
                      DiskChecker diskChecker,
                      LedgerDirsManager ledgerDirsManager,
                      LedgerDirsManager indexDirsManager,
                      StatsLogger statsLogger,
                      ByteBufAllocator allocator,
                      Supplier<BookieServiceInfo> bookieServiceInfoProvider)
            throws IOException, InterruptedException, BookieException {
        super("Bookie-" + conf.getBookiePort());
        this.bookieServiceInfoProvider = bookieServiceInfoProvider;
        this.statsLogger = statsLogger;
        this.conf = conf;
        this.journalDirectories = Lists.newArrayList();
        for (File journalDirectory : conf.getJournalDirs()) {
            this.journalDirectories.add(getCurrentDirectory(journalDirectory));
        }
        this.ledgerDirsManager = ledgerDirsManager;
        this.indexDirsManager = indexDirsManager;
        this.writeDataToJournal = conf.getJournalWriteData();
        this.allocator = allocator;
        this.registrationManager = registrationManager;
        stateManager = initializeStateManager();
        checkEnvironment();

        // register shutdown handler using trigger mode
        stateManager.setShutdownHandler(exitCode -> triggerBookieShutdown(exitCode));
        // Initialise dirsMonitor. This would look through all the
        // configured directories. When disk errors or all the ledger
        // directories are full, would throws exception and fail bookie startup.
        List<LedgerDirsManager> dirsManagers = new ArrayList<>();
        dirsManagers.add(ledgerDirsManager);
        if (indexDirsManager != ledgerDirsManager) {
            dirsManagers.add(indexDirsManager);
        }
        this.dirsMonitor = new LedgerDirsMonitor(conf, diskChecker, dirsManagers);
        try {
            this.dirsMonitor.init();
        } catch (NoWritableLedgerDirException nle) {
            // start in read-only mode if no writable dirs and read-only allowed
            if (!conf.isReadOnlyModeEnabled()) {
                throw nle;
            } else {
                this.stateManager.transitionToReadOnlyMode();
            }
        }

        JournalAliveListener journalAliveListener =
                () -> BookieImpl.this.triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
        // instantiate the journals
        journals = Lists.newArrayList();
        for (int i = 0; i < journalDirectories.size(); i++) {
            journals.add(new Journal(i, journalDirectories.get(i),
                    conf, ledgerDirsManager, statsLogger.scope(JOURNAL_SCOPE), allocator, journalAliveListener));
        }

        this.entryLogPerLedgerEnabled = conf.isEntryLogPerLedgerEnabled();
        CheckpointSource checkpointSource = new CheckpointSourceList(journals);

        this.ledgerStorage = storage;
        boolean isDbLedgerStorage = ledgerStorage instanceof DbLedgerStorage;

        /*
         * with this change https://github.com/apache/bookkeeper/pull/677,
         * LedgerStorage drives the checkpoint logic.
         *
         * <p>There are two exceptions:
         *
         * 1) with multiple entry logs, checkpoint logic based on a entry log is
         *    not possible, hence it needs to be timebased recurring thing and
         *    it is driven by SyncThread. SyncThread.start does that and it is
         *    started in Bookie.start method.
         *
         * 2) DbLedgerStorage
         */
        if (entryLogPerLedgerEnabled || isDbLedgerStorage) {
            syncThread = new SyncThread(conf, getLedgerDirsListener(), ledgerStorage, checkpointSource, statsLogger) {
                @Override
                public void startCheckpoint(Checkpoint checkpoint) {
                    /*
                     * in the case of entryLogPerLedgerEnabled, LedgerStorage
                     * dont drive checkpoint logic, but instead it is done
                     * periodically by SyncThread. So startCheckpoint which
                     * will be called by LedgerStorage will be no-op.
                     */
                }

                @Override
                public void start() {
                    executor.scheduleAtFixedRate(() -> {
                        doCheckpoint(checkpointSource.newCheckpoint());
                    }, conf.getFlushInterval(), conf.getFlushInterval(), TimeUnit.MILLISECONDS);
                }
            };
        } else {
            syncThread = new SyncThread(conf, getLedgerDirsListener(), ledgerStorage, checkpointSource, statsLogger);
        }

        LedgerStorage.LedgerDeletionListener ledgerDeletionListener = new LedgerStorage.LedgerDeletionListener() {
            @Override
            public void ledgerDeleted(long ledgerId) {
                masterKeyCache.remove(ledgerId);
            }
        };

        ledgerStorage.setStateManager(stateManager);
        ledgerStorage.setCheckpointSource(checkpointSource);
        ledgerStorage.setCheckpointer(syncThread);
        ledgerStorage.registerLedgerDeletionListener(ledgerDeletionListener);
        handles = new HandleFactoryImpl(ledgerStorage);

        // Expose Stats
        this.bookieStats = new BookieStats(statsLogger, journalDirectories.size(), conf.getJournalQueueSize());
    }

    StateManager initializeStateManager() throws IOException {
        return new BookieStateManager(conf, statsLogger, registrationManager,
                ledgerDirsManager, bookieServiceInfoProvider);
    }

    void readJournal() throws IOException, BookieException {
        if (!conf.getJournalWriteData()) {
            LOG.warn("Journal disabled for add entry requests. Running BookKeeper this way can "
                    + "lead to data loss. It is recommended to use data integrity checking when "
                    + "running without the journal to minimize data loss risk");
        }

        long startTs = System.currentTimeMillis();
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

                            // Force to re-insert the master key in ledger storage
                            handles.getHandle(ledgerId, masterKey);
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
                    } else if (entryId == METAENTRY_ID_LEDGER_EXPLICITLAC) {
                        if (journalVersion >= JournalChannel.V6) {
                            int explicitLacBufLength = recBuff.getInt();
                            ByteBuf explicitLacBuf = Unpooled.buffer(explicitLacBufLength);
                            byte[] explicitLacBufArray = new byte[explicitLacBufLength];
                            recBuff.get(explicitLacBufArray);
                            explicitLacBuf.writeBytes(explicitLacBufArray);
                            byte[] key = masterKeyCache.get(ledgerId);
                            if (key == null) {
                                key = ledgerStorage.readMasterKey(ledgerId);
                            }
                            LedgerDescriptor handle = handles.getHandle(ledgerId, key);
                            handle.setExplicitLac(explicitLacBuf);
                        } else {
                            throw new IOException("Invalid journal. Contains explicitLAC " + " but layout version ("
                                    + journalVersion + ") is too old to hold this");
                        }
                    } else if (entryId < 0) {
                        /*
                         * this is possible if bookie code binary is rolledback
                         * to older version but when it is trying to read
                         * Journal which was created previously using newer
                         * code/journalversion, which introduced new special
                         * entry. So in anycase, if we see unrecognizable
                         * special entry while replaying journal we should skip
                         * (ignore) it.
                         */
                        LOG.warn("Read unrecognizable entryId: {} for ledger: {} while replaying Journal. Skipping it",
                                entryId, ledgerId);
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
            replay(journal, scanner);
        }
        long elapsedTs = System.currentTimeMillis() - startTs;
        LOG.info("Finished replaying journal in {} ms.", elapsedTs);
    }

    /**
     * Replay journal files and updates journal's in-memory lastLogMark object.
     *
     * @param journal Journal object corresponding to a journalDir
     * @param scanner Scanner to process replayed entries.
     * @throws IOException
     */
    private void replay(Journal journal, JournalScanner scanner) throws IOException {
        final LogMark markedLog = journal.getLastLogMark().getCurMark();
        List<Long> logs = Journal.listJournalIds(journal.getJournalDirectory(), journalId ->
            journalId >= markedLog.getLogFileId());
        // last log mark may be missed due to no sync up before
        // validate filtered log ids only when we have markedLogId
        if (markedLog.getLogFileId() > 0) {
            if (logs.size() == 0 || logs.get(0) != markedLog.getLogFileId()) {
                String path = journal.getJournalDirectory().getAbsolutePath();
                throw new IOException("Recovery log " + markedLog.getLogFileId() + " is missing at " + path);
            }
        }

        // TODO: When reading in the journal logs that need to be synced, we
        // should use BufferedChannels instead to minimize the amount of
        // system calls done.
        for (Long id : logs) {
            long logPosition = 0L;
            if (id == markedLog.getLogFileId()) {
                logPosition = markedLog.getLogFileOffset();
            }
            LOG.info("Replaying journal {} from position {}", id, logPosition);
            long scanOffset = journal.scanJournal(id, logPosition, scanner, conf.isSkipReplayJournalInvalidRecord());
            // Update LastLogMark after completely replaying journal
            // scanOffset will point to EOF position
            // After LedgerStorage flush, SyncThread should persist this to disk
            journal.setLastLogMark(id, scanOffset);
        }
    }

    @Override
    public synchronized void start() {
        setDaemon(true);
        ThreadRegistry.register("BookieThread", 0);
        if (LOG.isDebugEnabled()) {
            LOG.debug("I'm starting a bookie with journal directories {}",
                    journalDirectories.stream().map(File::getName).collect(Collectors.joining(", ")));
        }
        //Start DiskChecker thread
        dirsMonitor.start();

        // replay journals
        try {
            readJournal();
        } catch (IOException | BookieException ioe) {
            LOG.error("Exception while replaying journals, shutting down", ioe);
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
            return;
        }

        if (conf.isLocalConsistencyCheckOnStartup()) {
            LOG.info("Running local consistency check on startup prior to accepting IO.");
            List<LedgerStorage.DetectedInconsistency> errors = null;
            try {
                errors = ledgerStorage.localConsistencyCheck(Optional.empty());
            } catch (IOException e) {
                LOG.error("Got a fatal exception while checking store", e);
                shutdown(ExitCode.BOOKIE_EXCEPTION);
                return;
            }
            if (errors != null && errors.size() > 0) {
                LOG.error("Bookie failed local consistency check:");
                for (LedgerStorage.DetectedInconsistency error : errors) {
                    LOG.error("Ledger {}, entry {}: ", error.getLedgerId(), error.getEntryId(), error.getException());
                }
                shutdown(ExitCode.BOOKIE_EXCEPTION);
                return;
            }
        }

        LOG.info("Finished reading journal, starting bookie");


        /*
         * start sync thread first, so during replaying journals, we could do
         * checkpoint which reduce the chance that we need to replay journals
         * again if bookie restarted again before finished journal replays.
         */
        syncThread.start();

        // start bookie thread
        super.start();

        // After successful bookie startup, register listener for disk
        // error/full notifications.
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        if (indexDirsManager != ledgerDirsManager) {
            indexDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        }

        ledgerStorage.start();

        // check the bookie status to start with, and set running.
        // since bookie server use running as a flag to tell bookie server whether it is alive
        // if setting it in bookie thread, the watcher might run before bookie thread.
        stateManager.initState();

        try {
            stateManager.registerBookie(true).get();
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
            public void diskFailed(File disk) {
                // Shutdown the bookie on disk failure.
                triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            }

            @Override
            public void allDisksFull(boolean highPriorityWritesAllowed) {
                // Transition to readOnly mode on all disks full
                stateManager.setHighPriorityWritesAvailability(highPriorityWritesAllowed);
                stateManager.transitionToReadOnlyMode();
            }

            @Override
            public void fatalError() {
                LOG.error("Fatal error reported by ledgerDirsManager");
                triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            }

            @Override
            public void diskWritable(File disk) {
                if (conf.isReadOnlyModeOnAnyDiskFullEnabled()) {
                    return;
                }
                // Transition to writable mode when a disk becomes writable again.
                stateManager.setHighPriorityWritesAvailability(true);
                stateManager.transitionToWritableMode();
            }

            @Override
            public void diskJustWritable(File disk) {
                if (conf.isReadOnlyModeOnAnyDiskFullEnabled()) {
                    return;
                }
                // Transition to writable mode when a disk becomes writable again.
                stateManager.setHighPriorityWritesAvailability(true);
                stateManager.transitionToWritableMode();
            }

            @Override
            public void anyDiskFull(boolean highPriorityWritesAllowed) {
                if (conf.isReadOnlyModeOnAnyDiskFullEnabled()) {
                    stateManager.setHighPriorityWritesAvailability(highPriorityWritesAllowed);
                    stateManager.transitionToReadOnlyMode();
                }
            }

            @Override
            public void allDisksWritable() {
                // Transition to writable mode when a disk becomes writable again.
                stateManager.setHighPriorityWritesAvailability(true);
                stateManager.transitionToWritableMode();
            }
        };
    }

    /*
     * Check whether Bookie is writable.
     */
    public boolean isReadOnly() {
        return stateManager.isReadOnly();
    }

    /**
     * Check whether Bookie is available for high priority writes.
     *
     * @return true if the bookie is able to take high priority writes.
     */
    public boolean isAvailableForHighPriorityWrites() {
        return stateManager.isAvailableForHighPriorityWrites();
    }

    public boolean isRunning() {
        return stateManager.isRunning();
    }

    @Override
    public void run() {
        // start journals
        for (Journal journal: journals) {
            journal.start();
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
                BookieImpl.this.shutdown(exitCode);
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
    ReentrantLock lock = new ReentrantLock(true);
    int shutdown(int exitCode) {
        lock.lock();
        try {
            if (isRunning()) {
                // the exitCode only set when first shutdown usually due to exception found
                LOG.info("Shutting down Bookie-{} with exitCode {}",
                         conf.getBookiePort(), exitCode);
                if (this.exitCode == ExitCode.OK) {
                    this.exitCode = exitCode;
                }

                stateManager.forceToShuttingDown();

                // turn bookie to read only during shutting down process
                LOG.info("Turning bookie to read only during shut down");
                stateManager.forceToReadOnly();

                // Shutdown Sync thread
                syncThread.shutdown();

                // Shutdown journals
                for (Journal journal : journals) {
                    journal.shutdown();
                }

                // Shutdown the EntryLogger which has the GarbageCollector Thread running
                ledgerStorage.shutdown();

                //Shutdown disk checker
                dirsMonitor.shutdown();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted during shutting down bookie : ", ie);
        } catch (Exception e) {
            LOG.error("Got Exception while trying to shutdown Bookie", e);
            throw e;
        } finally {
            lock.unlock();
            // setting running to false here, so watch thread
            // in bookie server know it only after bookie shut down
            stateManager.close();
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
    @VisibleForTesting
    LedgerDescriptor getLedgerForEntry(ByteBuf entry, final byte[] masterKey)
            throws IOException, BookieException {
        final long ledgerId = entry.getLong(entry.readerIndex());

        return handles.getHandle(ledgerId, masterKey);
    }

    private Journal getJournal(long ledgerId) {
        return journals.get(MathUtils.signSafeMod(ledgerId, journals.size()));
    }

    @VisibleForTesting
    public ByteBuf createMasterKeyEntry(long ledgerId, byte[] masterKey) {
        // new handle, we should add the key to journal ensure we can rebuild
        ByteBuf bb = allocator.directBuffer(8 + 8 + 4 + masterKey.length);
        bb.writeLong(ledgerId);
        bb.writeLong(METAENTRY_ID_LEDGER_KEY);
        bb.writeInt(masterKey.length);
        bb.writeBytes(masterKey);
        return bb;
    }

    /**
     * Add an entry to a ledger as specified by handle.
     */
    private void addEntryInternal(LedgerDescriptor handle, ByteBuf entry,
                                  boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException, InterruptedException {
        long ledgerId = handle.getLedgerId();
        long entryId = handle.addEntry(entry);

        bookieStats.getWriteBytes().addCount(entry.readableBytes());

        // journal `addEntry` should happen after the entry is added to ledger storage.
        // otherwise the journal entry can potentially be rolled before the ledger is created in ledger storage.
        if (masterKeyCache.get(ledgerId) == null) {
            // Force the load into masterKey cache
            byte[] oldValue = masterKeyCache.putIfAbsent(ledgerId, masterKey);
            if (oldValue == null) {
                ByteBuf masterKeyEntry = createMasterKeyEntry(ledgerId, masterKey);
                try {
                    getJournal(ledgerId).logAddEntry(
                            masterKeyEntry, false /* ackBeforeSync */, new NopWriteCallback(), null);
                } finally {
                    ReferenceCountUtil.release(masterKeyEntry);
                }
            }
        }

        if (!writeDataToJournal) {
            cb.writeComplete(0, ledgerId, entryId, null, ctx);
            if (ctx instanceof BookieRequestHandler) {
                ((BookieRequestHandler) ctx).flushPendingResponse();
            }
            return;
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Adding {}@{}", entryId, ledgerId);
        }
        getJournal(ledgerId).logAddEntry(entry, ackBeforeSync, cb, ctx);
    }

    /**
     * Add entry to a ledger, even if the ledger has previous been fenced. This should only
     * happen in bookie recovery or ledger recovery cases, where entries are being replicates
     * so that they exist on a quorum of bookies. The corresponding client side call for this
     * is not exposed to users.
     */
    public void recoveryAddEntry(ByteBuf entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException, InterruptedException {
        long requestNanos = MathUtils.nowInNano();
        boolean success = false;
        int entrySize = 0;
        try {
            LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);
            synchronized (handle) {
                entrySize = entry.readableBytes();
                addEntryInternal(handle, entry, false /* ackBeforeSync */, cb, ctx, masterKey);
            }
            success = true;
        } catch (NoWritableLedgerDirException e) {
            stateManager.transitionToReadOnlyMode();
            throw new IOException(e);
        } finally {
            long elapsedNanos = MathUtils.elapsedNanos(requestNanos);
            if (success) {
                bookieStats.getRecoveryAddEntryStats().registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                bookieStats.getAddBytesStats().registerSuccessfulValue(entrySize);
            } else {
                bookieStats.getRecoveryAddEntryStats().registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                bookieStats.getAddBytesStats().registerFailedValue(entrySize);
            }

            ReferenceCountUtil.release(entry);
        }
    }

    @VisibleForTesting
    public ByteBuf createExplicitLACEntry(long ledgerId, ByteBuf explicitLac) {
        ByteBuf bb = allocator.directBuffer(8 + 8 + 4 + explicitLac.capacity());
        bb.writeLong(ledgerId);
        bb.writeLong(METAENTRY_ID_LEDGER_EXPLICITLAC);
        bb.writeInt(explicitLac.capacity());
        bb.writeBytes(explicitLac);
        return bb;
    }

    public void setExplicitLac(ByteBuf entry, WriteCallback writeCallback, Object ctx, byte[] masterKey)
            throws IOException, InterruptedException, BookieException {
        ByteBuf explicitLACEntry = null;
        try {
            long ledgerId = entry.getLong(entry.readerIndex());
            LedgerDescriptor handle = handles.getHandle(ledgerId, masterKey);
            synchronized (handle) {
                entry.markReaderIndex();
                handle.setExplicitLac(entry);
                entry.resetReaderIndex();
                explicitLACEntry = createExplicitLACEntry(ledgerId, entry);
                getJournal(ledgerId).logAddEntry(explicitLACEntry, false /* ackBeforeSync */, writeCallback, ctx);
            }
        } catch (NoWritableLedgerDirException e) {
            stateManager.transitionToReadOnlyMode();
            throw new IOException(e);
        } finally {
            ReferenceCountUtil.release(entry);
            if (explicitLACEntry != null) {
                ReferenceCountUtil.release(explicitLACEntry);
            }
        }
    }

    public ByteBuf getExplicitLac(long ledgerId) throws IOException, Bookie.NoLedgerException, BookieException {
        ByteBuf lac;
        LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
        synchronized (handle) {
            lac = handle.getExplicitLac();
        }
        return lac;
    }

    /**
     * Force sync given 'ledgerId' entries on the journal to the disk.
     * It works like a regular addEntry with ackBeforeSync=false.
     * This is useful for ledgers with DEFERRED_SYNC write flag.
     */
    public void forceLedger(long ledgerId, WriteCallback cb,
                            Object ctx) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Forcing ledger {}", ledgerId);
        }
        Journal journal = getJournal(ledgerId);
        journal.forceLedger(ledgerId, cb, ctx);
        bookieStats.getForceLedgerOps().inc();
    }

    /**
     * Add entry to a ledger.
     */
    public void addEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException, InterruptedException {
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
                addEntryInternal(handle, entry, ackBeforeSync, cb, ctx, masterKey);
            }
            success = true;
        } catch (NoWritableLedgerDirException e) {
            stateManager.transitionToReadOnlyMode();
            throw new IOException(e);
        } finally {
            long elapsedNanos = MathUtils.elapsedNanos(requestNanos);
            if (success) {
                bookieStats.getAddEntryStats().registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                bookieStats.getAddBytesStats().registerSuccessfulValue(entrySize);
            } else {
                bookieStats.getAddEntryStats().registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                bookieStats.getAddBytesStats().registerFailedValue(entrySize);
            }

            ReferenceCountUtil.release(entry);
        }
    }

    /**
     * Fences a ledger. From this point on, clients will be unable to
     * write to this ledger. Only recoveryAddEntry will be
     * able to add entries to the ledger.
     * This method is idempotent. Once a ledger is fenced, it can
     * never be unfenced. Fencing a fenced ledger has no effect.
     * @return
     */
    public CompletableFuture<Boolean> fenceLedger(long ledgerId, byte[] masterKey)
            throws IOException, BookieException {
        LedgerDescriptor handle = handles.getHandle(ledgerId, masterKey);
        return handle.fenceAndLogInJournal(getJournal(ledgerId));
    }

    public ByteBuf readEntry(long ledgerId, long entryId)
            throws IOException, NoLedgerException, BookieException {
        long requestNanos = MathUtils.nowInNano();
        boolean success = false;
        int entrySize = 0;
        try {
            LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Reading {}@{}", entryId, ledgerId);
            }
            ByteBuf entry = handle.readEntry(entryId);
            entrySize = entry.readableBytes();
            bookieStats.getReadBytes().addCount(entrySize);
            success = true;
            return entry;
        } finally {
            long elapsedNanos = MathUtils.elapsedNanos(requestNanos);
            if (success) {
                bookieStats.getReadEntryStats().registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                bookieStats.getReadBytesStats().registerSuccessfulValue(entrySize);
            } else {
                bookieStats.getReadEntryStats().registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                bookieStats.getReadBytesStats().registerFailedValue(entrySize);
            }
        }
    }

    public long readLastAddConfirmed(long ledgerId) throws IOException, BookieException {
        LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
        return handle.getLastAddConfirmed();
    }

    public boolean waitForLastAddConfirmedUpdate(long ledgerId,
                                                 long previousLAC,
                                                 Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
        return handle.waitForLastAddConfirmedUpdate(previousLAC, watcher);
    }

    public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                    Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
        handle.cancelWaitForLastAddConfirmedUpdate(watcher);
    }

    @VisibleForTesting
    public LedgerStorage getLedgerStorage() {
        return ledgerStorage;
    }

    @VisibleForTesting
    public BookieStateManager getStateManager() {
        return (BookieStateManager) this.stateManager;
    }

    public ByteBufAllocator getAllocator() {
        return allocator;
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
                        confirm = force;
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
                    LOG.error("Formatting index directory " + dir + " failed");
                    return false;
                }
            }
        }

        // Clean up metadata directories if they are separate from the
        // ledger dirs
        if (!Strings.isNullOrEmpty(conf.getGcEntryLogMetadataCachePath())) {
            File metadataDir = new File(conf.getGcEntryLogMetadataCachePath());
            if (!cleanDir(metadataDir)) {
                LOG.error("Formatting ledger metadata directory {} failed", metadataDir);
                return false;
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
     * Returns exit code - cause of failure.
     *
     * @return {@link ExitCode}
     */
    public int getExitCode() {
        return exitCode;
    }

    public OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException, NoLedgerException {
        long requestNanos = MathUtils.nowInNano();
        boolean success = false;
        try {
            LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
            if (LOG.isTraceEnabled()) {
                LOG.trace("GetEntriesOfLedger {}", ledgerId);
            }
            OfLong entriesOfLedger = handle.getListOfEntriesOfLedger(ledgerId);
            success = true;
            return entriesOfLedger;
        } finally {
            long elapsedNanos = MathUtils.elapsedNanos(requestNanos);
            if (success) {
                bookieStats.getReadEntryStats().registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
            } else {
                bookieStats.getReadEntryStats().registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
            }
        }
    }

    @VisibleForTesting
    public List<Journal> getJournals() {
        return this.journals;
    }
}
