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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.jmx.BKMBeanRegistry;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * Implements a bookie.
 *
 */

public class Bookie extends Thread {
    HashMap<Long, LedgerDescriptor> ledgers = new HashMap<Long, LedgerDescriptor>();
    static Logger LOG = LoggerFactory.getLogger(Bookie.class);
    final static long MB = 1024 * 1024L;
    // max journal file size
    final long maxJournalSize;
    // number journal files kept before marked journal
    final int maxBackupJournals;

    final File journalDirectory;

    final File ledgerDirectories[];

    final ServerConfiguration conf;

    final SyncThread syncThread;
    final LedgerManager ledgerManager;

    /**
     * Current directory layout version. Increment this 
     * when you make a change to the format of any of the files in 
     * this directory or to the general layout of the directory.
     */
    static final int CURRENT_DIRECTORY_LAYOUT_VERSION = 1;
    static final String VERSION_FILENAME = "VERSION";
    
    // ZK registration path for this bookie
    static final String BOOKIE_REGISTRATION_PATH = "/ledgers/available/";

    // ZooKeeper client instance for the Bookie
    ZooKeeper zk;
    private volatile boolean isZkExpired = true;

    // Running flag
    private volatile boolean running = false;

    // jmx related beans
    BookieBean jmxBookieBean;
    LedgerCacheBean jmxLedgerCacheBean;

    public static class NoLedgerException extends IOException {
        private static final long serialVersionUID = 1L;
        private long ledgerId;
        public NoLedgerException(long ledgerId) {
            this.ledgerId = ledgerId;
        }
        public long getLedgerId() {
            return ledgerId;
        }
    }
    public static class NoEntryException extends IOException {
        private static final long serialVersionUID = 1L;
        private long ledgerId;
        private long entryId;
        public NoEntryException(long ledgerId, long entryId) {
            super("Entry " + entryId + " not found in " + ledgerId);
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

    EntryLogger entryLogger;
    LedgerCache ledgerCache;
    /**
     * SyncThread is a background thread which flushes ledger index pages periodically.
     * Also it takes responsibility of garbage collecting journal files.
     *
     * <p>
     * Before flushing, SyncThread first records a log marker {journalId, journalPos} in memory,
     * which indicates entries before this log marker would be persisted to ledger files.
     * Then sync thread begins flushing ledger index pages to ledger index files, flush entry
     * logger to ensure all entries persisted to entry loggers for future reads.
     * </p>
     * <p>
     * After all data has been persisted to ledger index files and entry loggers, it is safe
     * to persist the log marker to disk. If bookie failed after persist log mark,
     * bookie is able to relay journal entries started from last log mark without losing
     * any entries.
     * </p>
     * <p>
     * Those journal files whose id are less than the log id in last log mark, could be
     * removed safely after persisting last log mark. We provide a setting to let user keeping
     * number of old journal files which may be used for manual recovery in critical disaster.
     * </p>
     */
    class SyncThread extends Thread {
        volatile boolean running = true;
        // flag to ensure sync thread will not be interrupted during flush
        final AtomicBoolean flushing = new AtomicBoolean(false);
        // make flush interval as a parameter
        final int flushInterval;
        public SyncThread(ServerConfiguration conf) {
            super("SyncThread");
            flushInterval = conf.getFlushInterval();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Flush Interval : " + flushInterval);
            }
        }
        @Override
        public void run() {
            while(running) {
                synchronized(this) {
                    try {
                        wait(flushInterval);
                        if (!entryLogger.testAndClearSomethingWritten()) {
                            continue;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        continue;
                    }
                }

                // try to mark flushing flag to make sure it would not be interrupted
                // by shutdown during flushing. otherwise it will receive
                // ClosedByInterruptException which may cause index file & entry logger
                // closed and corrupted.
                if (!flushing.compareAndSet(false, true)) {
                    // set flushing flag failed, means flushing is true now
                    // indicates another thread wants to interrupt sync thread to exit
                    break;
                }

                lastLogMark.markLog();
                try {
                    ledgerCache.flushLedger(true);
                } catch (IOException e) {
                    LOG.error("Exception flushing Ledger", e);
                }
                try {
                    entryLogger.flush();
                } catch (IOException e) {
                    LOG.error("Exception flushing entry logger", e);
                }
                lastLogMark.rollLog();

                // list the journals that have been marked
                List<Long> logs = listJournalIds(journalDirectory, new JournalIdFilter() {
                    @Override
                    public boolean accept(long journalId) {
                        if (journalId < lastLogMark.lastMark.txnLogId) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });

                // keep MAX_BACKUP_JOURNALS journal files before marked journal
                if (logs.size() >= maxBackupJournals) {
                    int maxIdx = logs.size() - maxBackupJournals;
                    for (int i=0; i<maxIdx; i++) {
                        long id = logs.get(i);
                        // make sure the journal id is smaller than marked journal id
                        if (id < lastLogMark.lastMark.txnLogId) {
                            File journalFile = new File(journalDirectory, Long.toHexString(id) + ".txn");
                            journalFile.delete();
                            LOG.info("garbage collected journal " + journalFile.getName());
                        }
                    }
                }

                // clear flushing flag
                flushing.set(false);
            }
        }

        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            if (flushing.compareAndSet(false, true)) {
                // if setting flushing flag succeed, means syncThread is not flushing now
                // it is safe to interrupt itself now 
                this.interrupt();
            }
            this.join();
        }
    }

    public Bookie(ServerConfiguration conf) 
            throws IOException, KeeperException, InterruptedException {
        super("Bookie-" + conf.getBookiePort());
        this.conf = conf;
        this.journalDirectory = conf.getJournalDir();
        this.ledgerDirectories = conf.getLedgerDirs();
        this.maxJournalSize = conf.getMaxJournalSize() * MB;
        this.maxBackupJournals = conf.getMaxBackupJournals();

        // check directory layouts
        checkDirectoryLayoutVersion(journalDirectory);
        for (File dir : ledgerDirectories) {
            checkDirectoryLayoutVersion(dir);
        }

        // instantiate zookeeper client to initialize ledger manager
        ZooKeeper newZk = instantiateZookeeperClient(conf.getZkServers());
        ledgerManager = LedgerManagerFactory.newLedgerManager(conf, newZk);

        syncThread = new SyncThread(conf);
        entryLogger = new EntryLogger(conf, this);
        ledgerCache = new LedgerCache(conf, ledgerManager);

        lastLogMark.readLog();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Last Log Mark : " + lastLogMark);
        }
        final long markedLogId = lastLogMark.txnLogId;
        List<Long> logs = listJournalIds(journalDirectory, new JournalIdFilter() {
            @Override
            public boolean accept(long journalId) {
                if (journalId < markedLogId) {
                    return false;
                }
                return true;
            }
        });
        // last log mark may be missed due to no sync up before
        // validate filtered log ids only when we have markedLogId
        if (markedLogId > 0) {
            if (logs.size() == 0 || logs.get(0) != markedLogId) {
                throw new IOException("Recovery log " + markedLogId + " is missing");
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Try to relay journal logs : " + logs);
        }
        // TODO: When reading in the journal logs that need to be synced, we
        // should use BufferedChannels instead to minimize the amount of
        // system calls done.
        ByteBuffer lenBuff = ByteBuffer.allocate(4);
        ByteBuffer recBuff = ByteBuffer.allocate(64*1024);
        for(Long id: logs) {
            JournalChannel recLog;
            if(id == markedLogId) {
                long markedLogPosition = lastLogMark.txnLogPosition;
                recLog = new JournalChannel(journalDirectory, id, markedLogPosition);
            } else {
                recLog = new JournalChannel(journalDirectory, id);
            }

            while(true) {
                lenBuff.clear();
                fullRead(recLog, lenBuff);
                if (lenBuff.remaining() != 0) {
                    break;
                }
                lenBuff.flip();
                int len = lenBuff.getInt();
                if (len == 0) {
                    break;
                }
                recBuff.clear();
                if (recBuff.remaining() < len) {
                    recBuff = ByteBuffer.allocate(len);
                }
                recBuff.limit(len);
                if (fullRead(recLog, recBuff) != len) {
                    // This seems scary, but it just means that this is where we
                    // left off writing
                    break;
                }
                recBuff.flip();
                long ledgerId = recBuff.getLong();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Relay journal - ledger id : " + ledgerId);
                }
                LedgerDescriptor handle = getHandle(ledgerId, false);
                try {
                    recBuff.rewind();
                    handle.addEntry(recBuff);
                } finally {
                    putHandle(handle);
                }
            }
            recLog.close();
        }
        // pass zookeeper instance here
        // since GarbageCollector thread should only start after journal
        // finished replay
        this.zk = newZk;
        // make the bookie available
        registerBookie(conf.getBookiePort());
        setDaemon(true);
        LOG.debug("I'm starting a bookie with journal directory " + journalDirectory.getName());
        start();
        syncThread.start();
        // set running here.
        // since bookie server use running as a flag to tell bookie server whether it is alive
        // if setting it in bookie thread, the watcher might run before bookie thread.
        running = true;
    }

    public static interface JournalIdFilter {
        public boolean accept(long journalId);
    }

    /**
     * List all journal ids by a specified journal id filer
     *
     * @param journalDir journal dir
     * @param filter journal id filter
     * @return list of filtered ids
     */
    public static List<Long> listJournalIds(File journalDir, JournalIdFilter filter) {
        File logFiles[] = journalDir.listFiles();
        List<Long> logs = new ArrayList<Long>();
        for(File f: logFiles) {
            String name = f.getName();
            if (!name.endsWith(".txn")) {
                continue;
            }
            String idString = name.split("\\.")[0];
            long id = Long.parseLong(idString, 16);
            if (filter != null) {
                if (filter.accept(id)) {
                    logs.add(id);
                }
            } else {
                logs.add(id);
            }
        }
        Collections.sort(logs);
        return logs;
    }

    /**
     * Register jmx with parent
     *
     * @param parent parent bk mbean info
     */
    public void registerJMX(BKMBeanInfo parent) {
        try {
            jmxBookieBean = new BookieBean(this);
            BKMBeanRegistry.getInstance().register(jmxBookieBean, parent);

            try {
                jmxLedgerCacheBean = new LedgerCacheBean(this.ledgerCache);
                BKMBeanRegistry.getInstance().register(jmxLedgerCacheBean, jmxBookieBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX for ledger cache", e);
                jmxLedgerCacheBean = null;
            }

        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxBookieBean = null;
        }
    }

    /**
     * Unregister jmx
     */
    public void unregisterJMX() {
        try {
            if (jmxLedgerCacheBean != null) {
                BKMBeanRegistry.getInstance().unregister(jmxLedgerCacheBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        try {
            if (jmxBookieBean != null) {
                BKMBeanRegistry.getInstance().unregister(jmxBookieBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxBookieBean = null;
        jmxLedgerCacheBean = null;
    }


    /**
     * Instantiate the ZooKeeper client for the Bookie.
     */
    private ZooKeeper instantiateZookeeperClient(String zkServers) throws IOException {
        if (zkServers == null) {
            LOG.warn("No ZK servers passed to Bookie constructor so BookKeeper clients won't know about this server!");
            isZkExpired = false;
            return null;
        }
        int zkTimeout = conf.getZkTimeout();
        // Create the ZooKeeper client instance
        return newZookeeper(zkServers, zkTimeout);
    }

    /**
     * Register as an available bookie
     */
    private void registerBookie(int port) throws IOException {
        if (null == zk) {
            // zookeeper instance is null, means not register itself to zk
            return;
        }
        // Create the ZK ephemeral node for this Bookie.
        try {
            zk.create(BOOKIE_REGISTRATION_PATH + InetAddress.getLocalHost().getHostAddress() + ":" + port, new byte[0],
                      Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            LOG.error("ZK exception registering ephemeral Znode for Bookie!", e);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new IOException(e);
        }
    }

    /**
     * Create a new zookeeper client to zk cluster.
     *
     * <p>
     * Bookie Server just used zk client when syncing ledgers for garbage collection.
     * So when zk client is expired, it means this bookie server is not available in
     * bookie server list. The bookie client will be notified for its expiration. No
     * more bookie request will be sent to this server. So it's better to exit when zk
     * expired.
     * </p>
     * <p>
     * Since there are lots of bk operations cached in queue, so we wait for all the operations
     * are processed and quit. It is done by calling <b>shutdown</b>.
     * </p>
     *
     * @param zkServers the quorum list of zk servers
     * @param sessionTimeout session timeout of zk connection
     *
     * @return zk client instance
     */
    private ZooKeeper newZookeeper(final String zkServers,
                                   final int sessionTimeout) throws IOException {
        ZooKeeper newZk = new ZooKeeper(zkServers, sessionTimeout,
        new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // handle session disconnects and expires
                if (event.getType()
                .equals(Watcher.Event.EventType.None)) {
                    if (event.getState().equals(
                    Watcher.Event.KeeperState.Disconnected)) {
                        LOG.warn("ZK client has been disconnected to the ZK server!");
                    } else if (event.getState().equals(
                    Watcher.Event.KeeperState.SyncConnected)) {
                        LOG.info("ZK client has been reconnected to the ZK server!");
                    }
                }
                // Check for expired connection.
                if (event.getState().equals(
                Watcher.Event.KeeperState.Expired)) {
                    LOG.error("ZK client connection to the ZK server has expired!");
                    isZkExpired = true;
                    try {
                        shutdown();
                    } catch (InterruptedException ie) {
                        System.exit(-1);
                    }
                }
            }
        });
        isZkExpired = false;
        return newZk;
    }

    /**
     * Check the layout version of a directory. If it is outside of the 
     * range which this version of the software can handle, throw an
     * exception.
     *
     * @param dir Directory to check
     * @throws IOException if layout version if is outside usable range
     *               or if there is a problem reading the version file
     */
    private void checkDirectoryLayoutVersion(File dir) 
            throws IOException {
        if (!dir.isDirectory()) {
            throw new IOException("Directory("+dir+") isn't a directory");
        }
        File versionFile = new File(dir, VERSION_FILENAME);
        
        FileInputStream fis;
        try {
            fis = new FileInputStream(versionFile);
        } catch (FileNotFoundException e) {
            /* 
             * If the version file is not found, this must
             * either be the first time we've used this directory,
             * or it must date from before layout versions were introduced.
             * In both cases, we just create the version file
             */
            LOG.info("No version file found, creating");
            createDirectoryLayoutVersionFile(dir);
            return;
        }
        
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        try {
            String layoutVersionStr = br.readLine();
            int layoutVersion = Integer.parseInt(layoutVersionStr);
            if (layoutVersion != CURRENT_DIRECTORY_LAYOUT_VERSION) {
                String errmsg = "Directory has an invalid version, expected " 
                    + CURRENT_DIRECTORY_LAYOUT_VERSION + ", found " + layoutVersion;
                LOG.error(errmsg);
                throw new IOException(errmsg);
            }
        } catch(NumberFormatException e) {
            throw new IOException("Version file has invalid content", e);
        } finally {
            try {
                fis.close();
            } catch (IOException e) {
                LOG.warn("Error closing version file", e);
            }
        }
    }
    
    /**
     * Create the directory layout version file with the current
     * directory layout version
     */
    private void createDirectoryLayoutVersionFile(File dir) throws IOException {
        File versionFile = new File(dir, VERSION_FILENAME);

        FileOutputStream fos = new FileOutputStream(versionFile);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fos));
            bw.write(String.valueOf(CURRENT_DIRECTORY_LAYOUT_VERSION));
        } finally {
            if (bw != null) {
                bw.close();
            }
            fos.close();
        }
    }

    private static int fullRead(JournalChannel fc, ByteBuffer bb) throws IOException {
        int total = 0;
        while(bb.remaining() > 0) {
            int rc = fc.read(bb);
            if (rc <= 0) {
                return total;
            }
            total += rc;
        }
        return total;
    }
    private void putHandle(LedgerDescriptor handle) {
        synchronized (ledgers) {
            handle.decRef();
        }
    }

    private LedgerDescriptor getHandle(long ledgerId, boolean readonly, byte[] masterKey) throws IOException {
        LedgerDescriptor handle = null;
        synchronized (ledgers) {
            handle = ledgers.get(ledgerId);
            if (handle == null) {
                FileInfo fi = null;
                try {
                    // get file info will throw NoLedgerException
                    fi = ledgerCache.getFileInfo(ledgerId, !readonly);

                    // if an existed ledger index file, we can get its master key
                    // if an new created ledger index file, we will get a null master key
                    byte[] existingMasterKey = fi.readMasterKey();
                    ByteBuffer masterKeyToSet = ByteBuffer.wrap(masterKey);
                    if (existingMasterKey == null) {
                        // no master key set before
                        fi.writeMasterKey(masterKey);
                    } else if (!masterKeyToSet.equals(ByteBuffer.wrap(existingMasterKey))) {
                        throw new IOException("Wrong master key for ledger " + ledgerId);
                    }
                    handle = createHandle(ledgerId, readonly);
                    ledgers.put(ledgerId, handle);
                    handle.setMasterKey(masterKeyToSet);
                } finally {
                    if (fi != null) {
                        fi.release();
                    }
                }
            }
            handle.incRef();
        }
        return handle;
    }

    private LedgerDescriptor getHandle(long ledgerId, boolean readonly) throws IOException {
        LedgerDescriptor handle = null;
        synchronized (ledgers) {
            handle = ledgers.get(ledgerId);
            if (handle == null) {
                FileInfo fi = null;
                try {
                    // get file info will throw NoLedgerException
                    fi = ledgerCache.getFileInfo(ledgerId, !readonly);

                    // if an existed ledger index file, we can get its master key
                    // if an new created ledger index file, we will get a null master key
                    byte[] existingMasterKey = fi.readMasterKey();
                    if (existingMasterKey == null) {
                        throw new IOException("Weird! No master key found in ledger " + ledgerId);
                    }

                    handle = createHandle(ledgerId, readonly);
                    ledgers.put(ledgerId, handle);
                    handle.setMasterKey(ByteBuffer.wrap(existingMasterKey));
                } finally {
                    if (fi != null) {
                        fi.release();
                    }
                }
            }
            handle.incRef();
        }
        return handle;
    }


    private LedgerDescriptor createHandle(long ledgerId, boolean readOnly) throws IOException {
        return new LedgerDescriptor(ledgerId, entryLogger, ledgerCache);
    }

    static class QueueEntry {
        QueueEntry(ByteBuffer entry, long ledgerId, long entryId,
                   WriteCallback cb, Object ctx) {
            this.entry = entry.duplicate();
            this.cb = cb;
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        ByteBuffer entry;

        long ledgerId;

        long entryId;

        WriteCallback cb;

        Object ctx;
    }

    LinkedBlockingQueue<QueueEntry> queue = new LinkedBlockingQueue<QueueEntry>();

    class LastLogMark {
        long txnLogId;
        long txnLogPosition;
        LastLogMark lastMark;
        LastLogMark(long logId, long logPosition) {
            this.txnLogId = logId;
            this.txnLogPosition = logPosition;
        }
        synchronized void setLastLogMark(long logId, long logPosition) {
            txnLogId = logId;
            txnLogPosition = logPosition;
        }
        synchronized void markLog() {
            lastMark = new LastLogMark(txnLogId, txnLogPosition);
        }
        synchronized void rollLog() {
            byte buff[] = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            // we should record <logId, logPosition> marked in markLog
            // which is safe since records before lastMark have been
            // persisted to disk (both index & entry logger)
            bb.putLong(lastMark.txnLogId);
            bb.putLong(lastMark.txnLogPosition);
            if (LOG.isDebugEnabled()) {
                LOG.debug("RollLog to persist last marked log : " + lastMark);
            }
            for(File dir: ledgerDirectories) {
                File file = new File(dir, "lastMark");
                try {
                    FileOutputStream fos = new FileOutputStream(file);
                    fos.write(buff);
                    fos.getChannel().force(true);
                    fos.close();
                } catch (IOException e) {
                    LOG.error("Problems writing to " + file, e);
                }
            }
        }

        /**
         * Read last mark from lastMark file.
         * The last mark should first be max journal log id,
         * and then max log position in max journal log.
         */
        synchronized void readLog() {
            byte buff[] = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            for(File dir: ledgerDirectories) {
                File file = new File(dir, "lastMark");
                try {
                    FileInputStream fis = new FileInputStream(file);
                    fis.read(buff);
                    fis.close();
                    bb.clear();
                    long i = bb.getLong();
                    long p = bb.getLong();
                    if (i > txnLogId) {
                        txnLogId = i;
                        if(p > txnLogPosition) {
                          txnLogPosition = p;
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Problems reading from " + file + " (this is okay if it is the first time starting this bookie");
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            
            sb.append("LastMark: logId - ").append(txnLogId)
              .append(" , position - ").append(txnLogPosition);
            
            return sb.toString();
        }
    }

    private LastLogMark lastLogMark = new LastLogMark(0, 0);

    LastLogMark getLastLogMark() {
        return lastLogMark;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * A thread used for persisting journal entries to journal files.
     * 
     * <p>
     * Besides persisting journal entries, it also takes responsibility of
     * rolling journal files when a journal file reaches journal file size
     * limitation.
     * </p>
     * <p>
     * During journal rolling, it first closes the writing journal, generates
     * new journal file using current timestamp, and continue persistence logic.
     * Those journals will be garbage collected in SyncThread.
     * </p>
     */
    @Override
    public void run() {
        LinkedList<QueueEntry> toFlush = new LinkedList<QueueEntry>();
        ByteBuffer lenBuff = ByteBuffer.allocate(4);
        try {
            long logId = 0;
            JournalChannel logFile = null;
            BufferedChannel bc = null;
            long nextPrealloc = 0;
            long lastFlushPosition = 0;

            QueueEntry qe = null;
            while (true) {
                // new journal file to write
                if (null == logFile) {
                    logId = System.currentTimeMillis();
                    logFile = new JournalChannel(journalDirectory, logId);
                    bc = logFile.getBufferedChannel();

                    lastFlushPosition = 0;
                }

                if (qe == null) {
                    if (toFlush.isEmpty()) {
                        qe = queue.take();
                    } else {
                        qe = queue.poll();
                        if (qe == null || bc.position() > lastFlushPosition + 512*1024) {
                            //logFile.force(false);
                            bc.flush(true);
                            lastFlushPosition = bc.position();
                            lastLogMark.setLastLogMark(logId, lastFlushPosition);
                            for (QueueEntry e : toFlush) {
                                e.cb.writeComplete(0, e.ledgerId, e.entryId, null, e.ctx);
                            }
                            toFlush.clear();

                            // check whether journal file is over file limit
                            if (bc.position() > maxJournalSize) {
                                logFile.close();
                                logFile = null;
                                continue;
                            }
                        }
                    }
                }

                if (isZkExpired) {
                    LOG.warn("Exiting... zk client has expired.");
                    break;
                }
                if (qe == null) { // no more queue entry
                    continue;
                }
                lenBuff.clear();
                lenBuff.putInt(qe.entry.remaining());
                lenBuff.flip();
                //
                // we should be doing the following, but then we run out of
                // direct byte buffers
                // logFile.write(new ByteBuffer[] { lenBuff, qe.entry });
                bc.write(lenBuff);
                bc.write(qe.entry);

                logFile.preAllocIfNeeded();

                toFlush.add(qe);
                qe = null;
            }
        } catch (Exception e) {
            LOG.error("Bookie thread exiting", e);
        }
    }

    public synchronized void shutdown() throws InterruptedException {
        if (!running) { // avoid shutdown twice
            return;
        }
        // Shutdown the ZK client
        if(zk != null) zk.close();
        this.interrupt();
        this.join();
        syncThread.shutdown(); 
        for(LedgerDescriptor d: ledgers.values()) {
            d.close();
        }
        // Shutdown the EntryLogger which has the GarbageCollector Thread running
        entryLogger.shutdown();
        // close Ledger Manager
        ledgerManager.close();
        // setting running to false here, so watch thread in bookie server know it only after bookie shut down
        running = false;
    }

    /** 
     * Retrieve the ledger descriptor for the ledger which entry should be added to.
     * The LedgerDescriptor returned from this method should be eventually freed with 
     * #putHandle().
     *
     * @throws BookieException if masterKey does not match the master key of the ledger
     */
    private LedgerDescriptor getLedgerForEntry(ByteBuffer entry, byte[] masterKey) 
            throws IOException, BookieException {
        long ledgerId = entry.getLong();
        LedgerDescriptor handle = getHandle(ledgerId, false, masterKey);

        if(!handle.cmpMasterKey(ByteBuffer.wrap(masterKey))) {
            putHandle(handle);
            throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
        }
        return handle;
    }

    /**
     * Add an entry to a ledger as specified by handle. 
     */
    private void addEntryInternal(LedgerDescriptor handle, ByteBuffer entry, WriteCallback cb, Object ctx)
            throws IOException, BookieException {
        long ledgerId = handle.getLedgerId();
        entry.rewind();
        long entryId = handle.addEntry(entry);

        entry.rewind();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Adding " + entryId + "@" + ledgerId);
        }
        queue.add(new QueueEntry(entry, ledgerId, entryId, cb, ctx));
    }

    /**
     * Add entry to a ledger, even if the ledger has previous been fenced. This should only
     * happen in bookie recovery or ledger recovery cases, where entries are being replicates 
     * so that they exist on a quorum of bookies. The corresponding client side call for this
     * is not exposed to users.
     */
    public void recoveryAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey) 
            throws IOException, BookieException {
        LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);
        synchronized (handle) {
            try {
                addEntryInternal(handle, entry, cb, ctx);
            } finally {
                putHandle(handle);
            }
        }
    }
    
    /** 
     * Add entry to a ledger.
     * @throws BookieException.LedgerFencedException if the ledger is fenced
     */
    public void addEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);
        synchronized (handle) {
            try {
                if (handle.isFenced()) {
                    throw BookieException.create(BookieException.Code.LedgerFencedException);
                }
                
                addEntryInternal(handle, entry, cb, ctx);
            } finally {
                putHandle(handle);
            }
        }
    }

    /**
     * Fences a ledger. From this point on, clients will be unable to 
     * write to this ledger. Only recoveryAddEntry will be
     * able to add entries to the ledger.
     * This method is idempotent. Once a ledger is fenced, it can
     * never be unfenced. Fencing a fenced ledger has no effect.
     */
    public void fenceLedger(long ledgerId) throws IOException {
        LedgerDescriptor handle = getHandle(ledgerId, true);
        synchronized (handle) {
            handle.setFenced();
        }
    }

    public ByteBuffer readEntry(long ledgerId, long entryId) throws IOException {
        LedgerDescriptor handle = getHandle(ledgerId, true);
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Reading " + entryId + "@" + ledgerId);
            }
            return handle.readEntry(entryId);
        } finally {
            putHandle(handle);
        }
    }

    // The rest of the code is test stuff
    static class CounterCallback implements WriteCallback {
        int count;

        synchronized public void writeComplete(int rc, long l, long e, InetSocketAddress addr, Object ctx) {
            count--;
            if (count == 0) {
                notifyAll();
            }
        }

        synchronized public void incCount() {
            count++;
        }

        synchronized public void waitZero() throws InterruptedException {
            while (count > 0) {
                wait();
            }
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) 
            throws IOException, InterruptedException, BookieException, KeeperException {
        Bookie b = new Bookie(new ServerConfiguration());
        CounterCallback cb = new CounterCallback();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            ByteBuffer buff = ByteBuffer.allocate(1024);
            buff.putLong(1);
            buff.putLong(i);
            buff.limit(1024);
            buff.position(0);
            cb.incCount();
            b.addEntry(buff, cb, null, new byte[0]);
        }
        cb.waitZero();
        long end = System.currentTimeMillis();
        System.out.println("Took " + (end-start) + "ms");
    }
}
