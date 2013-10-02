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
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;

/**
 * This class manages the writing of the bookkeeper entries. All the new
 * entries are written to a common log. The LedgerCache will have pointers
 * into files created by this class with offsets into the files to find
 * the actual ledger entry. The entry log files created by this class are
 * identified by a long.
 */
public class EntryLogger {
    private static final Logger LOG = LoggerFactory.getLogger(EntryLogger.class);

    volatile File currentDir;
    private LedgerDirsManager ledgerDirsManager;
    private AtomicBoolean shouldCreateNewEntryLog = new AtomicBoolean(false);

    private long logId;

    /**
     * The maximum size of a entry logger file.
     */
    final long logSizeLimit;
    private volatile BufferedChannel logChannel;
    private final CopyOnWriteArrayList<EntryLogListener> listeners
        = new CopyOnWriteArrayList<EntryLogListener>();
    // this indicates that a write has happened since the last flush
    private volatile boolean somethingWritten = false;

    /**
     * The 1K block at the head of the entry logger file
     * that contains the fingerprint and (future) meta-data
     */
    final static int LOGFILE_HEADER_SIZE = 1024;
    final ByteBuffer LOGFILE_HEADER = ByteBuffer.allocate(LOGFILE_HEADER_SIZE);

    final static int MIN_SANE_ENTRY_SIZE = 8 + 8;
    final static long MB = 1024 * 1024;

    /**
     * Scan entries in a entry log file.
     */
    static interface EntryLogScanner {
        /**
         * Tests whether or not the entries belongs to the specified ledger
         * should be processed.
         *
         * @param ledgerId
         *          Ledger ID.
         * @return true if and only the entries of the ledger should be scanned.
         */
        public boolean accept(long ledgerId);

        /**
         * Process an entry.
         *
         * @param ledgerId
         *          Ledger ID.
         * @param offset
         *          File offset of this entry.
         * @param entry
         *          Entry ByteBuffer
         * @throws IOException
         */
        public void process(long ledgerId, long offset, ByteBuffer entry) throws IOException;
    }

    /**
     * Entry Log Listener
     */
    static interface EntryLogListener {
        /**
         * Callback when entry log is flushed.
         */
        public void onEntryLogFlushed();
    }

    /**
     * Create an EntryLogger that stores it's log files in the given
     * directories
     */
    public EntryLogger(ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager) throws IOException {
        this.ledgerDirsManager = ledgerDirsManager;
        // log size limit
        this.logSizeLimit = conf.getEntryLogSizeLimit();

        // Initialize the entry log header buffer. This cannot be a static object
        // since in our unit tests, we run multiple Bookies and thus EntryLoggers
        // within the same JVM. All of these Bookie instances access this header
        // so there can be race conditions when entry logs are rolled over and
        // this header buffer is cleared before writing it into the new logChannel.
        LOGFILE_HEADER.put("BKLO".getBytes());

        // Find the largest logId
        logId = -1;
        for (File dir : ledgerDirsManager.getAllLedgerDirs()) {
            if (!dir.exists()) {
                throw new FileNotFoundException(
                        "Entry log directory does not exist");
            }
            long lastLogId = getLastLogId(dir);
            if (lastLogId > logId) {
                logId = lastLogId;
            }
        }

        initialize();
    }

    void addListener(EntryLogListener listener) {
        if (null != listener) {
            listeners.add(listener);
        }
    }

    /**
     * Maps entry log files to open channels.
     */
    private ConcurrentHashMap<Long, BufferedChannel> channels = new ConcurrentHashMap<Long, BufferedChannel>();

    synchronized long getCurrentLogId() {
        return logId;
    }

    protected void initialize() throws IOException {
        // Register listener for disk full notifications.
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        // create a new log to write
        createNewLog();
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {
            @Override
            public void diskFull(File disk) {
                // If the current entry log disk is full, then create new entry
                // log.
                if (currentDir != null && currentDir.equals(disk)) {
                    shouldCreateNewEntryLog.set(true);
                }
            }

            @Override
            public void diskFailed(File disk) {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void allDisksFull() {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void fatalError() {
                // Nothing to handle here. Will be handled in Bookie
            }
        };
    }

    /**
     * Creates a new log file
     */
    void createNewLog() throws IOException {
        if (logChannel != null) {
            logChannel.flush(true);
        }

        // It would better not to overwrite existing entry log files
        String logFileName = null;
        do {
            logFileName = Long.toHexString(++logId) + ".log";
            for (File dir : ledgerDirsManager.getAllLedgerDirs()) {
                File newLogFile = new File(dir, logFileName);
                if (newLogFile.exists()) {
                    LOG.warn("Found existed entry log " + newLogFile
                           + " when trying to create it as a new log.");
                    logFileName = null;
                    break;
                }
            }
        } while (logFileName == null);

        // Update last log id first
        currentDir = ledgerDirsManager.pickRandomWritableDir();
        setLastLogId(currentDir, logId);

        File newLogFile = new File(currentDir, logFileName);
        logChannel = new BufferedChannel(new RandomAccessFile(newLogFile, "rw").getChannel(), 64*1024);
        logChannel.write((ByteBuffer) LOGFILE_HEADER.clear());
        channels.put(logId, logChannel);
    }

    /**
     * Remove entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    protected boolean removeEntryLog(long entryLogId) {
        BufferedChannel bc = channels.remove(entryLogId);
        if (null != bc) {
            // close its underlying file channel, so it could be deleted really
            try {
                bc.getFileChannel().close();
            } catch (IOException ie) {
                LOG.warn("Exception while closing garbage collected entryLog file : ", ie);
            }
        }
        File entryLogFile;
        try {
            entryLogFile = findFile(entryLogId);
        } catch (FileNotFoundException e) {
            LOG.error("Trying to delete an entryLog file that could not be found: "
                    + entryLogId + ".log");
            return false;
        }
        if (!entryLogFile.delete()) {
            LOG.warn("Could not delete entry log file {}", entryLogFile);
        }
        return true;
    }

    /**
     * writes the given id to the "lastId" file in the given directory.
     */
    private void setLastLogId(File dir, long logId) throws IOException {
        FileOutputStream fos;
        fos = new FileOutputStream(new File(dir, "lastId"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        try {
            bw.write(Long.toHexString(logId) + "\n");
            bw.flush();
        } finally {
            try {
                bw.close();
            } catch (IOException e) {
            }
        }
    }

    private long getLastLogId(File dir) {
        long id = readLastLogId(dir);
        // read success
        if (id > 0) {
            return id;
        }
        // read failed, scan the ledger directories to find biggest log id
        File[] logFiles = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.getName().endsWith(".log");
            }
        });
        List<Long> logs = new ArrayList<Long>();
        for (File lf : logFiles) {
            String idString = lf.getName().split("\\.")[0];
            try {
                long lid = Long.parseLong(idString, 16);
                logs.add(lid);
            } catch (NumberFormatException nfe) {
            }
        }
        // no log file found in this directory
        if (0 == logs.size()) {
            return -1;
        }
        // order the collections
        Collections.sort(logs);
        return logs.get(logs.size() - 1);
    }

    /**
     * reads id from the "lastId" file in the given directory.
     */
    private long readLastLogId(File f) {
        FileInputStream fis;
        try {
            fis = new FileInputStream(new File(f, "lastId"));
        } catch (FileNotFoundException e) {
            return -1;
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        try {
            String lastIdString = br.readLine();
            return Long.parseLong(lastIdString, 16);
        } catch (IOException e) {
            return -1;
        } catch(NumberFormatException e) {
            return -1;
        } finally {
            try {
                br.close();
            } catch (IOException e) {
            }
        }
    }

    synchronized void flush() throws IOException {
        if (logChannel != null) {
            logChannel.flush(true);
        }
        somethingWritten = false;
        for (EntryLogListener listener: listeners) {
            listener.onEntryLogFlushed();
        }
    }

    boolean isFlushRequired() {
        return somethingWritten;
    }

    synchronized long addEntry(long ledger, ByteBuffer entry) throws IOException {
        // Create new log if logSizeLimit reached or current disk is full
        boolean createNewLog = shouldCreateNewEntryLog.get();
        if (createNewLog
                || (logChannel.position() + entry.remaining() + 4 > logSizeLimit)) {
            createNewLog();

            // Reset the flag
            if (createNewLog) {
                shouldCreateNewEntryLog.set(false);
            }
        }
        ByteBuffer buff = ByteBuffer.allocate(4);
        buff.putInt(entry.remaining());
        buff.flip();
        logChannel.write(buff);
        long pos = logChannel.position();
        logChannel.write(entry);
        //logChannel.flush(false);

        somethingWritten = true;

        return (logId << 32L) | pos;
    }

    byte[] readEntry(long ledgerId, long entryId, long location) throws IOException, Bookie.NoEntryException {
        long entryLogId = location >> 32L;
        long pos = location & 0xffffffffL;
        ByteBuffer sizeBuff = ByteBuffer.allocate(4);
        pos -= 4; // we want to get the ledgerId and length to check
        BufferedChannel fc;
        try {
            fc = getChannelForLogId(entryLogId);
        } catch (FileNotFoundException e) {
            FileNotFoundException newe = new FileNotFoundException(e.getMessage() + " for " + ledgerId + " with location " + location);
            newe.setStackTrace(e.getStackTrace());
            throw newe;
        }
        if (fc.read(sizeBuff, pos) != sizeBuff.capacity()) {
            throw new Bookie.NoEntryException("Short read from entrylog " + entryLogId,
                                              ledgerId, entryId);
        }
        pos += 4;
        sizeBuff.flip();
        int entrySize = sizeBuff.getInt();
        // entrySize does not include the ledgerId
        if (entrySize > MB) {
            LOG.error("Sanity check failed for entry size of " + entrySize + " at location " + pos + " in " + entryLogId);

        }
        if (entrySize < MIN_SANE_ENTRY_SIZE) {
            LOG.error("Read invalid entry length {}", entrySize);
            throw new IOException("Invalid entry length " + entrySize);
        }
        byte data[] = new byte[entrySize];
        ByteBuffer buff = ByteBuffer.wrap(data);
        int rc = fc.read(buff, pos);
        if ( rc != data.length) {
            // Note that throwing NoEntryException here instead of IOException is not
            // without risk. If all bookies in a quorum throw this same exception
            // the client will assume that it has reached the end of the ledger.
            // However, this may not be the case, as a very specific error condition
            // could have occurred, where the length of the entry was corrupted on all
            // replicas. However, the chance of this happening is very very low, so
            // returning NoEntryException is mostly safe.
            throw new Bookie.NoEntryException("Short read for " + ledgerId + "@"
                                              + entryId + " in " + entryLogId + "@"
                                              + pos + "("+rc+"!="+data.length+")", ledgerId, entryId);
        }
        buff.flip();
        long thisLedgerId = buff.getLong();
        if (thisLedgerId != ledgerId) {
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos + " entry belongs to " + thisLedgerId + " not " + ledgerId);
        }
        long thisEntryId = buff.getLong();
        if (thisEntryId != entryId) {
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos + " entry is " + thisEntryId + " not " + entryId);
        }

        return data;
    }

    private BufferedChannel getChannelForLogId(long entryLogId) throws IOException {
        BufferedChannel fc = channels.get(entryLogId);
        if (fc != null) {
            return fc;
        }
        File file = findFile(entryLogId);
        // get channel is used to open an existing entry log file
        // it would be better to open using read mode
        FileChannel newFc = new RandomAccessFile(file, "r").getChannel();
        // If the file already exists before creating a BufferedChannel layer above it,
        // set the FileChannel's position to the end so the write buffer knows where to start.
        newFc.position(newFc.size());
        fc = new BufferedChannel(newFc, 8192);

        BufferedChannel oldfc = channels.putIfAbsent(entryLogId, fc);
        if (oldfc != null) {
            newFc.close();
            return oldfc;
        } else {
            return fc;
        }
    }

    /**
     * Whether the log file exists or not.
     */
    boolean logExists(long logId) {
        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File f = new File(d, Long.toHexString(logId) + ".log");
            if (f.exists()) {
                return true;
            }
        }
        return false;
    }

    private File findFile(long logId) throws FileNotFoundException {
        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File f = new File(d, Long.toHexString(logId)+".log");
            if (f.exists()) {
                return f;
            }
        }
        throw new FileNotFoundException("No file for log " + Long.toHexString(logId));
    }

    /**
     * Scan entry log
     *
     * @param entryLogId
     *          Entry Log Id
     * @param scanner
     *          Entry Log Scanner
     * @throws IOException
     */
    protected void scanEntryLog(long entryLogId, EntryLogScanner scanner) throws IOException {
        ByteBuffer sizeBuff = ByteBuffer.allocate(4);
        ByteBuffer lidBuff = ByteBuffer.allocate(8);
        BufferedChannel bc;
        // Get the BufferedChannel for the current entry log file
        try {
            bc = getChannelForLogId(entryLogId);
        } catch (IOException e) {
            LOG.warn("Failed to get channel to scan entry log: " + entryLogId + ".log");
            throw e;
        }
        // Start the read position in the current entry log file to be after
        // the header where all of the ledger entries are.
        long pos = LOGFILE_HEADER_SIZE;
        // Read through the entry log file and extract the ledger ID's.
        while (true) {
            // Check if we've finished reading the entry log file.
            if (pos >= bc.size()) {
                break;
            }
            if (bc.read(sizeBuff, pos) != sizeBuff.capacity()) {
                throw new IOException("Short read for entry size from entrylog " + entryLogId);
            }
            long offset = pos;
            pos += 4;
            sizeBuff.flip();
            int entrySize = sizeBuff.getInt();
            if (entrySize > MB) {
                LOG.warn("Found large size entry of " + entrySize + " at location " + pos + " in "
                        + entryLogId);
            }
            sizeBuff.clear();
            // try to read ledger id first
            if (bc.read(lidBuff, pos) != lidBuff.capacity()) {
                throw new IOException("Short read for ledger id from entrylog " + entryLogId);
            }
            lidBuff.flip();
            long lid = lidBuff.getLong();
            lidBuff.clear();
            if (!scanner.accept(lid)) {
                // skip this entry
                pos += entrySize;
                continue;
            }
            // read the entry
            byte data[] = new byte[entrySize];
            ByteBuffer buff = ByteBuffer.wrap(data);
            int rc = bc.read(buff, pos);
            if (rc != data.length) {
                throw new IOException("Short read for ledger entry from entryLog " + entryLogId
                                    + "@" + pos + "(" + rc + "!=" + data.length + ")");
            }
            buff.flip();
            // process the entry
            scanner.process(lid, offset, buff);
            // Advance position to the next entry
            pos += entrySize;
        }
    }

    /**
     * Shutdown method to gracefully stop entry logger.
     */
    public void shutdown() {
        // since logChannel is buffered channel, do flush when shutting down
        try {
            flush();
            for (Entry<Long, BufferedChannel> channelEntry : channels
                    .entrySet()) {
                channelEntry.getValue().getFileChannel().close();
            }
        } catch (IOException ie) {
            // we have no idea how to avoid io exception during shutting down, so just ignore it
            LOG.error("Error flush entry log during shutting down, which may cause entry log corrupted.", ie);
        } finally {
            for (Entry<Long, BufferedChannel> channelEntry : channels
                    .entrySet()) {
                FileChannel fileChannel = channelEntry.getValue()
                        .getFileChannel();
                if (fileChannel.isOpen()) {
                    IOUtils.close(LOG, fileChannel);
                }
            }
        }
    }

}
