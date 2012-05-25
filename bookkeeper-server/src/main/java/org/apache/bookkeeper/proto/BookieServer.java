package org.apache.bookkeeper.proto;

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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import javax.management.JMException;

import org.apache.zookeeper.KeeperException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanRegistry;
import org.apache.bookkeeper.proto.NIOServerFactory.Cnxn;
import static org.apache.bookkeeper.proto.BookieProtocol.PacketHeader;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the server-side part of the BookKeeper protocol.
 *
 */
public class BookieServer implements NIOServerFactory.PacketProcessor, BookkeeperInternalCallbacks.WriteCallback {
    final ServerConfiguration conf;
    NIOServerFactory nioServerFactory;
    private volatile boolean running = false;
    Bookie bookie;
    DeathWatcher deathWatcher;
    static Logger LOG = LoggerFactory.getLogger(BookieServer.class);

    int exitCode = ExitCode.OK;

    // operation stats
    final BKStats bkStats = BKStats.getInstance();
    final boolean isStatsEnabled;
    protected BookieServerBean jmxBkServerBean;

    public BookieServer(ServerConfiguration conf) 
            throws IOException, KeeperException, InterruptedException, BookieException {
        this.conf = conf;
        this.bookie = newBookie(conf);

        isStatsEnabled = conf.isStatisticsEnabled();
    }

    protected Bookie newBookie(ServerConfiguration conf)
        throws IOException, KeeperException, InterruptedException, BookieException {
        return new Bookie(conf);
    }

    public void start() throws IOException {
        this.bookie.start();

        nioServerFactory = new NIOServerFactory(conf, this);
        nioServerFactory.start();
        running = true;
        deathWatcher = new DeathWatcher(conf);
        deathWatcher.start();

        // register jmx
        registerJMX();
    }

    public InetSocketAddress getLocalAddress() {
        try {
            return new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), conf.getBookiePort());
        } catch (UnknownHostException uhe) {
            return nioServerFactory.getLocalAddress();
        }
    }

    public synchronized void shutdown() {
        if (!running) {
            return;
        }
        nioServerFactory.shutdown();
        exitCode = bookie.shutdown();
        running = false;

        // unregister JMX
        unregisterJMX();
    }

    protected void registerJMX() {
        try {
            jmxBkServerBean = new BookieServerBean(conf, this);
            BKMBeanRegistry.getInstance().register(jmxBkServerBean, null);

            bookie.registerJMX(jmxBkServerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxBkServerBean = null;
        }
    }

    protected void unregisterJMX() {
        try {
            bookie.unregisterJMX();
            if (jmxBkServerBean != null) {
                BKMBeanRegistry.getInstance().unregister(jmxBkServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxBkServerBean = null;
    }

    public boolean isRunning() {
        return bookie.isRunning() && nioServerFactory.isRunning() && running;
    }

    /**
     * Whether bookie is running?
     *
     * @return true if bookie is running, otherwise return false
     */
    public boolean isBookieRunning() {
        return bookie.isRunning();
    }

    /**
     * Whether nio server is running?
     *
     * @return true if nio server is running, otherwise return false
     */
    public boolean isNioServerRunning() {
        return nioServerFactory.isRunning();
    }

    public void join() throws InterruptedException {
        nioServerFactory.join();
    }

    public int getExitCode() {
        return exitCode;
    }

    /**
     * A thread to watch whether bookie & nioserver is still alive
     */
    class DeathWatcher extends Thread {

        final int watchInterval;

        DeathWatcher(ServerConfiguration conf) {
            watchInterval = conf.getDeathWatchInterval();
        }

        @Override
        public void run() {
            while(true) {
                try {
                    Thread.sleep(watchInterval);
                } catch (InterruptedException ie) {
                    // do nothing
                }
                if (!isBookieRunning() || !isNioServerRunning()) {
                    shutdown();
                    break;
                }
            }
        }
    }

    static final Options bkOpts = new Options();
    static {
        bkOpts.addOption("c", "conf", true, "Configuration for Bookie Server");
        bkOpts.addOption("h", "help", false, "Print help message");
    }

    /**
     * Print usage
     */
    private static void printUsage() {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("BookieServer [options]\n\tor\n"
                   + "BookieServer <bookie_port> <zk_servers> <journal_dir> <ledger_dir [ledger_dir]>", bkOpts);
    }

    private static void loadConfFile(ServerConfiguration conf, String confFile)
        throws IllegalArgumentException {
        try {
            conf.loadConf(new File(confFile).toURI().toURL());
        } catch (MalformedURLException e) {
            LOG.error("Could not open configuration file: " + confFile, e);
            throw new IllegalArgumentException();
        } catch (ConfigurationException e) {
            LOG.error("Malformed configuration file: " + confFile, e);
            throw new IllegalArgumentException();
        }
        LOG.info("Using configuration file " + confFile);
    }

    private static ServerConfiguration parseArgs(String[] args)
        throws IllegalArgumentException {
        try {
            BasicParser parser = new BasicParser();
            CommandLine cmdLine = parser.parse(bkOpts, args);

            if (cmdLine.hasOption('h')) {
                throw new IllegalArgumentException();
            }

            ServerConfiguration conf = new ServerConfiguration();
            String[] leftArgs = cmdLine.getArgs();

            if (cmdLine.hasOption('c')) {
                if (null != leftArgs && leftArgs.length > 0) {
                    throw new IllegalArgumentException();
                }
                String confFile = cmdLine.getOptionValue("c");
                loadConfFile(conf, confFile);
                return conf;
            }

            if (leftArgs.length < 4) {
                throw new IllegalArgumentException();
            }

            // command line arguments overwrite settings in configuration file
            conf.setBookiePort(Integer.parseInt(leftArgs[0]));
            conf.setZkServers(leftArgs[1]);
            conf.setJournalDirName(leftArgs[2]);
            String[] ledgerDirNames = new String[leftArgs.length - 3];
            System.arraycopy(leftArgs, 3, ledgerDirNames, 0, ledgerDirNames.length);
            conf.setLedgerDirNames(ledgerDirNames);

            return conf;
        } catch (ParseException e) {
            LOG.error("Error parsing command line arguments : ", e);
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) {
        ServerConfiguration conf = null;
        try {
            conf = parseArgs(args);
        } catch (IllegalArgumentException iae) {
            LOG.error("Error parsing command line arguments : ", iae);
            System.err.println(iae.getMessage());
            printUsage();
            System.exit(ExitCode.INVALID_CONF);
        }

        StringBuilder sb = new StringBuilder();
        String[] ledgerDirNames = conf.getLedgerDirNames();
        for (int i = 0; i < ledgerDirNames.length; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append(ledgerDirNames[i]);
        }

        String hello = String.format(
                           "Hello, I'm your bookie, listening on port %1$s. ZKServers are on %2$s. Journals are in %3$s. Ledgers are stored in %4$s.",
                           conf.getBookiePort(), conf.getZkServers(),
                           conf.getJournalDirName(), sb);
        LOG.info(hello);
        try {
            final BookieServer bs = new BookieServer(conf);
            bs.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    bs.shutdown();
                    LOG.info("Shut down bookie server successfully");
                }
            });
            LOG.info("Register shutdown hook successfully");
            bs.join();

            System.exit(bs.getExitCode());
        } catch (Exception e) {
            LOG.error("Exception running bookie server : ", e);
            System.exit(ExitCode.SERVER_EXCEPTION);
        }
    }

    public void processPacket(ByteBuffer packet, Cnxn src) {
        PacketHeader h = PacketHeader.fromInt(packet.getInt());

        boolean success = false;
        int statType = BKStats.STATS_UNKNOWN;
        long startTime = 0;
        if (isStatsEnabled) {
            startTime = System.currentTimeMillis();
        }

        // packet format is different between ADDENTRY and READENTRY
        long ledgerId = -1;
        long entryId = BookieProtocol.INVALID_ENTRY_ID;
        byte[] masterKey = null;
        switch (h.getOpCode()) {
        case BookieProtocol.ADDENTRY:
            // first read master key
            masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];
            packet.get(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);
            ByteBuffer bb = packet.duplicate();
            ledgerId = bb.getLong();
            entryId = bb.getLong();
            break;
        case BookieProtocol.READENTRY:
            ledgerId = packet.getLong();
            entryId = packet.getLong();
            break;
        }

        if (h.getVersion() < BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION
            || h.getVersion() > BookieProtocol.CURRENT_PROTOCOL_VERSION) {
            LOG.error("Invalid protocol version, expected something between "
                      + BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION 
                      + " & " + BookieProtocol.CURRENT_PROTOCOL_VERSION
                    + ". got " + h.getVersion());
            src.sendResponse(buildResponse(BookieProtocol.EBADVERSION, 
                                           h.getVersion(), h.getOpCode(), ledgerId, entryId));
            return;
        }
        short flags = h.getFlags();
        switch (h.getOpCode()) {
        case BookieProtocol.ADDENTRY:
            statType = BKStats.STATS_ADD;
            try {
                TimedCnxn tsrc = new TimedCnxn(src, startTime);
                // LOG.debug("Master key: " + new String(masterKey));
                if ((flags & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD) {
                    bookie.recoveryAddEntry(packet.slice(), this, tsrc, masterKey);
                } else {
                    bookie.addEntry(packet.slice(), this, tsrc, masterKey);
                }
                success = true;
            } catch (IOException e) {
                LOG.error("Error writing " + entryId + "@" + ledgerId, e);
                src.sendResponse(buildResponse(BookieProtocol.EIO, h.getVersion(), h.getOpCode(), ledgerId, entryId));
            } catch (BookieException.LedgerFencedException lfe) {
                LOG.error("Attempt to write to fenced ledger", lfe);
                src.sendResponse(buildResponse(BookieProtocol.EFENCED, h.getVersion(), h.getOpCode(), ledgerId, entryId));
            } catch (BookieException e) {
                LOG.error("Unauthorized access to ledger " + ledgerId, e);
                src.sendResponse(buildResponse(BookieProtocol.EUA, h.getVersion(), h.getOpCode(), ledgerId, entryId));
            }
            break;
        case BookieProtocol.READENTRY:
            statType = BKStats.STATS_READ;
            ByteBuffer[] rsp = new ByteBuffer[2];
            LOG.debug("Received new read request: " + ledgerId + ", " + entryId);
            int errorCode = BookieProtocol.EIO;
            try {
                if ((flags & BookieProtocol.FLAG_DO_FENCING) == BookieProtocol.FLAG_DO_FENCING) {
                    LOG.warn("Ledger " + ledgerId + " fenced by " + src.getPeerName());
                    if (h.getVersion() >= 2) {
                        masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];
                        packet.get(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);

                        bookie.fenceLedger(ledgerId, masterKey);
                    } else {
                        LOG.error("Password not provided, Not safe to fence {}", ledgerId);
                        throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                    }
                }
                rsp[1] = bookie.readEntry(ledgerId, entryId);
                LOG.debug("##### Read entry ##### " + rsp[1].remaining());
                errorCode = BookieProtocol.EOK;
                success = true;
            } catch (Bookie.NoLedgerException e) {
                if (LOG.isTraceEnabled()) {
                    LOG.error("Error reading " + entryId + "@" + ledgerId, e);
                }
                errorCode = BookieProtocol.ENOLEDGER;
            } catch (Bookie.NoEntryException e) {
                if (LOG.isTraceEnabled()) {
                    LOG.error("Error reading " + entryId + "@" + ledgerId, e);
                }
                errorCode = BookieProtocol.ENOENTRY;
            } catch (IOException e) {
                if (LOG.isTraceEnabled()) {
                    LOG.error("Error reading " + entryId + "@" + ledgerId, e);
                }
                errorCode = BookieProtocol.EIO;
            } catch (BookieException e) {
                LOG.error("Unauthorized access to ledger " + ledgerId, e);
                errorCode = BookieProtocol.EUA;
            }
            rsp[0] = buildResponse(errorCode, h.getVersion(), h.getOpCode(), ledgerId, entryId);

            if (LOG.isTraceEnabled()) {
                LOG.trace("Read entry rc = " + errorCode + " for " + entryId + "@" + ledgerId);
            }
            if (rsp[1] == null) {
                // We haven't filled in entry data, so we have to send back
                // the ledger and entry ids here
                rsp[1] = ByteBuffer.allocate(16);
                rsp[1].putLong(ledgerId);
                rsp[1].putLong(entryId);
                rsp[1].flip();
            }
            LOG.debug("Sending response for: " + entryId + ", " + new String(rsp[1].array()));
            src.sendResponse(rsp);
            break;
        default:
            src.sendResponse(buildResponse(BookieProtocol.EBADREQ, h.getVersion(), h.getOpCode(), ledgerId, entryId));
        }
        if (isStatsEnabled) {
            if (success) {
                // for add operations, we compute latency in writeComplete callbacks.
                if (statType != BKStats.STATS_ADD) {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    bkStats.getOpStats(statType).updateLatency(elapsedTime);
                }
            } else {
                bkStats.getOpStats(statType).incrementFailedOps();
            }
        }
    }

    private ByteBuffer buildResponse(int errorCode, byte version, byte opCode, long ledgerId, long entryId) {
        ByteBuffer rsp = ByteBuffer.allocate(24);
        rsp.putInt(new PacketHeader(version, 
                                    opCode, (short)0).toInt());
        rsp.putInt(errorCode);
        rsp.putLong(ledgerId);
        rsp.putLong(entryId);

        rsp.flip();
        return rsp;
    }

    public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx) {
        TimedCnxn tcnxn = (TimedCnxn) ctx;
        Cnxn src = tcnxn.cnxn;
        long startTime = tcnxn.time;
        ByteBuffer bb = ByteBuffer.allocate(24);
        bb.putInt(new PacketHeader(BookieProtocol.CURRENT_PROTOCOL_VERSION, 
                                   BookieProtocol.ADDENTRY, (short)0).toInt());
        bb.putInt(rc);
        bb.putLong(ledgerId);
        bb.putLong(entryId);
        bb.flip();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Add entry rc = " + rc + " for " + entryId + "@" + ledgerId);
        }
        src.sendResponse(new ByteBuffer[] { bb });
        if (isStatsEnabled) {
            // compute the latency
            if (0 == rc) {
                // for add operations, we compute latency in writeComplete callbacks.
                long elapsedTime = System.currentTimeMillis() - startTime;
                bkStats.getOpStats(BKStats.STATS_ADD).updateLatency(elapsedTime);
            } else {
                bkStats.getOpStats(BKStats.STATS_ADD).incrementFailedOps();                
            }
        }
    }

    /**
     * A cnxn wrapper for time
     */
    static class TimedCnxn {
        Cnxn cnxn;
        long time;

        public TimedCnxn(Cnxn cnxn, long startTime) {
            this.cnxn = cnxn;
            this.time = startTime;
        }
    }

}
