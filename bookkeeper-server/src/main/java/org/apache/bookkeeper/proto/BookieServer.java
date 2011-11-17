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
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.NIOServerFactory.Cnxn;
import static org.apache.bookkeeper.proto.BookieProtocol.PacketHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the server-side part of the BookKeeper protocol.
 *
 */
public class BookieServer implements NIOServerFactory.PacketProcessor, BookkeeperInternalCallbacks.WriteCallback {
    int port;
    NIOServerFactory nioServerFactory;
    private volatile boolean running = false;
    Bookie bookie;
    DeathWatcher deathWatcher;
    static Logger LOG = LoggerFactory.getLogger(BookieServer.class);

    public BookieServer(int port, String zkServers, File journalDirectory, File ledgerDirectories[]) throws IOException {
        this.port = port;
        this.bookie = new Bookie(port, zkServers, journalDirectory, ledgerDirectories);
    }

    public void start() throws IOException {
        nioServerFactory = new NIOServerFactory(port, this);
        running = true;
        deathWatcher = new DeathWatcher();
        deathWatcher.start();
    }

    public InetSocketAddress getLocalAddress() {
        try {
            return new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), port);
        } catch (UnknownHostException uhe) {
            return nioServerFactory.getLocalAddress();
        }
    }

    public synchronized void shutdown() throws InterruptedException {
        if (!running) {
            return;
        }
        nioServerFactory.shutdown();
        bookie.shutdown();
        running = false;
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

    /**
     * A thread to watch whether bookie & nioserver is still alive
     */
    class DeathWatcher extends Thread {
        @Override
        public void run() {
            int watchInterval = Integer.getInteger("bookie_death_watch_interval", 1000);
            while(true) {
                try {
                    Thread.sleep(watchInterval);
                } catch (InterruptedException ie) {
                    // do nothing
                }
                if (!isBookieRunning() || !isNioServerRunning()) {
                    try {
                        shutdown();
                    } catch (InterruptedException ie) {
                        System.exit(-1);
                    }
                    break;
                }
            }
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 4) {
            System.err.println("USAGE: BookieServer port zkServers journalDirectory ledgerDirectory [ledgerDirectory]*");
            return;
        }
        int port = Integer.parseInt(args[0]);
        String zkServers = args[1];
        File journalDirectory = new File(args[2]);
        File ledgerDirectory[] = new File[args.length - 3];
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ledgerDirectory.length; i++) {
            ledgerDirectory[i] = new File(args[i + 3]);
            if (i != 0) {
                sb.append(',');
            }
            sb.append(ledgerDirectory[i]);
        }
        String hello = String.format(
                           "Hello, I'm your bookie, listening on port %1$s. ZKServers are on %2$s. Journals are in %3$s. Ledgers are stored in %4$s.",
                           port, zkServers, journalDirectory, sb);
        LOG.info(hello);
        BookieServer bs = new BookieServer(port, zkServers, journalDirectory, ledgerDirectory);
        bs.start();
        bs.join();
    }

    public void processPacket(ByteBuffer packet, Cnxn src) {
        PacketHeader h = PacketHeader.fromInt(packet.getInt());

        // packet format is different between ADDENTRY and READENTRY
        long ledgerId = -1;
        long entryId = -1;
        byte[] masterKey = null;
        switch (h.getOpCode()) {
        case BookieProtocol.ADDENTRY:
            // first read master key
            masterKey = new byte[20];
            packet.get(masterKey, 0, 20);
            // !! fall thru to read ledger id and entry id
        case BookieProtocol.READENTRY:
            ByteBuffer bb = packet.duplicate();
            ledgerId = bb.getLong();
            entryId = bb.getLong();
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
            try {
                // LOG.debug("Master key: " + new String(masterKey));
                if ((flags & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD) {
                    bookie.recoveryAddEntry(packet.slice(), this, src, masterKey);
                } else {
                    bookie.addEntry(packet.slice(), this, src, masterKey);
                }
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
            ByteBuffer[] rsp = new ByteBuffer[2];
            LOG.debug("Received new read request: " + ledgerId + ", " + entryId);
            int errorCode = BookieProtocol.EIO;
            try {
                if ((flags & BookieProtocol.FLAG_DO_FENCING) == BookieProtocol.FLAG_DO_FENCING) {
                    LOG.warn("Ledger " + ledgerId + " fenced by " + src.getPeerName());
                    bookie.fenceLedger(ledgerId);
                }
                rsp[1] = bookie.readEntry(ledgerId, entryId);
                LOG.debug("##### Read entry ##### " + rsp[1].remaining());
                errorCode = BookieProtocol.EOK;
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
        Cnxn src = (Cnxn) ctx;
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
    }

}
