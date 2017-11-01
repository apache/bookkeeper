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
package org.apache.bookkeeper.proto;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SERVER_SCOPE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.UnknownHostException;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieCriticalThread;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.bookie.ReadOnlyBookie;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.SecurityProviderFactoryFactory;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the server-side part of the BookKeeper protocol.
 *
 */
public class BookieServer {
    final ServerConfiguration conf;
    BookieNettyServer nettyServer;
    private volatile boolean running = false;
    Bookie bookie;
    DeathWatcher deathWatcher;
    private final static Logger LOG = LoggerFactory.getLogger(BookieServer.class);

    int exitCode = ExitCode.OK;

    // request processor
    private final RequestProcessor requestProcessor;

    // Expose Stats
    private final StatsLogger statsLogger;

    public BookieServer(ServerConfiguration conf) throws IOException,
            KeeperException, InterruptedException, BookieException,
            UnavailableException, CompatibilityException, SecurityException {
        this(conf, NullStatsLogger.INSTANCE);
    }

    public BookieServer(ServerConfiguration conf, StatsLogger statsLogger)
            throws IOException, KeeperException, InterruptedException,
            BookieException, UnavailableException, CompatibilityException, SecurityException {
        this.conf = conf;
        this.statsLogger = statsLogger;
        this.nettyServer = new BookieNettyServer(this.conf, null);
        try {
            this.bookie = newBookie(conf);
        } catch (IOException | KeeperException | InterruptedException | BookieException e) {
            // interrupted on constructing a bookie
            this.nettyServer.shutdown();
            throw e;
        }
        final SecurityHandlerFactory shFactory;

        shFactory = SecurityProviderFactoryFactory
                .getSecurityProviderFactory(conf.getTLSProviderFactoryClass());
        this.requestProcessor = new BookieRequestProcessor(conf, bookie,
                statsLogger.scope(SERVER_SCOPE), shFactory);
        this.nettyServer.setRequestProcessor(this.requestProcessor);
    }

    protected Bookie newBookie(ServerConfiguration conf)
        throws IOException, KeeperException, InterruptedException, BookieException {
        return conf.isForceReadOnlyBookie() ?
                new ReadOnlyBookie(conf, statsLogger.scope(BOOKIE_SCOPE)) :
                new Bookie(conf, statsLogger.scope(BOOKIE_SCOPE));
    }

    public void start() throws IOException, UnavailableException, InterruptedException, BKException {
        this.bookie.start();
        // fail fast, when bookie startup is not successful
        if (!this.bookie.isRunning()) {
            exitCode = bookie.getExitCode();
            return;
        }
        this.nettyServer.start();

        running = true;
        deathWatcher = new DeathWatcher(conf);
        deathWatcher.start();
    }

    @VisibleForTesting
    public BookieSocketAddress getLocalAddress() throws UnknownHostException {
        return Bookie.getBookieAddress(conf);
    }

    @VisibleForTesting
    public Bookie getBookie() {
        return bookie;
    }

    /**
     * Suspend processing of requests in the bookie (for testing)
     */
    @VisibleForTesting
    public void suspendProcessing() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Suspending bookie server, port is {}", conf.getBookiePort());
        }
        nettyServer.suspendProcessing();
    }

    /**
     * Resume processing requests in the bookie (for testing)
     */
    @VisibleForTesting
    public void resumeProcessing() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Resuming bookie server, port is {}", conf.getBookiePort());
        }
        nettyServer.resumeProcessing();
    }

    public synchronized void shutdown() {
        LOG.info("Shutting down BookieServer");
        this.nettyServer.shutdown();
        if (!running) {
            return;
        }
        exitCode = bookie.shutdown();
        this.requestProcessor.close();
        running = false;
    }

    public boolean isRunning() {
        return bookie.isRunning() && nettyServer.isRunning() && running;
    }

    /**
     * Whether bookie is running?
     *
     * @return true if bookie is running, otherwise return false
     */
    public boolean isBookieRunning() {
        return bookie.isRunning();
    }

    public void join() throws InterruptedException {
        bookie.join();
    }

    public int getExitCode() {
        return exitCode;
    }

    /**
     * A thread to watch whether bookie & nioserver is still alive
     */
    private class DeathWatcher extends BookieCriticalThread {

        private final int watchInterval;

        DeathWatcher(ServerConfiguration conf) {
            super("BookieDeathWatcher-" + conf.getBookiePort());
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
                if (!isBookieRunning()) {
                    shutdown();
                    break;
                }
            }
            LOG.info("BookieDeathWatcher exited loop!");
        }
    }

    /**
     * Legacy Method to run bookie server.
     */
    public static void main(String[] args) {
        Main.main(args);
    }

    @Override
    public  String toString() {
        String id = "UNKNOWN";

        try {
            id = Bookie.getBookieAddress(conf).toString();
        } catch (UnknownHostException e) {
            //Ignored...
        }
        return "Bookie Server listening on " + id;
    }
}
