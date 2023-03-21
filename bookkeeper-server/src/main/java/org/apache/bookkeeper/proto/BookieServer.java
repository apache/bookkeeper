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
package org.apache.bookkeeper.proto;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SERVER_SCOPE;
import static org.apache.bookkeeper.conf.AbstractConfiguration.PERMITTED_STARTUP_USERS;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieCriticalThread;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.bookie.UncleanShutdownDetection;
import org.apache.bookkeeper.common.util.JsonUtil.ParseJsonException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.server.Main;
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
    private final Bookie bookie;
    DeathWatcher deathWatcher;
    UncleanShutdownDetection uncleanShutdownDetection;
    private static final Logger LOG = LoggerFactory.getLogger(BookieServer.class);

    int exitCode = ExitCode.OK;

    // request processor
    private final RequestProcessor requestProcessor;

    // Expose Stats
    private final StatsLogger statsLogger;

    // Exception handler
    private volatile UncaughtExceptionHandler uncaughtExceptionHandler = null;

    public BookieServer(ServerConfiguration conf,
                        Bookie bookie,
                        StatsLogger statsLogger,
                        ByteBufAllocator allocator,
                        UncleanShutdownDetection uncleanShutdownDetection)
            throws IOException, KeeperException, InterruptedException,
            BookieException, UnavailableException, CompatibilityException, SecurityException {
        this.conf = conf;
        validateUser(conf);
        String configAsString;
        try {
            configAsString = conf.asJson();
            LOG.info(configAsString);
        } catch (ParseJsonException pe) {
            LOG.error("Got ParseJsonException while converting Config to JSONString", pe);
        }

        this.statsLogger = statsLogger;
        this.bookie = bookie;
        this.nettyServer = new BookieNettyServer(this.conf, null, allocator);
        this.uncleanShutdownDetection = uncleanShutdownDetection;

        final SecurityHandlerFactory shFactory;

        shFactory = SecurityProviderFactoryFactory
                .getSecurityProviderFactory(conf.getTLSProviderFactoryClass());

        this.requestProcessor = new BookieRequestProcessor(conf, bookie,
                statsLogger.scope(SERVER_SCOPE), shFactory, allocator, nettyServer.allChannels);
        this.nettyServer.setRequestProcessor(this.requestProcessor);
    }

    /**
     * Currently the uncaught exception handler is used for DeathWatcher to notify
     * lifecycle management that a bookie is dead for some reasons.
     *
     * <p>in future, we can register this <tt>exceptionHandler</tt> to critical threads
     * so when those threads are dead, it will automatically trigger lifecycle management
     * to shutdown the process.
     */
    public void setExceptionHandler(UncaughtExceptionHandler exceptionHandler) {
        this.uncaughtExceptionHandler = exceptionHandler;
    }

    public void start() throws InterruptedException, IOException {
        this.bookie.start();

        // fail fast, when bookie startup is not successful
        if (!this.bookie.isRunning()) {
            exitCode = bookie.getExitCode();
            this.requestProcessor.close();
            return;
        }

        this.uncleanShutdownDetection.registerStartUp();
        this.nettyServer.start();

        running = true;
        deathWatcher = new DeathWatcher(conf);
        if (null != uncaughtExceptionHandler) {
            deathWatcher.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        deathWatcher.start();

        // fixes test flappers at random places until ISSUE#1400 is resolved
        // https://github.com/apache/bookkeeper/issues/1400
        TimeUnit.MILLISECONDS.sleep(250);
    }

    @VisibleForTesting
    public BookieSocketAddress getLocalAddress() throws UnknownHostException {
        return BookieImpl.getBookieAddress(conf);
    }

    @VisibleForTesting
    public BookieId getBookieId() throws UnknownHostException {
        return BookieImpl.getBookieId(conf);
    }

    @VisibleForTesting
    public Bookie getBookie() {
        return bookie;
    }

    @VisibleForTesting
    public BookieRequestProcessor getBookieRequestProcessor() {
        return (BookieRequestProcessor) requestProcessor;
    }

    /**
     * Suspend processing of requests in the bookie (for testing).
     */
    @VisibleForTesting
    public void suspendProcessing() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Suspending bookie server, port is {}", conf.getBookiePort());
        }
        nettyServer.suspendProcessing();
    }

    /**
     * Resume processing requests in the bookie (for testing).
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
        this.requestProcessor.close();
        exitCode = bookie.shutdown();
        uncleanShutdownDetection.registerCleanShutdown();
        running = false;
    }

    /**
     * Ensure the current user can start-up the process if it's restricted.
     */
    private void validateUser(ServerConfiguration conf) throws BookieException {
        if (conf.containsKey(PERMITTED_STARTUP_USERS)) {
            String currentUser = System.getProperty("user.name");
            String[] propertyValue = conf.getPermittedStartupUsers();
            for (String s : propertyValue) {
                if (s.equals(currentUser)) {
                    return;
                }
            }
            String errorMsg =
                    "System cannot start because current user isn't in permittedStartupUsers."
                            + " Current user: " + currentUser + " permittedStartupUsers: "
                            + Arrays.toString(propertyValue);
            LOG.error(errorMsg);
            throw new BookieException.BookieUnauthorizedAccessException(errorMsg);
        }
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
     * A thread to watch whether bookie and nioserver are still alive.
     */
    private class DeathWatcher extends BookieCriticalThread {

        private final int watchInterval;

        DeathWatcher(ServerConfiguration conf) {
            super("BookieDeathWatcher-" + conf.getBookiePort());
            watchInterval = conf.getDeathWatchInterval();
            // set a default uncaught exception handler to shutdown the bookie server
            // when it notices the bookie is not running any more.
            setUncaughtExceptionHandler((thread, cause) -> {
                LOG.info("BookieDeathWatcher exited loop due to uncaught exception from thread {}",
                    thread.getName(), cause);
                shutdown();
            });
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(watchInterval);
                } catch (InterruptedException ie) {
                    // do nothing
                    Thread.currentThread().interrupt();
                }
                if (!isBookieRunning()) {
                    LOG.info("BookieDeathWatcher noticed the bookie is not running any more, exiting the watch loop!");
                    // death watcher has noticed that bookie is not running any more
                    // throw an exception to fail the death watcher thread and it will
                    // trigger the uncaught exception handler to handle this "bookie not running" situation.
                    throw new RuntimeException("Bookie is not running any more");
                }
            }
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
        String addr = "UNKNOWN";
        String id = "?";
        try {
            addr = BookieImpl.getBookieAddress(conf).toString();
            id = getBookieId().toString();
        } catch (UnknownHostException e) {
            //Ignored...
        }
        return "Bookie Server listening on " + addr + " with id " + id;
    }
}
