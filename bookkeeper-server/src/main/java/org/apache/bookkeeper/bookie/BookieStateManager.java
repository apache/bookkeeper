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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A implementation of StateManager.
 */
public class BookieStateManager implements StateManager {
    private static final Logger LOG = LoggerFactory.getLogger(BookieStateManager.class);
    private final ServerConfiguration conf;
    private final LedgerDirsManager ledgerDirsManager;


    // use an executor to execute the state changes task
    final ExecutorService stateService = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("BookieStateManagerService-%d").build());

    // Running flag
    private volatile boolean running = false;
    // Flag identify whether it is in shutting down progress
    private volatile boolean shuttingdown = false;
    // Bookie status
    private final BookieStatus bookieStatus = new BookieStatus();
    private final AtomicBoolean rmRegistered = new AtomicBoolean(false);
    private final AtomicBoolean forceReadOnly = new AtomicBoolean(false);

    private final String bookieId;
    private ShutdownHandler shutdownHandler;
    private RegistrationManager registrationManager;


    public BookieStateManager(ServerConfiguration conf, RegistrationManager registrationManager,
                              LedgerDirsManager ledgerDirsManager) throws IOException {
        this.conf = conf;
        this.registrationManager = registrationManager;
        this.ledgerDirsManager = ledgerDirsManager;
        // ZK ephemeral node for this Bookie.
        this.bookieId = getMyId();
    }

    @Override
    public void initState(){
        if (forceReadOnly.get()) {
            this.bookieStatus.setToReadOnlyMode();
        } else if (conf.isPersistBookieStatusEnabled()) {
            this.bookieStatus.readFromDirectories(ledgerDirsManager.getAllLedgerDirs());
        }
        running = true;
    }

    @Override
    public void forceToShuttingDown(){
        // mark bookie as in shutting down progress
        shuttingdown = true;
    }

    @Override
    public void forceToReadOnly(){
        this.forceReadOnly.set(true);
    }

    @Override
    public void forceToShutDown(){
        this.running = false;
    }

    @Override
    public void forceToUnregistered(){
        this.rmRegistered.set(false);
    }

    // 1 : up, 0 : readonly, -1 : unregistered, used for stats
    @Override
    public int getState(){
        if (!rmRegistered.get()){
            return -1;
        } else if (forceReadOnly.get() || bookieStatus.isInReadOnlyMode()) {
            return 0;
        } else {
            return 1;
        }
    }

    @Override
    public boolean isReadOnly(){
        return forceReadOnly.get() || bookieStatus.isInReadOnlyMode();
    }

    @Override
    public boolean isRunning(){
        return running;
    }
    @Override
    public boolean isShuttingDown(){
        return shuttingdown;
    }

    @Override
    public void close() {
        stateService.shutdown();
    }

    @Override
    public Future<Void> registerBookie(final boolean throwException) {
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
                        shutdownHandler.shutdown(ExitCode.ZK_REG_FAIL);
                    }
                }
                return (Void) null;
            }
        });
    }

    @Override
    public void transitionToWritableMode() {
        stateService.execute(() -> doTransitionToWritableMode());
    }

    @Override
    public void transitionToReadOnlyMode() {
        stateService.execute(() -> doTransitionToReadOnlyMode());
    }

    void doRegisterBookie() throws IOException {
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
                    + " Shutting down bookie");
            shutdownHandler.shutdown(ExitCode.BOOKIE_EXCEPTION);
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
            shutdownHandler.shutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        }
    }
    public void setShutdownHandler(ShutdownHandler handler){
        shutdownHandler = handler;
    }

    @VisibleForTesting
    public void setRegistrationManager(RegistrationManager rm) {
        this.registrationManager = rm;
    }
    private String getMyId() throws UnknownHostException {
        return Bookie.getBookieAddress(conf).toString();
    }

}

