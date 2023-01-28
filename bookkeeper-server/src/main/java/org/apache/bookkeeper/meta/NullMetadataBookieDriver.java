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
package org.apache.bookkeeper.meta;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;

/**
 * A no-op implementation of MetadataBookieDriver.
 */
public class NullMetadataBookieDriver implements MetadataBookieDriver {
    @Override
    public MetadataBookieDriver initialize(ServerConfiguration conf,
            StatsLogger statsLogger) {
        return this;
    }

    @Override
    public String getScheme() {
        return "null";
    }

    @Override
    public RegistrationManager createRegistrationManager() {
        return new NullRegistrationManager();
    }

    @Override
    public LedgerManagerFactory getLedgerManagerFactory() {
        return new NullLedgerManagerFactory();
    }

    @Override
    public LayoutManager getLayoutManager() {
        return new NullLayoutManager();
    }

    @Override
    public void close() {}

    /**
     * A no-op implementation of LedgerManagerFactory.
     */
    public static class NullLedgerManagerFactory implements LedgerManagerFactory {
        @Override
        public int getCurrentVersion() {
            return 1;
        }
        @Override
        public LedgerManagerFactory initialize(AbstractConfiguration conf,
                                               LayoutManager layoutManager,
                                               int factoryVersion) {
            return this;
        }
        @Override
        public void close() {}
        @Override
        public LedgerIdGenerator newLedgerIdGenerator() {
            return new NullLedgerIdGenerator();
        }

        @Override
        public LedgerManager newLedgerManager() {
            return new NullLedgerManager();
        }

        @Override
        public LedgerUnderreplicationManager newLedgerUnderreplicationManager() {
            return new NullLedgerUnderreplicationManager();
        }

        @Override
        public LedgerAuditorManager newLedgerAuditorManager() throws IOException, InterruptedException {
            return new NullLedgerAuditorManager();
        }

        @Override
        public void format(AbstractConfiguration<?> conf, LayoutManager lm) {}
        @Override
        public boolean validateAndNukeExistingCluster(AbstractConfiguration<?> conf,
                                                      LayoutManager lm) {
            return false;
        }
    }

    /**
     * A no-op implementation of LedgerAuditorManager.
     */
    public static class NullLedgerAuditorManager implements LedgerAuditorManager {

        @Override
        public void tryToBecomeAuditor(String bookieId, Consumer<AuditorEvent> listener)
                throws IOException, InterruptedException {
            // no-op
        }

        @Override
        public BookieId getCurrentAuditor() throws IOException, InterruptedException {
            return BookieId.parse("127.0.0.1:3181");
        }

        @Override
        public void close() throws Exception {
            // no-op
        }
    }

    /**
     * A no-op implementation of LayoutManager.
     */
    public static class NullLayoutManager implements LayoutManager {
        @Override
        public LedgerLayout readLedgerLayout() {
            return new LedgerLayout("null", -1);
        }

        @Override
        public void storeLedgerLayout(LedgerLayout layout) { }

        @Override
        public void deleteLedgerLayout() { }
    }

    /**
     * A no-op implementation of RegistrationManager.
     */
    public static class NullRegistrationManager implements RegistrationManager {
        @Override
        public void close() {}

        @Override
        public String getClusterInstanceId() {
            return "null";
        }

        @Override
        public void registerBookie(BookieId bookieId, boolean readOnly, BookieServiceInfo bookieService) {}

        @Override
        public void unregisterBookie(BookieId bookieId, boolean readOnly) {}

        @Override
        public boolean isBookieRegistered(BookieId bookieId) {
            return false;
        }

        @Override
        public void writeCookie(BookieId bookieId, Versioned<byte[]> cookieData) throws BookieException {

        }

        @Override
        public Versioned<byte[]> readCookie(BookieId bookieId) throws BookieException {
            return null;
        }

        @Override
        public void removeCookie(BookieId bookieId, Version version) {}

        @Override
        public boolean prepareFormat() {
            return false;
        }

        @Override
        public boolean initNewCluster() {
            return false;
        }

        @Override
        public boolean format() {
            return false;
        }

        @Override
        public boolean nukeExistingCluster() {
            return false;
        }

        @Override
        public void addRegistrationListener(RegistrationListener listener) {}
    }

    /**
     * A no-op implementation of LedgerIdGenerator.
     */
    public static class NullLedgerIdGenerator implements LedgerIdGenerator {
        @Override
        public void close() {}
        @Override
        public void generateLedgerId(GenericCallback<Long> cb) {
            cb.operationComplete(BKException.Code.IllegalOpException, null);
        }
    }

    /**
     * A no-op implementation of LedgerManager.
     */
    public static class NullLedgerManager implements LedgerManager {
        private CompletableFuture<Versioned<LedgerMetadata>> illegalOp() {
            CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
            promise.completeExceptionally(new BKException.BKIllegalOpException());
            return promise;
        }

        @Override
        public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(long ledgerId,
                                                                                 LedgerMetadata metadata) {
            return illegalOp();
        }
        @Override
        public CompletableFuture<Void> removeLedgerMetadata(long ledgerId, Version version) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            promise.completeExceptionally(new BKException.BKIllegalOpException());
            return promise;
        }

        @Override
        public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
            return illegalOp();
        }

        @Override
        public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(
                long ledgerId, LedgerMetadata metadata, Version currentVersion) {
            return illegalOp();
        }

        @Override
        public void registerLedgerMetadataListener(long ledgerId,
                                                   LedgerMetadataListener listener) {}
        @Override
        public void unregisterLedgerMetadataListener(long ledgerId,
                                                     LedgerMetadataListener listener) {}
        @Override
        public void asyncProcessLedgers(Processor<Long> processor,
                                        AsyncCallback.VoidCallback finalCb,
                                        Object context, int successRc, int failureRc) {}
        @Override
        public LedgerManager.LedgerRangeIterator getLedgerRanges(long zkOpTimeOutMs) {
            return new LedgerManager.LedgerRangeIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }
                @Override
                public LedgerManager.LedgerRange next() {
                    throw new NoSuchElementException();
                }
            };
        }

        @Override
        public void close() {}
    }

    /**
     * A no-op implementation of LedgerUnderreplicationManager.
     */
    public static class NullLedgerUnderreplicationManager implements LedgerUnderreplicationManager {
        @Override
        public boolean isLedgerBeingReplicated(long ledgerId) throws ReplicationException {
            return false;
        }

        @Override
        public CompletableFuture<Void> markLedgerUnderreplicatedAsync(long ledgerId,
                                                                      Collection<String> missingReplicas) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            promise.completeExceptionally(new ReplicationException.UnavailableException("null"));
            return promise;
        }
        @Override
        public void markLedgerReplicated(long ledgerId)
                throws ReplicationException.UnavailableException {
            throw new ReplicationException.UnavailableException("null");
        }
        @Override
        public UnderreplicatedLedger getLedgerUnreplicationInfo(long ledgerId)
                throws ReplicationException.UnavailableException {
            throw new ReplicationException.UnavailableException("null");
        }
        @Override
        public Iterator<UnderreplicatedLedger> listLedgersToRereplicate(Predicate<List<String>> predicate) {
            return new Iterator<UnderreplicatedLedger>() {
                @Override
                public boolean hasNext() {
                    return false;
                }
                @Override
                public UnderreplicatedLedger next() {
                    throw new NoSuchElementException();
                }
            };
        }
        @Override
        public long getLedgerToRereplicate() throws ReplicationException.UnavailableException {
            throw new ReplicationException.UnavailableException("null");
        }
        @Override
        public long pollLedgerToRereplicate() throws ReplicationException.UnavailableException {
            throw new ReplicationException.UnavailableException("null");
        }

        @Override
        public void acquireUnderreplicatedLedger(long ledgerId) throws ReplicationException {
            // no-op
        }

        @Override
        public void releaseUnderreplicatedLedger(long ledgerId) {}
        @Override
        public void close() {}
        @Override
        public void disableLedgerReplication() {}
        @Override
        public void enableLedgerReplication() {}
        @Override
        public boolean isLedgerReplicationEnabled() {
            return false;
        }
        @Override
        public void notifyLedgerReplicationEnabled(GenericCallback<Void> cb) {}
        @Override
        public boolean initializeLostBookieRecoveryDelay(int lostBookieRecoveryDelay) {
            return false;
        }
        @Override
        public void setLostBookieRecoveryDelay(int lostBookieRecoveryDelay) {}
        @Override
        public int getLostBookieRecoveryDelay() {
            return Integer.MAX_VALUE;
        }
        @Override
        public void setCheckAllLedgersCTime(long checkAllLedgersCTime) {}
        @Override
        public long getCheckAllLedgersCTime() {
            return Integer.MAX_VALUE;
        }
        @Override
        public void setPlacementPolicyCheckCTime(long placementPolicyCheckCTime) {}
        @Override
        public long getPlacementPolicyCheckCTime() {
            return Long.MAX_VALUE;
        }
        @Override
        public void setReplicasCheckCTime(long replicasCheckCTime) {}
        @Override
        public long getReplicasCheckCTime() {
            return Long.MAX_VALUE;
        }
        @Override
        public void notifyLostBookieRecoveryDelayChanged(GenericCallback<Void> cb) {}
        @Override
        public String getReplicationWorkerIdRereplicatingLedger(long ledgerId)
                throws ReplicationException.UnavailableException {
            throw new ReplicationException.UnavailableException("null");
        }
        @Override
        public void notifyUnderReplicationLedgerChanged(GenericCallback<Void> cb) {}
    }
}
