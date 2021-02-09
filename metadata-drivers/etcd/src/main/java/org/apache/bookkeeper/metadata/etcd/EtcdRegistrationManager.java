/*
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
 */
package org.apache.bookkeeper.metadata.etcd;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getBookiesEndPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getBookiesPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getBucketsPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getClusterInstanceIdPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getCookiePath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getCookiesPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getLayoutKey;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getLedgersPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getReadonlyBookiePath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getReadonlyBookiesPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getScopeEndKey;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getUnderreplicationPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getWritableBookiePath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getWritableBookiesPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.msResult;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.Cmp.Op;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchEvent.EventType;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.BookieIllegalOpException;
import org.apache.bookkeeper.bookie.BookieException.CookieNotFoundException;
import org.apache.bookkeeper.bookie.BookieException.MetadataStoreException;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerLayout;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Etcd registration manager.
 */
@Slf4j
class EtcdRegistrationManager implements RegistrationManager {

    private final String scope;
    @Getter(AccessLevel.PACKAGE)
    private final Client client;
    private final boolean ownClient;
    private final KV kvClient;
    @Getter(AccessLevel.PACKAGE)
    private final EtcdBookieRegister bkRegister;

    @VisibleForTesting
    EtcdRegistrationManager(Client client,
                            String scope) {
        this(client, scope, 60);
    }

    @VisibleForTesting
    EtcdRegistrationManager(Client client,
                            String scope,
                            long ttlSeconds) {
        this(client, scope, ttlSeconds, () -> {});
    }

    @VisibleForTesting
    EtcdRegistrationManager(Client client,
                            String scope,
                            long ttlSeconds,
                            RegistrationListener listener) {
        this(
            client,
            scope,
            new EtcdBookieRegister(
                client.getLeaseClient(),
                ttlSeconds,
                listener
            ).start(),
            true);
    }

    EtcdRegistrationManager(Client client,
                            String scope,
                            EtcdBookieRegister bkRegister) {
        this(client, scope, bkRegister, false);
    }

    private EtcdRegistrationManager(Client client,
                                    String scope,
                                    EtcdBookieRegister bkRegister,
                                    boolean ownClient) {
        this.scope = scope;
        this.client = client;
        this.kvClient = client.getKVClient();
        this.bkRegister = bkRegister;
        this.ownClient = ownClient;
    }

    @Override
    public void close() {
        if (ownClient) {
            log.info("Closing registration manager under scope '{}'", scope);
            bkRegister.close();
            client.close();
            log.info("Successfully closed registration manager under scope '{}'", scope);
        }
    }

    @Override
    public void registerBookie(BookieId bookieId, boolean readOnly,
                               BookieServiceInfo bookieServiceInfo) throws BookieException {
        if (readOnly) {
            doRegisterReadonlyBookie(bookieId, bkRegister.get());
        } else {
            doRegisterBookie(getWritableBookiePath(scope, bookieId), bkRegister.get());
        }
    }

    private boolean checkRegNodeAndWaitExpired(String regPath, long leaseId)
            throws MetadataStoreException {
        ByteSequence regPathBs = ByteSequence.from(regPath, UTF_8);
        GetResponse getResp = msResult(kvClient.get(regPathBs));
        if (getResp.getCount() <= 0) {
            // key doesn't exist anymore
            return false;
        } else {
            return waitUntilRegNodeExpired(regPath, leaseId);
        }
    }

    private boolean waitUntilRegNodeExpired(String regPath, long leaseId)
            throws MetadataStoreException {
        ByteSequence regPathBs = ByteSequence.from(regPath, UTF_8);
        // check regPath again
        GetResponse getResp = msResult(kvClient.get(regPathBs));
        if (getResp.getCount() <= 0) {
            // key disappears after watching it
            return false;
        } else {
            KeyValue kv = getResp.getKvs().get(0);
            if (kv.getLease() != leaseId) {
                Watch watchClient = client.getWatchClient();
                final CompletableFuture<Void> watchFuture = new CompletableFuture<>();
                Watcher watcher = watchClient.watch(
                    regPathBs,
                    WatchOption.newBuilder()
                        .withRevision(getResp.getHeader().getRevision() + 1)
                        .build(),
                    response -> {
                        for (WatchEvent event : response.getEvents()) {
                            log.info("Received watch event on '{}' : EventType = {}, lease {}",
                                regPath, event.getEventType(), leaseId);
                            if (EventType.DELETE == event.getEventType()) {
                                watchFuture.complete(null);
                                return;
                            }
                        }
                    },
                    exception -> {
                        log.warn("Exception in keepAlive for watch event on {}, lease {}",
                                regPath, leaseId, exception);
                        watchFuture.completeExceptionally(new UncheckedExecutionException(
                                    "Interrupted at waiting previous registration under "
                                    + regPath + " (lease = " + kv.getLease() + ") to be expired", exception));
                    }
                );
                log.info("Previous bookie registration (lease = {}) still exists at {}, "
                                + "so new lease '{}' will be waiting previous lease for {} seconds to be expired",
                        kv.getLease(), regPath, leaseId, bkRegister.getTtlSeconds());

                try {
                    msResult(watchFuture, 2 * bkRegister.getTtlSeconds(), TimeUnit.SECONDS);
                    return false;
                } catch (TimeoutException e) {
                    watchFuture.cancel(true);
                    throw new MetadataStoreException(
                        "Previous bookie registration still exists at "
                            + regPath + " (lease = " + kv.getLease() + ") after "
                            + (2 * bkRegister.getTtlSeconds()) + " seconds elapsed");
                } catch (UncheckedExecutionException uee) {
                    throw new MetadataStoreException(uee.getMessage(), uee.getCause());
                } finally {
                    watcher.close();
                }
            } else {
                // key exists with same lease
                return true;
            }
        }
    }

    private void doRegisterBookie(String regPath, long leaseId) throws MetadataStoreException {
        if (checkRegNodeAndWaitExpired(regPath, leaseId)) {
            // the bookie is already registered under `${regPath}` with `${leaseId}`.
            return;
        }

        ByteSequence regPathBs = ByteSequence.from(regPath, UTF_8);
        Txn txn = kvClient.txn()
            .If(new Cmp(
                regPathBs,
                Op.GREATER,
                CmpTarget.createRevision(0)))
            .Then(io.etcd.jetcd.op.Op.get(regPathBs, GetOption.DEFAULT))
            .Else(io.etcd.jetcd.op.Op.put(
                regPathBs,
                ByteSequence.from(new byte[0]),
                PutOption.newBuilder()
                    .withLeaseId(bkRegister.get())
                    .build()
            ));
        TxnResponse txnResp = msResult(txn.commit());
        if (txnResp.isSucceeded()) {
            // the key already exists
            GetResponse getResp = txnResp.getGetResponses().get(0);
            if (getResp.getCount() <= 0) {
                throw new MetadataStoreException(
                    "Failed to register bookie under '" + regPath
                        + "', but no bookie is registered there.");
            } else {
                KeyValue kv = getResp.getKvs().get(0);
                throw new MetadataStoreException("Another bookie already registered under '"
                    + regPath + "': lease = " + kv.getLease());
            }
        } else {
            log.info("Successfully registered bookie at {}", regPath);
        }
    }

    private void doRegisterReadonlyBookie(BookieId bookieId, long leaseId) throws MetadataStoreException {
        String readonlyRegPath = getReadonlyBookiePath(scope, bookieId);
        doRegisterBookie(readonlyRegPath, leaseId);
        String writableRegPath = getWritableBookiePath(scope, bookieId);
        msResult(kvClient.delete(ByteSequence.from(writableRegPath, UTF_8)));
    }

    @Override
    public void unregisterBookie(BookieId bookieId, boolean readOnly) throws BookieException {
        String regPath;
        if (readOnly) {
            regPath = getReadonlyBookiePath(scope, bookieId);
        } else {
            regPath = getWritableBookiePath(scope, bookieId);
        }
        DeleteResponse delResp = msResult(kvClient.delete(ByteSequence.from(regPath, UTF_8)));
        if (delResp.getDeleted() > 0) {
            log.info("Successfully unregistered bookie {} from {}", bookieId, regPath);
        } else {
            log.info("Bookie disappeared from {} before unregistering", regPath);
        }
    }

    @Override
    public boolean isBookieRegistered(BookieId bookieId) throws BookieException {
        CompletableFuture<GetResponse> getWritableFuture = kvClient.get(
            ByteSequence.from(getWritableBookiePath(scope, bookieId), UTF_8),
            GetOption.newBuilder()
                .withCountOnly(true)
                .build());
        CompletableFuture<GetResponse> getReadonlyFuture = kvClient.get(
            ByteSequence.from(getReadonlyBookiePath(scope, bookieId), UTF_8),
            GetOption.newBuilder()
                .withCountOnly(true)
                .build());

        return msResult(getWritableFuture).getCount() > 0
            || msResult(getReadonlyFuture).getCount() > 0;
    }

    @Override
    public void writeCookie(BookieId bookieId, Versioned<byte[]> cookieData) throws BookieException {
        ByteSequence cookiePath = ByteSequence.from(getCookiePath(scope, bookieId), UTF_8);
        Txn txn = kvClient.txn();
        if (Version.NEW == cookieData.getVersion()) {
            txn.If(new Cmp(
                cookiePath,
                Op.GREATER,
                CmpTarget.createRevision(0L))
            )
            // if key not exists, create one.
            .Else(io.etcd.jetcd.op.Op.put(
                cookiePath,
                ByteSequence.from(cookieData.getValue()),
                PutOption.DEFAULT)
            );
        } else {
            if (!(cookieData.getVersion() instanceof LongVersion)) {
                throw new BookieIllegalOpException("Invalid version type, expected it to be LongVersion");
            }
            txn.If(new Cmp(
                cookiePath,
                Op.EQUAL,
                CmpTarget.modRevision(((LongVersion) cookieData.getVersion()).getLongVersion()))
            )
            .Then(io.etcd.jetcd.op.Op.put(
                cookiePath,
                ByteSequence.from(cookieData.getValue()),
                PutOption.DEFAULT)
            );
        }
        TxnResponse response = msResult(txn.commit());
        if (response.isSucceeded() != (Version.NEW != cookieData.getVersion())) {
            throw new MetadataStoreException(
                "Conflict on writing cookie for bookie " + bookieId);
        }
    }

    @Override
    public Versioned<byte[]> readCookie(BookieId bookieId) throws BookieException {
        ByteSequence cookiePath = ByteSequence.from(getCookiePath(scope, bookieId), UTF_8);
        GetResponse resp = msResult(kvClient.get(cookiePath));
        if (resp.getCount() <= 0) {
            throw new CookieNotFoundException(bookieId.toString());
        } else {
            KeyValue kv = resp.getKvs().get(0);
            return new Versioned<>(
                kv.getValue().getBytes(),
                new LongVersion(kv.getModRevision()));
        }
    }

    @Override
    public void removeCookie(BookieId bookieId, Version version) throws BookieException {
        ByteSequence cookiePath = ByteSequence.from(getCookiePath(scope, bookieId), UTF_8);
        Txn delTxn = kvClient.txn()
            .If(new Cmp(
                cookiePath,
                Op.EQUAL,
                CmpTarget.modRevision(((LongVersion) version).getLongVersion())
            ))
            .Then(io.etcd.jetcd.op.Op.delete(
                cookiePath,
                DeleteOption.DEFAULT
            ))
            .Else(io.etcd.jetcd.op.Op.get(
                cookiePath,
                GetOption.newBuilder().withCountOnly(true).build()
            ));
        TxnResponse txnResp = msResult(delTxn.commit());
        if (!txnResp.isSucceeded()) {
            GetResponse getResp = txnResp.getGetResponses().get(0);
            if (getResp.getCount() > 0) {
                throw new MetadataStoreException(
                    "Failed to remove cookie from " + cookiePath.toString(UTF_8)
                        + " for bookie " + bookieId + " : bad version '" + version + "'");
            } else {
                throw new CookieNotFoundException(bookieId.toString());
            }
        } else {
            log.info("Removed cookie from {} for bookie {}",
                cookiePath.toString(UTF_8), bookieId);
        }
    }

    @Override
    public String getClusterInstanceId() throws BookieException {
        GetResponse response = msResult(
            kvClient.get(ByteSequence.from(getClusterInstanceIdPath(scope), UTF_8)));
        if (response.getCount() <= 0) {
            log.error("BookKeeper metadata doesn't exist in Etcd. "
                + "Has the cluster been initialized? "
                + "Try running bin/bookkeeper shell initNewCluster");
            throw new MetadataStoreException("BookKeeper is not initialized under '" + scope + "' yet");
        } else {
            KeyValue kv = response.getKvs().get(0);
            return new String(kv.getValue().getBytes(), UTF_8);
        }
    }

    @Override
    public boolean prepareFormat() throws Exception {
        ByteSequence rootScopeKey = ByteSequence.from(scope, UTF_8);
        GetResponse resp = msResult(kvClient.get(rootScopeKey));
        return resp.getCount() > 0;
    }

    @Override
    public boolean initNewCluster() throws Exception {
        return initNewCluster(kvClient, scope);
    }

    static boolean initNewCluster(KV kvClient, String scope) throws Exception {
        ByteSequence rootScopeKey = ByteSequence.from(scope, UTF_8);
        String instanceId = UUID.randomUUID().toString();
        LedgerLayout layout = new LedgerLayout(
            EtcdLedgerManagerFactory.class.getName(),
            EtcdLedgerManagerFactory.VERSION
        );
        Txn initTxn = kvClient.txn()
            .If(new Cmp(
                rootScopeKey,
                Op.GREATER,
                CmpTarget.createRevision(0L)
            ))
            // only put keys when root scope doesn't exist
            .Else(
                // `${scope}`
                io.etcd.jetcd.op.Op.put(
                    rootScopeKey,
                    EtcdConstants.EMPTY_BS,
                    PutOption.DEFAULT
                ),
                // `${scope}/layout`
                io.etcd.jetcd.op.Op.put(
                    ByteSequence.from(getLayoutKey(scope), UTF_8),
                    ByteSequence.from(layout.serialize()),
                    PutOption.DEFAULT
                ),
                // `${scope}/instanceid`
                io.etcd.jetcd.op.Op.put(
                    ByteSequence.from(getClusterInstanceIdPath(scope), UTF_8),
                    ByteSequence.from(instanceId, UTF_8),
                    PutOption.DEFAULT
                ),
                // `${scope}/cookies`
                io.etcd.jetcd.op.Op.put(
                    ByteSequence.from(getCookiesPath(scope), UTF_8),
                    EtcdConstants.EMPTY_BS,
                    PutOption.DEFAULT
                ),
                // `${scope}/bookies`
                io.etcd.jetcd.op.Op.put(
                    ByteSequence.from(getBookiesPath(scope), UTF_8),
                    EtcdConstants.EMPTY_BS,
                    PutOption.DEFAULT
                ),
                // `${scope}/bookies/writable`
                io.etcd.jetcd.op.Op.put(
                    ByteSequence.from(getWritableBookiesPath(scope), UTF_8),
                    EtcdConstants.EMPTY_BS,
                    PutOption.DEFAULT
                ),
                // `${scope}/bookies/readonly`
                io.etcd.jetcd.op.Op.put(
                    ByteSequence.from(getReadonlyBookiesPath(scope), UTF_8),
                    EtcdConstants.EMPTY_BS,
                    PutOption.DEFAULT
                ),
                // `${scope}/ledgers`
                io.etcd.jetcd.op.Op.put(
                    ByteSequence.from(getLedgersPath(scope), UTF_8),
                    EtcdConstants.EMPTY_BS,
                    PutOption.DEFAULT
                ),
                // `${scope}/buckets`
                io.etcd.jetcd.op.Op.put(
                    ByteSequence.from(getBucketsPath(scope), UTF_8),
                    EtcdConstants.EMPTY_BS,
                    PutOption.DEFAULT
                ),
                // `${scope}/underreplication`
                io.etcd.jetcd.op.Op.put(
                    ByteSequence.from(getUnderreplicationPath(scope), UTF_8),
                    EtcdConstants.EMPTY_BS,
                    PutOption.DEFAULT
                )
            );

        return !msResult(initTxn.commit()).isSucceeded();
    }

    @Override
    public boolean format() throws Exception {
        return format(kvClient, scope);
    }

    static boolean format(KV kvClient, String scope) throws Exception {
        ByteSequence rootScopeKey = ByteSequence.from(scope, UTF_8);
        GetResponse resp = msResult(kvClient.get(rootScopeKey));
        if (resp.getCount() <= 0) {
            // cluster doesn't exist
            return initNewCluster(kvClient, scope);
        } else if (nukeExistingCluster(kvClient, scope)) { // cluster exists and has successfully nuked it
            return initNewCluster(kvClient, scope);
        } else {
            return false;
        }
    }

    @Override
    public boolean nukeExistingCluster() throws Exception {
        return nukeExistingCluster(kvClient, scope);
    }

    static boolean nukeExistingCluster(KV kvClient, String scope) throws Exception {
        ByteSequence rootScopeKey = ByteSequence.from(scope, UTF_8);
        GetResponse resp = msResult(kvClient.get(rootScopeKey));
        if (resp.getCount() <= 0) {
            log.info("There is no existing cluster with under scope '{}' in Etcd, "
                + "so exiting nuke operation", scope);
            return true;
        }

        String bookiesPath = getBookiesPath(scope);
        String bookiesEndPath = getBookiesEndPath(scope);
        resp = msResult(kvClient.get(
            ByteSequence.from(bookiesPath, UTF_8),
            GetOption.newBuilder()
                .withRange(ByteSequence.from(bookiesEndPath, UTF_8))
                .withKeysOnly(true)
                .build()
        ));
        String writableBookiesPath = getWritableBookiesPath(scope);
        String readonlyBookiesPath = getReadonlyBookiesPath(scope);
        boolean hasBookiesAlive = false;
        for (KeyValue kv : resp.getKvs()) {
            String keyStr = new String(kv.getKey().getBytes(), UTF_8);
            if (keyStr.equals(bookiesPath)
                || keyStr.equals(writableBookiesPath)
                || keyStr.equals(readonlyBookiesPath)) {
                continue;
            } else {
                hasBookiesAlive = true;
                break;
            }
        }
        if (hasBookiesAlive) {
            log.error("Bookies are still up and connected to this cluster, "
                + "stop all bookies before nuking the cluster");
            return false;
        }
        DeleteResponse delResp = msResult(kvClient.delete(
            rootScopeKey,
            DeleteOption.newBuilder()
                .withRange(ByteSequence.from(getScopeEndKey(scope), UTF_8))
                .build()));
        log.info("Successfully nuked cluster under scope '{}' : {} kv pairs deleted",
            scope, delResp.getDeleted());
        return true;
    }
}
