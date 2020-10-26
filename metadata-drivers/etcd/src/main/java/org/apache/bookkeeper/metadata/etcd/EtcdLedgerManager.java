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

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.Txn;
import com.coreos.jetcd.common.exception.ClosedClientException;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.Cmp.Op;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.options.DeleteOption;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.metadata.etcd.helpers.KeyIterator;
import org.apache.bookkeeper.metadata.etcd.helpers.KeyStream;
import org.apache.bookkeeper.metadata.etcd.helpers.ValueStream;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback.VoidCallback;

/**
 * Etcd ledger manager.
 */
@Slf4j
class EtcdLedgerManager implements LedgerManager {

    private final LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
    private final Function<ByteSequence, LedgerMetadata> ledgerMetadataFunction = bs -> {
        try {
            return serDe.parseConfig(
                bs.getBytes(),
                Optional.empty()
            );
        } catch (IOException ioe) {
            log.error("Could not parse ledger metadata : {}", bs.toStringUtf8(), ioe);
            throw new RuntimeException(
                "Could not parse ledger metadata : " + bs.toStringUtf8(), ioe);
        }
    };

    private final String scope;
    private final Client client;
    private final KV kvClient;
    private final EtcdWatchClient watchClient;
    private final ConcurrentLongHashMap<ValueStream<LedgerMetadata>> watchers =
        new ConcurrentLongHashMap<>();
    private final ConcurrentMap<LedgerMetadataListener, LedgerMetadataConsumer> listeners =
        new ConcurrentHashMap<>();

    private volatile boolean closed = false;

    EtcdLedgerManager(Client client,
                      String scope) {
        this.client = client;
        this.kvClient = client.getKVClient();
        this.scope = scope;
        this.watchClient = new EtcdWatchClient(client);
    }

    private boolean isClosed() {
        return closed;
    }

    ValueStream<LedgerMetadata> getLedgerMetadataStream(long ledgerId) {
        return watchers.get(ledgerId);
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(long ledgerId,
                                                                             LedgerMetadata metadata) {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
        String ledgerKey = EtcdUtils.getLedgerKey(scope, ledgerId);
        log.info("Create ledger metadata under key {}", ledgerKey);

        ByteSequence ledgerKeyBs = ByteSequence.fromString(ledgerKey);
        final ByteSequence valueBs;
        try {
            valueBs = ByteSequence.fromBytes(serDe.serialize(metadata));
        } catch (IOException ioe) {
            promise.completeExceptionally(new BKException.BKMetadataSerializationException(ioe));
            return promise;
        }
        kvClient.txn()
            .If(new Cmp(
                ledgerKeyBs,
                Op.GREATER,
                CmpTarget.createRevision(0L)))
            .Then(com.coreos.jetcd.op.Op.get(
                ledgerKeyBs,
                GetOption.newBuilder()
                    .withCountOnly(true)
                    .build()))
            .Else(com.coreos.jetcd.op.Op.put(
                ledgerKeyBs,
                valueBs,
                PutOption.DEFAULT))
            .commit()
            .thenAccept(resp -> {
                if (resp.isSucceeded()) {
                    GetResponse getResp = resp.getGetResponses().get(0);
                    if (getResp.getCount() <= 0) {
                        // key doesn't exist but we fail to put the key
                        promise.completeExceptionally(new BKException.BKUnexpectedConditionException());
                    } else {
                        // key exists
                        promise.completeExceptionally(new BKException.BKLedgerExistException());
                    }
                } else {
                    promise.complete(new Versioned<>(metadata,
                                                     new LongVersion(resp.getHeader().getRevision())));
                }
            })
            .exceptionally(cause -> {
                    promise.completeExceptionally(new BKException.MetaStoreException());
                    return null;
                });
        return promise;
    }

    @Override
    public CompletableFuture<Void> removeLedgerMetadata(long ledgerId, Version version) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        long revision = -0xabcd;
        if (Version.NEW == version) {
            log.error("Request to delete ledger {} metadata with version set to the initial one", ledgerId);
            promise.completeExceptionally(new BKException.BKMetadataVersionException());
            return promise;
        } else if (Version.ANY != version) {
            if (!(version instanceof LongVersion)) {
                log.info("Not an instance of LongVersion : {}", ledgerId);
                promise.completeExceptionally(new BKException.BKMetadataVersionException());
                return promise;
            } else {
                revision = ((LongVersion) version).getLongVersion();
            }
        }

        String ledgerKey = EtcdUtils.getLedgerKey(scope, ledgerId);
        ByteSequence ledgerKeyBs = ByteSequence.fromString(ledgerKey);
        Txn txn = kvClient.txn();
        if (revision == -0xabcd) {
            txn = txn.If(new Cmp(
                ledgerKeyBs,
                Op.GREATER,
                CmpTarget.createRevision(0L)
            ));
        } else {
            txn = txn.If(new Cmp(
                ledgerKeyBs,
                Op.EQUAL,
                CmpTarget.modRevision(revision)
            ));
        }
        txn
            .Then(com.coreos.jetcd.op.Op.delete(
                ledgerKeyBs,
                DeleteOption.DEFAULT
            ))
            .Else(com.coreos.jetcd.op.Op.get(
                ledgerKeyBs,
                GetOption.DEFAULT
            ))
            .commit()
            .thenAccept(txnResp -> {
                if (txnResp.isSucceeded()) {
                    promise.complete(null);
                } else {
                    GetResponse getResp = txnResp.getGetResponses().get(0);
                    if (getResp.getCount() > 0) {
                        // fail to delete the ledger
                        promise.completeExceptionally(new BKException.BKMetadataVersionException());
                    } else {
                        log.warn("Deleting ledger {} failed due to : ledger key {} doesn't exist", ledgerId, ledgerKey);
                        promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
                    }
                }
            })
            .exceptionally(cause -> {
                    promise.completeExceptionally(new BKException.MetaStoreException());
                    return null;
                });
        return promise;
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
        String ledgerKey = EtcdUtils.getLedgerKey(scope, ledgerId);
        ByteSequence ledgerKeyBs = ByteSequence.fromString(ledgerKey);
        log.info("read ledger metadata under key {}", ledgerKey);
        kvClient.get(ledgerKeyBs)
            .thenAccept(getResp -> {
                if (getResp.getCount() > 0) {
                    KeyValue kv = getResp.getKvs().get(0);
                    byte[] data = kv.getValue().getBytes();
                    try {
                        LedgerMetadata metadata = serDe.parseConfig(data, Optional.empty());
                        promise.complete(new Versioned<>(metadata, new LongVersion(kv.getModRevision())));
                    } catch (IOException ioe) {
                        log.error("Could not parse ledger metadata for ledger : {}", ledgerId, ioe);
                        promise.completeExceptionally(new BKException.MetaStoreException());
                        return;
                    }
                } else {
                    promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
                }
            })
            .exceptionally(cause -> {
                    promise.completeExceptionally(new BKException.MetaStoreException());
                    return null;
                });
        return promise;
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                                                                            Version currentVersion) {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
        if (Version.NEW == currentVersion || !(currentVersion instanceof LongVersion)) {
            promise.completeExceptionally(new BKException.BKMetadataVersionException());
            return promise;
        }
        final LongVersion lv = (LongVersion) currentVersion;
        String ledgerKey = EtcdUtils.getLedgerKey(scope, ledgerId);
        ByteSequence ledgerKeyBs = ByteSequence.fromString(ledgerKey);

        final ByteSequence valueBs;
        try {
            valueBs = ByteSequence.fromBytes(serDe.serialize(metadata));
        } catch (IOException ioe) {
            promise.completeExceptionally(new BKException.BKMetadataSerializationException(ioe));
            return promise;
        }

        kvClient.txn()
            .If(new Cmp(
                ledgerKeyBs,
                Op.EQUAL,
                CmpTarget.modRevision(lv.getLongVersion())))
            .Then(com.coreos.jetcd.op.Op.put(
                ledgerKeyBs,
                valueBs,
                PutOption.DEFAULT))
            .Else(com.coreos.jetcd.op.Op.get(
                ledgerKeyBs,
                GetOption.DEFAULT))
            .commit()
            .thenAccept(resp -> {
                if (resp.isSucceeded()) {
                    promise.complete(new Versioned<>(metadata, new LongVersion(resp.getHeader().getRevision())));
                } else {
                    GetResponse getResp = resp.getGetResponses().get(0);
                    if (getResp.getCount() > 0) {
                        log.warn("Conditional update ledger metadata failed :"
                            + " expected version = {}, actual version = {}",
                            getResp.getKvs().get(0).getModRevision(), lv);
                        promise.completeExceptionally(new BKException.BKMetadataVersionException());
                    } else {
                        promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
                    }
                }
            })
            .exceptionally(cause -> {
                    promise.completeExceptionally(new BKException.MetaStoreException());
                    return null;
                });
        return promise;
    }

    private LedgerMetadataConsumer listenerToConsumer(long ledgerId,
                                                      LedgerMetadataListener listener,
                                                      Consumer<Long> onDeletedConsumer) {
        return new LedgerMetadataConsumer(
            ledgerId,
            listener,
            onDeletedConsumer
        );
    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        if (listeners.containsKey(listener)) {
            return;
        }

        ValueStream<LedgerMetadata> lmStream = watchers.computeIfAbsent(
            ledgerId, (lid) -> new ValueStream<>(
                client,
                watchClient,
                ledgerMetadataFunction,
                ByteSequence.fromString(EtcdUtils.getLedgerKey(scope, ledgerId)))
        );
        LedgerMetadataConsumer lmConsumer = listenerToConsumer(ledgerId, listener,
            (lid) -> {
                if (watchers.remove(lid, lmStream)) {
                    log.info("Closed ledger metadata watcher on ledger {} deletion.", lid);
                    lmStream.closeAsync();
                }
            });
        LedgerMetadataConsumer oldConsumer = listeners.putIfAbsent(listener, lmConsumer);
        if (null != oldConsumer) {
            return;
        } else {
            lmStream.readAndWatch(lmConsumer)
                .whenComplete((values, cause) -> {
                    if (null != cause && !(cause instanceof ClosedClientException)) {
                        // fail to register ledger metadata listener, re-attempt it
                        registerLedgerMetadataListener(ledgerId, listener);
                    }
                });
        }
    }

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        LedgerMetadataConsumer lmConsumer = listeners.remove(listener);
        unregisterLedgerMetadataListener(ledgerId, lmConsumer);
    }

    private void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataConsumer lmConsumer) {
        ValueStream<LedgerMetadata> lmStream = watchers.get(ledgerId);
        if (null == lmStream) {
            return;
        } else {
            lmStream.unwatch(lmConsumer).thenAccept(noConsumers -> {
                if (noConsumers) {
                    if (watchers.remove(ledgerId, lmStream)) {
                        log.info("Closed ledger metadata watcher on ledger {} since there are no listeners any more.",
                            ledgerId);
                        lmStream.closeAsync();
                    }
                }
            }).exceptionally(cause -> {
                if (cause instanceof ClosedClientException) {
                    // fail to unwatch a consumer
                    unregisterLedgerMetadataListener(ledgerId, lmConsumer);
                }
                return null;
            });
        }
    }

    @Override
    public void asyncProcessLedgers(Processor<Long> processor,
                                    VoidCallback finalCb,
                                    Object context,
                                    int successRc,
                                    int failureRc) {
        KeyStream<Long> ks = new KeyStream<>(
            kvClient,
            ByteSequence.fromString(EtcdUtils.getLedgerKey(scope, 0L)),
            ByteSequence.fromString(EtcdUtils.getLedgerKey(scope, Long.MAX_VALUE)),
            bs -> {
                UUID uuid = EtcdUtils.parseLedgerKey(bs.toStringUtf8());
                return uuid.getLeastSignificantBits();
            }
        );
        processLedgers(
            ks, processor, finalCb, context, successRc, failureRc);
    }

    private void processLedgers(KeyStream<Long> ks,
                                Processor<Long> processor,
                                VoidCallback finalCb,
                                Object context,
                                int successRc,
                                int failureRc) {
        ks.readNext().whenCompleteAsync((ledgers, cause) -> {
            if (null != cause) {
                finalCb.processResult(failureRc, null, context);
            } else {
                if (ledgers.isEmpty()) {
                    finalCb.processResult(successRc, null, context);
                } else {
                    ledgers.forEach(l -> processor.process(l, finalCb));
                    processLedgers(ks, processor, finalCb, context, successRc, failureRc);
                }
            }
        });
    }

    @Override
    public LedgerRangeIterator getLedgerRanges(long opTimeOutMs) {
        KeyStream<Long> ks = new KeyStream<>(
            kvClient,
            ByteSequence.fromString(EtcdUtils.getLedgerKey(scope, 0L)),
            ByteSequence.fromString(EtcdUtils.getLedgerKey(scope, Long.MAX_VALUE)),
            bs -> {
                UUID uuid = EtcdUtils.parseLedgerKey(bs.toStringUtf8());
                return uuid.getLeastSignificantBits();
            }
        );
        KeyIterator<Long> ki = new KeyIterator<>(ks);
        return new LedgerRangeIterator() {
            @Override
            public boolean hasNext() throws IOException {
                try {
                    return ki.hasNext();
                } catch (Exception e) {
                    if (e instanceof IOException) {
                        throw ((IOException) e);
                    } else {
                        throw new IOException(e);
                    }
                }
            }

            @Override
            public LedgerRange next() throws IOException {
                try {
                    final List<Long> values = ki.next();
                    final Set<Long> ledgers = Sets.newTreeSet();
                    ledgers.addAll(values);
                    return new LedgerRange(ledgers);
                } catch (Exception e) {
                    if (e instanceof IOException) {
                        throw ((IOException) e);
                    } else {
                        throw new IOException(e);
                    }
                }
            }
        };
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        watchClient.close();
    }
}
