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

import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getIdGenKey;

import com.coreos.jetcd.KV;
import com.coreos.jetcd.Txn;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.Cmp.Op;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;

/**
 * Etcd id generator.
 */
class EtcdIdGenerator implements LedgerIdGenerator {

    private final String scope;
    private final KV kvClient;
    private long nextId = -1L;
    private long idKeyVersion = -1L;
    private CompletableFuture<Void> fetchKeyFuture = null;
    private ByteSequence idGenKey;

    EtcdIdGenerator(KV kvClient, String scope) {
       this.kvClient = kvClient;
       this.scope = scope;
       this.idGenKey = ByteSequence.fromString(getIdGenKey(scope));
    }

    @Override
    public void generateLedgerId(GenericCallback<Long> cb) {
        fetchIdKeyIfNecessary()
            .thenRun(() -> executeGenerateLedgerIdTxn(cb))
            .exceptionally(cause -> {
                cb.operationComplete(Code.MetaStoreException, null);
                return null;
            });
    }

    private void executeGenerateLedgerIdTxn(GenericCallback<Long> cb) {
        Txn txn;
        final long idToAllocate;
        final long idAllocatedAtVersion;
        synchronized (this) {
            Cmp keyCompare;
            if (idKeyVersion < 0L) {
                // key doesn't exist
                keyCompare = new Cmp(
                    idGenKey,
                    Op.EQUAL,
                    CmpTarget.value(ByteSequence.fromBytes(new byte[0])));
            } else {
                keyCompare = new Cmp(
                    idGenKey,
                    Op.EQUAL,
                    CmpTarget.version(idKeyVersion));
            }
            txn = kvClient.txn()
                .If(keyCompare)
                .Then(com.coreos.jetcd.op.Op.put(
                    idGenKey,
                    ByteSequence.fromBytes(EtcdUtils.toBytes(nextId)),
                    PutOption.DEFAULT))
                .Else(com.coreos.jetcd.op.Op.get(
                    idGenKey,
                    GetOption.DEFAULT));
            idToAllocate = nextId;
            idAllocatedAtVersion = idKeyVersion;
        }
        txn.commit()
            .thenAccept(resp -> {
                if (resp.isSucceeded()) {
                    // the creation succeed
                    synchronized (this) {
                        if (nextId == idToAllocate) {
                            nextId = idToAllocate + 1;
                        }
                        if (idKeyVersion == idAllocatedAtVersion) {
                            idKeyVersion = idAllocatedAtVersion + 1;
                        }
                    }
                    cb.operationComplete(Code.OK, idToAllocate);
                } else {
                    // the creation failed
                    GetResponse getResp = resp.getGetResponses().get(0);
                    if (getResp.getCount() <= 0) {
                        // key was deleted somehow
                        cb.operationComplete(Code.UnexpectedConditionException, null);
                    } else {
                        KeyValue kv = getResp.getKvs().get(0);
                        synchronized (this) {
                            idKeyVersion = kv.getVersion();
                            nextId = EtcdUtils.toLong(kv.getValue().getBytes(), 0) + 1;
                        }
                        // try again
                        executeGenerateLedgerIdTxn(cb);
                    }
                }
            })
            .exceptionally(cause -> {
                cb.operationComplete(Code.MetaStoreException, null);
                return null;
            });
    }

    private CompletableFuture<Void> fetchIdKeyIfNecessary() {
        CompletableFuture<Void> keyFuture;
        synchronized (this) {
            if (null != fetchKeyFuture) {
                keyFuture = fetchKeyFuture;
            } else {
                keyFuture = fetchKeyFuture = FutureUtils.createFuture();
            }
        }
        if (null != keyFuture) {
            return keyFuture;
        } else {
            return kvClient.get(
                ByteSequence.fromString(getIdGenKey(scope)),
                GetOption.DEFAULT
            ).whenComplete(new FutureEventListener<GetResponse>() {
                @Override
                public void onSuccess(GetResponse response) {
                    synchronized (EtcdIdGenerator.this) {
                        if (response.getCount() == 0) {
                            // no key is found.
                            nextId = 0L;
                            idKeyVersion = -1L;
                        } else {
                            KeyValue kv = response.getKvs().get(0);
                            idKeyVersion = kv.getVersion();
                            long curId = EtcdUtils.toLong(
                                kv.getValue().getBytes(),
                                0);
                            nextId = curId + 1;
                        }
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    keyFuture.completeExceptionally(cause);
                    synchronized (EtcdIdGenerator.this) {
                        fetchKeyFuture = null;
                    }
                }
            }).thenApply(resp -> null);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
