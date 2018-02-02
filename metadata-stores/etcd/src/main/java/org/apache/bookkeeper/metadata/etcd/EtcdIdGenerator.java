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

import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.options.GetOption;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
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
    private long idKeyRev = -1L;
    private CompletableFuture<Void> fetchKeyFuture = null;
    private ByteSequence idGenKey;

    EtcdIdGenerator(KV kvClient, String scope) {
       this.kvClient = kvClient;
       this.scope = scope;
       this.idGenKey = ByteSequence.fromString()
    }

    @Override
    public void generateLedgerId(GenericCallback<Long> cb) {
        Cmp keyCompare;
        if (idKeyRev <= 0L) {
            keyCompare = new Cmp(

            )
        } else {

        }
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
                ByteSequence.fromString(EtcdUtils.getIdGenKey(scope)),
                GetOption.DEFAULT
            ).whenComplete(new FutureEventListener<GetResponse>() {
                @Override
                public void onSuccess(GetResponse response) {
                    if (response.getCount() == 0) {
                        // no key is found.
                        nextId = 0L;
                        idKeyRev = -1L;
                    } else {
                        KeyValue kv = response.getKvs().get(0);
                        idKeyRev = kv.getModRevision();
                        long curId = EtcdUtils.toLong(
                            kv.getValue().getBytes(),
                            0);
                        nextId = curId + 1;
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
