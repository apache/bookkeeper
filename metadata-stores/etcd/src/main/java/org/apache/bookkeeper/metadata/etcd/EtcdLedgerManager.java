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
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.Cmp.Op;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import java.io.IOException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.AsyncCallback.VoidCallback;

/**
 * Etcd ledger manager.
 */
class EtcdLedgerManager implements LedgerManager {

    private final String scope;
    private final KV kvClient;

    EtcdLedgerManager(KV kvClient, String scope) {
        this.kvClient = kvClient;
        this.scope = scope;
    }

    @Override
    public void createLedgerMetadata(long ledgerId,
                                     LedgerMetadata metadata,
                                     GenericCallback<Void> cb) {
        String ledgerKey = EtcdUtils.getLedgerKey(scope, ledgerId);
        kvClient.txn()
            .If(new Cmp(
                ByteSequence.fromString(ledgerKey),
                Op.EQUAL,
                CmpTarget.value(ByteSequence.fromBytes(new byte[0]))))
            .Then(com.coreos.jetcd.op.Op.put(
                ByteSequence.fromString(ledgerKey),
                ByteSequence.fromBytes(metadata.serialize()),
                PutOption.DEFAULT))
            .Else(com.coreos.jetcd.op.Op.get(
                ByteSequence.fromString(ledgerKey),
                GetOption.DEFAULT))
            .commit()
            .thenAccept(resp -> {
                if (resp.isSucceeded()) {
                    metadata.setVersion(new LongVersion(0L));
                    cb.operationComplete(Code.OK, null);
                } else {
                    GetResponse getResp = resp.getGetResponses().get(0);
                    if (getResp.getCount() <= 0) {
                        // key doesn't exist but we fail to put the key
                        cb.operationComplete(Code.UnexpectedConditionException, null);
                    } else {
                        // key exists
                        cb.operationComplete(Code.LedgerExistException, null);
                    }
                }
            })
            .exceptionally(cause -> {
                cb.operationComplete(Code.MetaStoreException, null);
                return null;
            });
    }

    @Override
    public void removeLedgerMetadata(long ledgerId, Version version, GenericCallback<Void> cb) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readLedgerMetadata(long ledgerId, GenericCallback<LedgerMetadata> readCb) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<Void> cb) {
        Version v = metadata.getVersion();
        if (Version.NEW == v || !(v instanceof LongVersion)) {
            cb.operationComplete(Code.MetadataVersionException, null);
            return;
        }
        final LongVersion lv = (LongVersion) v;

        String ledgerKey = EtcdUtils.getLedgerKey(scope, ledgerId);
        kvClient.txn()
            .If(new Cmp(
                ByteSequence.fromString(ledgerKey),
                Op.EQUAL,
                CmpTarget.version(lv.getLongVersion())))
            .Then(com.coreos.jetcd.op.Op.put(
                ByteSequence.fromString(ledgerKey),
                ByteSequence.fromBytes(metadata.serialize()),
                PutOption.DEFAULT))
            .commit()
            .thenAccept(resp -> {
                if (resp.isSucceeded()) {
                    metadata.setVersion(new LongVersion(lv.getLongVersion() + 1));
                    cb.operationComplete(Code.OK, null);
                } else {
                    cb.operationComplete(Code.MetadataVersionException, null);
                }
            })
            .exceptionally(cause -> {
                cb.operationComplete(Code.MetaStoreException, null);
                return null;
            });
    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void asyncProcessLedgers(Processor<Long> processor, VoidCallback finalCb, Object context, int successRc, int failureRc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LedgerRangeIterator getLedgerRanges() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {

    }
}
