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

import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.ioResult;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerLayout;

/**
 * Etcd based layout manager.
 */
@Slf4j
@Getter(AccessLevel.PACKAGE)
class EtcdLayoutManager implements LayoutManager {

    private final Client client;
    private final KV kvClient;
    private final String scope;
    private final ByteSequence layoutKey;

    EtcdLayoutManager(Client client, String scope) {
        this.client = client;
        this.kvClient = client.getKVClient();
        this.scope = scope;
        this.layoutKey = ByteSequence.from(EtcdUtils.getLayoutKey(scope), StandardCharsets.UTF_8);
    }

    @Override
    public LedgerLayout readLedgerLayout() throws IOException {
        GetResponse response = ioResult(kvClient.get(layoutKey, GetOption.DEFAULT));
        if (response.getCount() <= 0) {
            return null;
        } else {
            byte[] layoutData = response.getKvs().get(0).getValue().getBytes();
            return LedgerLayout.parseLayout(layoutData);
        }
    }

    @Override
    public void storeLedgerLayout(LedgerLayout layout) throws IOException {
        ByteSequence layoutData = ByteSequence.from(layout.serialize());
        TxnResponse response = ioResult(kvClient.txn()
            .If(new Cmp(layoutKey, Cmp.Op.GREATER, CmpTarget.createRevision(0)))
            .Then(io.etcd.jetcd.op.Op.get(layoutKey, GetOption.DEFAULT))
            .Else(io.etcd.jetcd.op.Op.put(layoutKey, layoutData, PutOption.DEFAULT))
            .commit());
        // key doesn't exist and we created the key
        if (!response.isSucceeded()) {
            return;
        // key exists and we retrieved the key
        } else {
            GetResponse resp = response.getGetResponses().get(0);
            if (resp.getCount() <= 0) {
                // fail to put key/value but key is not found
                throw new IOException("Creating layout node '" + layoutKey.toString(StandardCharsets.UTF_8)
                    + "' failed due to it already exists but no layout node is found");
            } else {
                throw new LedgerLayoutExistsException(
                    "Ledger layout already exists under '" + layoutKey.toString(StandardCharsets.UTF_8) + "'");
            }
        }
    }

    @Override
    public void deleteLedgerLayout() throws IOException {
        DeleteResponse response = ioResult(kvClient.delete(layoutKey));
        if (response.getDeleted() > 0) {
            if (log.isDebugEnabled()) {
                log.debug("Successfully delete layout '{}'", layoutKey.toString(StandardCharsets.UTF_8));
            }
            return;
        } else {
            throw new IOException("No ledger layout is found under '" + layoutKey.toString(StandardCharsets.UTF_8)
                    + "'");
        }
    }
}
