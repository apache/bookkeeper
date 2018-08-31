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

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.DeleteResponse;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.TxnResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.Cmp.Op;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import java.io.IOException;
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
        this.layoutKey = ByteSequence.fromString(EtcdUtils.getLayoutKey(scope));
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
        ByteSequence layoutData = ByteSequence.fromBytes(layout.serialize());
        TxnResponse response = ioResult(kvClient.txn()
            .If(new Cmp(layoutKey, Op.GREATER, CmpTarget.createRevision(0)))
            .Then(com.coreos.jetcd.op.Op.get(layoutKey, GetOption.DEFAULT))
            .Else(com.coreos.jetcd.op.Op.put(layoutKey, layoutData, PutOption.DEFAULT))
            .commit());
        // key doesn't exist and we created the key
        if (!response.isSucceeded()) {
            return;
        // key exists and we retrieved the key
        } else {
            GetResponse resp = response.getGetResponses().get(0);
            if (resp.getCount() <= 0) {
                // fail to put key/value but key is not found
                throw new IOException("Creating layout node '" + layoutKey.toStringUtf8()
                    + "' failed due to it already exists but no layout node is found");
            } else {
                throw new LedgerLayoutExistsException(
                    "Ledger layout already exists under '" + layoutKey.toStringUtf8() + "'");
            }
        }
    }

    @Override
    public void deleteLedgerLayout() throws IOException {
        DeleteResponse response = ioResult(kvClient.delete(layoutKey));
        if (response.getDeleted() > 0) {
            if (log.isDebugEnabled()) {
                log.debug("Successfully delete layout '{}'", layoutKey.toStringUtf8());
            }
            return;
        } else {
            throw new IOException("No ledger layout is found under '" + layoutKey.toStringUtf8() + "'");
        }
    }
}
