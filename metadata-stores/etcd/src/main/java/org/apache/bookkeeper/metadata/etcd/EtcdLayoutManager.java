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

import com.coreos.jetcd.KV;
import com.coreos.jetcd.Lease;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.options.GetOption;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerLayout;

/**
 * Etcd based layout manager.
 */
@Getter(AccessLevel.PACKAGE)
class EtcdLayoutManager implements LayoutManager {

    private final KV kvClient;
    private final Lease leaseClient;
    private final String scope;
    private final ByteSequence layoutKey;

    EtcdLayoutManager(KV kvClient,
                      Lease leaseClient,
                      String scope) {
        this.kvClient = kvClient;
        this.leaseClient = leaseClient;
        this.scope = scope;
        this.layoutKey = ByteSequence.fromString(
            EtcdUtils.getLayoutKey(scope));
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteLedgerLayout() throws IOException {
        throw new UnsupportedOperationException();
    }
}
