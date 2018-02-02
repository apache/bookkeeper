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

import java.io.IOException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.AsyncCallback.VoidCallback;

/**
 * Etcd ledger manager.
 */
class EtcdLedgerManager implements LedgerManager {
    @Override
    public void createLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<Void> cb) {

    }

    @Override
    public void removeLedgerMetadata(long ledgerId, Version version, GenericCallback<Void> cb) {

    }

    @Override
    public void readLedgerMetadata(long ledgerId, GenericCallback<LedgerMetadata> readCb) {

    }

    @Override
    public void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<Void> cb) {

    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {

    }

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {

    }

    @Override
    public void asyncProcessLedgers(Processor<Long> processor, VoidCallback finalCb, Object context, int successRc, int failureRc) {

    }

    @Override
    public LedgerRangeIterator getLedgerRanges() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
