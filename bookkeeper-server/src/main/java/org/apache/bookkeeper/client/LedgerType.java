/**
 *
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
 *
 */
package org.apache.bookkeeper.client;

import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * Indicates the type of ledger. LedgerTypes describes the behaviour of the ledger in respect to durability and
 * provides hints to the storage of data on Bookies
 */
@InterfaceStability.Stable
public enum LedgerType {
    /**
     * Persistent Durability, using Journal.<br>
     * Each entry is persisted to the journal and every writes receives and acknowledgement only with the guarantee that
     * it has been persisted durabily to it (data is fsync'd to the disk)
     */
    PD_JOURNAL,
    /**
     * Volatile Durability, using Journal.<br>
     * Each entry is persisted to the journal and writes receive acknowledgement without guarantees of persistence
     * (data is eventually fsync'd to disk).<br>
     * For this kind of ledgers the client MUST explicitly call {@link LedgerHandle#asyncSync(long, org.apache.bookkeeper.client.AsyncCallback.SyncCallback, java.lang.Object) }
     * in order to have guarantees of the durability of writes and in order to advance the LastAddConfirmed entry id
     */
    VD_JOURNAL
}
