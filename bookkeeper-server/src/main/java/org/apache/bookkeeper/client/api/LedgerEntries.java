/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client.api;

import java.util.Iterator;

/**
 * Interface to wrap a sequence of entries.
 *
 * @since 4.6
 */
public interface LedgerEntries
    extends AutoCloseable, Iterable<LedgerEntry> {

    /**
     * Gets a specific LedgerEntry by entryId.
     *
     * @param entryId the LedgerEntry id
     * @return the LedgerEntry, null if no LedgerEntry with such entryId
     */
    LedgerEntry getEntry(long entryId);

    /**
     * Get an iterator over all the ledger entries contained in the
     * LedgerEntries object.
     *
     * <p>Calling this method does not modify the reference count of the ByteBuf in the returned LedgerEntry objects.
     * The caller who calls {@link #iterator()} should make sure that they do not call ByteBuf.release() on the
     * LedgerEntry objects to avoid a double free.
     * All reference counts will be decremented when the containing LedgerEntries object is closed via {@link #close()}.
     *
     * @return an iterator of LedgerEntry objects
     */
    @Override
    Iterator<LedgerEntry> iterator();

    /**
     * Close to release the resources held by this instance.
     */
    @Override
    void close();
}
