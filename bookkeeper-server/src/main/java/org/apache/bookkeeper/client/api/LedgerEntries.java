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
 * Interface to wrap the entries.
 *
 * @since 4.6
 */
public interface LedgerEntries extends AutoCloseable {

    /**
     * Gets a specific LedgerEntry by entryId.
     *
     * @param entryId the LedgerEntry id
     * @return the LedgerEntry, null if no LedgerEntry with such entryId.
     */
    LedgerEntry getEntry(long entryId);

    /**
     * This method does not increment the reference count of ByteBuf for the entries in this LedgerEntries.
     * The caller who calls {@link #iterator()} should be careful for not releasing the references.
     * The implementation of this interface will release all the Entries ByteBuf reference when close.
     *
     *  @return the iterator of type LedgerEntry
     */
    Iterator<LedgerEntry> iterator();
}
