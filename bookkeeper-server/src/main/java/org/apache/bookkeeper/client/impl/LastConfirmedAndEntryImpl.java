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
package org.apache.bookkeeper.client.impl;

import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntry;

/**
 * This contains LastAddConfirmed entryId and a LedgerEntry wanted to read.
 * It is used for readLastAddConfirmedAndEntry.
 */
public class LastConfirmedAndEntryImpl implements LastConfirmedAndEntry{

    private final Long lac;
    private final LedgerEntry entry;

    public LastConfirmedAndEntryImpl(Long lac, LedgerEntry entry) {
        this.lac = lac;
        this.entry = entry;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public Long getLastAddConfirmed() {
        return lac;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public boolean hasEntry() {
        return entry != null;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public LedgerEntry getEntry() {
        return entry;
    }
}
