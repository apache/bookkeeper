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
package org.apache.bookkeeper.bookie.storage;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * Scan entries in a entry log file.
 */
public interface EntryLogScanner {
    /**
     * Tests whether or not the entries belongs to the specified ledger
     * should be processed.
     *
     * @param ledgerId
     *          Ledger ID.
     * @return true if and only the entries of the ledger should be scanned.
     */
    boolean accept(long ledgerId);

    /**
     * Process an entry.
     *
     * @param ledgerId
     *          Ledger ID.
     * @param offset
     *          File offset of this entry.
     * @param entry
     *          Entry ByteBuf
     * @throws IOException
     */
    void process(long ledgerId, long offset, ByteBuf entry) throws IOException;
}
