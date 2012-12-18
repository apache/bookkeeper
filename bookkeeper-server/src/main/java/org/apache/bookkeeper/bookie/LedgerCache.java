/*
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

package org.apache.bookkeeper.bookie;

import java.io.Closeable;
import java.io.IOException;

/**
 * This class maps a ledger entry number into a location (entrylogid, offset) in
 * an entry log file. It does user level caching to more efficiently manage disk
 * head scheduling.
 */
interface LedgerCache extends Closeable {

    boolean setFenced(long ledgerId) throws IOException;
    boolean isFenced(long ledgerId) throws IOException;

    void setMasterKey(long ledgerId, byte[] masterKey) throws IOException;
    byte[] readMasterKey(long ledgerId) throws IOException, BookieException;
    boolean ledgerExists(long ledgerId) throws IOException;

    void putEntryOffset(long ledger, long entry, long offset) throws IOException;
    long getEntryOffset(long ledger, long entry) throws IOException;

    void flushLedger(boolean doAll) throws IOException;
    long getLastEntry(long ledgerId) throws IOException;

    void deleteLedger(long ledgerId) throws IOException;

    LedgerCacheBean getJMXBean();
}
