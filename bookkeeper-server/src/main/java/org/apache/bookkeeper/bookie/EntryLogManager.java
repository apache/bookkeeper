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

package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.bookkeeper.bookie.EntryLogger.BufferedLogChannel;

interface EntryLogManager {

    /*
     * add entry to the corresponding entrylog and return the position of
     * the entry in the entrylog
     */
    long addEntry(long ledger, ByteBuf entry, boolean rollLog) throws IOException;

    /*
     * gets the active logChannel with the given entryLogId. null if it is
     * not existing.
     */
    BufferedLogChannel getCurrentLogIfPresent(long entryLogId);

    /*
     * Returns eligible writable ledger dir for the creation next entrylog
     */
    File getDirForNextEntryLog(List<File> writableLedgerDirs);

    /*
     * Do the operations required for checkpoint.
     */
    void checkpoint() throws IOException;

    /*
     * flush both current and rotated logs.
     */
    void flush() throws IOException;

    /*
     * close current logs.
     */
    void close() throws IOException;

    /*
     * force close current logs.
     */
    void forceClose();

    /*
     * prepare entrylogger/entrylogmanager before doing SortedLedgerStorage
     * Checkpoint.
     */
    void prepareSortedLedgerStorageCheckpoint(long numBytesFlushed) throws IOException;

    /*
     * this method should be called before doing entrymemtable flush, it
     * would save the state of the entrylogger before entrymemtable flush
     * and commitEntryMemTableFlush would take appropriate action after
     * entrymemtable flush.
     */
    void prepareEntryMemTableFlush();

    /*
     * this method should be called after doing entrymemtable flush,it would
     * take appropriate action after entrymemtable flush depending on the
     * current state of the entrylogger and the state of the entrylogger
     * during prepareEntryMemTableFlush.
     *
     * It is assumed that there would be corresponding
     * prepareEntryMemTableFlush for every commitEntryMemTableFlush and both
     * would be called from the same thread.
     *
     * returns boolean value indicating whether EntryMemTable should do checkpoint
     * after this commit method.
     */
    boolean commitEntryMemTableFlush() throws IOException;

    /*
     * creates new separate log for compaction.
     */
    BufferedLogChannel createNewLogForCompaction() throws IOException;
}
