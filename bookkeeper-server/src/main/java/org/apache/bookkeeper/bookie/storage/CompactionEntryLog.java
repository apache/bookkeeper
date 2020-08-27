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
 * An entrylog to received compacted entries.
 * <p/>
 * The expected lifecycle for a compaction entry log is:
 * 1. Creation
 * 2. Mark compacted
 * 3. Make available
 * 4. Cleanup
 * <p/>
 * Abort can happen at during any step.
 */
public interface CompactionEntryLog {
    /**
     * Add an entry to the log.
     * @param ledgerId the ledger the entry belong to
     * @param entry the payload of the entry
     * @return the position to which the entry was written
     */
    long addEntry(long ledgerId, ByteBuf entry) throws IOException;

    /**
     * Scan the entry log, reading out all contained entries.
     */
    void scan(EntryLogScanner scanner) throws IOException;

    /**
     * Flush any unwritten entries to physical storage.
     */
    void flush() throws IOException;

    /**
     * Abort the compaction log. This should delete any resources held
     * by this log.
     */
    void abort();

    /**
     * Mark the compaction log as compacted.
     * From this point, the heavy work of copying entries from one log
     * to another should be done. We don't want to repeat that work,
     * so this method should take steps to ensure that if the bookie crashes
     * we can resume the compaction from this point.
     */
    void markCompacted() throws IOException;

    /**
     * Make the log written by the compaction process available for reads.
     */
    void makeAvailable() throws IOException;

    /**
     * Clean up any temporary resources that were used by the compaction process.
     * At this point, there
     */
    void cleanup();

    /**
     * Get the log ID of the entrylog to which compacted entries are being written.
     */
    long getLogId();

    /**
     * Get the log ID of the entrylog which is being compacted.
     */
    long getCompactedLogId();
}
