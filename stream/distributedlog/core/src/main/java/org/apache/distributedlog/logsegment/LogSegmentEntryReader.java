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
package org.apache.distributedlog.logsegment;

import com.google.common.annotations.Beta;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.Entry;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.io.AsyncCloseable;

/**
 * An interface class to read the enveloped entry (serialized bytes of
 * {@link org.apache.distributedlog.Entry}) from a log segment.
 */
@Beta
public interface LogSegmentEntryReader extends AsyncCloseable {
    /**
     * An interface Listener for StateChange.
     */
    interface StateChangeListener {

        /**
         * Notify when caught up on inprogress.
         */
        void onCaughtupOnInprogress();

    }

    /**
     * Start the reader. The method to signal the implementation
     * to start preparing the data for consumption {@link #readNext(int)}
     */
    void start();

    /**
     * Register the state change listener.
     *
     * @param listener register the state change listener
     * @return entry reader
     */
    LogSegmentEntryReader registerListener(StateChangeListener listener);

    /**
     * Unregister the state change listener.
     *
     * @param listener register the state change listener
     * @return entry reader
     */
    LogSegmentEntryReader unregisterListener(StateChangeListener listener);

    /**
     * Return the log segment metadata for this reader.
     *
     * @return the log segment metadata
     */
    LogSegmentMetadata getSegment();

    /**
     * Update the log segment each time when the metadata has changed.
     *
     * @param segment new metadata of the log segment.
     */
    void onLogSegmentMetadataUpdated(LogSegmentMetadata segment);

    /**
     * Read next <i>numEntries</i> entries from current log segment.
     *
     *  <p><i>numEntries</i> will be best-effort.
     *
     * @param numEntries num entries to read from current log segment
     * @return A promise that when satisified will contain a non-empty list of entries with their content.
     * @throw {@link org.apache.distributedlog.exceptions.EndOfLogSegmentException} when
     *          read entries beyond the end of a <i>closed</i> log segment.
     */
    CompletableFuture<List<Entry.Reader>> readNext(int numEntries);

    /**
     * Return the last add confirmed entry id (LAC).
     *
     * @return the last add confirmed entry id.
     */
    long getLastAddConfirmed();

    /**
     * Is the reader reading beyond last add confirmed.
     *
     * @return true if the reader is reading beyond last add confirmed
     */
    boolean isBeyondLastAddConfirmed();

    /**
     * Has the log segment reader caught up with the inprogress log segment.
     *
     * @return true only if the log segment is inprogress and it is caught up, otherwise return false.
     */
    boolean hasCaughtUpOnInprogress();

}
