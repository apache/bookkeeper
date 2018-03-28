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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.Entry;
import org.apache.distributedlog.io.AsyncCloseable;

/**
 * An interface class to read entries {@link org.apache.distributedlog.Entry}
 * from a random access log segment.
 */
public interface LogSegmentRandomAccessEntryReader extends AsyncCloseable {

    /**
     * Read entries [startEntryId, endEntryId] from a random access log segment.
     *
     * @param startEntryId start entry id
     * @param endEntryId end entry id
     * @return A promise that when satisfied will contain a list of entries of [startEntryId, endEntryId].
     */
    CompletableFuture<List<Entry.Reader>> readEntries(long startEntryId, long endEntryId);

    /**
     * Return the last add confirmed entry id (LAC).
     *
     * @return the last add confirmed entry id.
     */
    long getLastAddConfirmed();
}
