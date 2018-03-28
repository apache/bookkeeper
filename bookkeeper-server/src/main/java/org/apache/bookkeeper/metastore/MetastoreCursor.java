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
package org.apache.bookkeeper.metastore;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * A Metastore Cursor.
 */
public interface MetastoreCursor extends Closeable {

    MetastoreCursor EMPTY_CURSOR = new MetastoreCursor() {
        @Override
        public boolean hasMoreEntries() {
            return false;
        }

        @Override
        public Iterator<MetastoreTableItem> readEntries(int numEntries)
        throws MSException {
            throw new MSException.NoEntriesException("No entries left in the cursor.");
        }

        @Override
        public void asyncReadEntries(int numEntries, ReadEntriesCallback callback, Object ctx) {
            callback.complete(MSException.Code.NoEntries.getCode(), null, ctx);
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
    };

    /**
     * A callback for reading entries.
     */
    interface ReadEntriesCallback extends
        MetastoreCallback<Iterator<MetastoreTableItem>> {
    }

    /**
     * Is there any entries left in the cursor to read.
     *
     * @return true if there is entries left, false otherwise.
     */
    boolean hasMoreEntries();

    /**
     * Read entries from the cursor, up to the specified <code>numEntries</code>.
     * The returned list can be smaller.
     *
     * @param numEntries
     *            maximum number of entries to read
     * @return the iterator of returned entries.
     * @throws MSException when failed to read entries from the cursor.
     */
    Iterator<MetastoreTableItem> readEntries(int numEntries) throws MSException;

    /**
     * Asynchronously read entries from the cursor, up to the specified <code>numEntries</code>.
     *
     * @see #readEntries(int)
     * @param numEntries
     *            maximum number of entries to read
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncReadEntries(int numEntries, ReadEntriesCallback callback, Object ctx);
}
