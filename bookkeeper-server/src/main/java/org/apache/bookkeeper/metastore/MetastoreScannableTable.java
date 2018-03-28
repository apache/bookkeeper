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

import java.util.Set;

/**
 * Metastore Scannable Table.
 */
public interface MetastoreScannableTable extends MetastoreTable {

    // Used by cursor, etc when they want to start at the beginning of a table
    String EMPTY_START_KEY = null;
    // Last row in a table.
    String EMPTY_END_KEY = null;
    /**
     * The order to loop over a table.
     */
    enum Order {
        ASC,
        DESC
    }

    /**
     * Open a cursor to loop over the entries belonging to a key range,
     * which returns all fields for each entry.
     *
     * <p>Return Code:<br/>
     * {@link MSException.Code.OK}: an opened cursor<br/>
     * {@link MSException.Code.IllegalOp}/{@link MSException.Code.ServiceDown}:
     * other issues
     *
     * @param firstKey
     *            Key to start scanning. If it is {@link EMPTY_START_KEY}, it starts
     *            from first key (inclusive).
     * @param firstInclusive
     *            true if firstKey is to be included in the returned view.
     * @param lastKey
     *            Key to stop scanning. If it is {@link EMPTY_END_KEY}, scan ends at
     *            the lastKey of the table (inclusive).
     * @param lastInclusive
     *            true if lastKey is to be included in the returned view.
     * @param order
     *            the order to loop over the entries
     * @param cb
     *            Callback to return an opened cursor.
     * @param ctx
     *            Callback context
     */
    void openCursor(String firstKey, boolean firstInclusive,
                           String lastKey, boolean lastInclusive,
                           Order order,
                           MetastoreCallback<MetastoreCursor> cb,
                           Object ctx);

    /**
     * Open a cursor to loop over the entries belonging to a key range,
     * which returns the specified <code>fields</code> for each entry.
     *
     * <p>Return Code:<br/>
     * {@link MSException.Code.OK}: an opened cursor<br/>
     * {@link MSException.Code.IllegalOp}/{@link MSException.Code.ServiceDown}:
     * other issues
     *
     * @param firstKey
     *            Key to start scanning. If it is {@link EMPTY_START_KEY}, it starts
     *            from first key (inclusive).
     * @param firstInclusive
     *            true if firstKey is to be included in the returned view.
     * @param lastKey
     *            Key to stop scanning. If it is {@link EMPTY_END_KEY}, scan ends at
     *            the lastKey of the table (inclusive).
     * @param lastInclusive
     *            true if lastKey is to be included in the returned view.
     * @param order
     *            the order to loop over the entries
     * @param fields
     *            Fields to select
     * @param cb
     *            Callback to return an opened cursor.
     * @param ctx
     *            Callback context
     */
    void openCursor(String firstKey, boolean firstInclusive,
                           String lastKey, boolean lastInclusive,
                           Order order, Set<String> fields,
                           MetastoreCallback<MetastoreCursor> cb,
                           Object ctx);

}
