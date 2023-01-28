/*
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
import org.apache.bookkeeper.metastore.MetastoreScannableTable.Order;

/**
 * Async to sync converter for a metastore scannable table.
 */
public class MetastoreScannableTableAsyncToSyncConverter extends
             MetastoreTableAsyncToSyncConverter {

    private MetastoreScannableTable scannableTable;

    public MetastoreScannableTableAsyncToSyncConverter(
            MetastoreScannableTable table) {
        super(table);
        this.scannableTable = table;
    }

    public MetastoreCursor openCursor(String firstKey, boolean firstInclusive,
                                      String lastKey, boolean lastInclusive,
                                      Order order)
    throws MSException {
        HeldValue<MetastoreCursor> retValue = new HeldValue<MetastoreCursor>();
        // make the actual async call
        this.scannableTable.openCursor(firstKey, firstInclusive, lastKey, lastInclusive,
                                       order, retValue, null);
        retValue.waitCallback();
        return retValue.getValue();
    }

    public MetastoreCursor openCursor(String firstKey, boolean firstInclusive,
                                      String lastKey, boolean lastInclusive,
                                      Order order, Set<String> fields)
    throws MSException {
        HeldValue<MetastoreCursor> retValue = new HeldValue<MetastoreCursor>();
        // make the actual async call
        this.scannableTable.openCursor(firstKey, firstInclusive, lastKey, lastInclusive,
                                       order, fields, retValue, null);
        retValue.waitCallback();
        return retValue.getValue();
    }

}
