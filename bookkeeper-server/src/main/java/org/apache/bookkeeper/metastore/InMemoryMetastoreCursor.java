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

import static org.apache.bookkeeper.metastore.InMemoryMetastoreTable.cloneValue;

import com.google.common.collect.ImmutableSortedMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.metastore.MSException.Code;
import org.apache.bookkeeper.versioning.Versioned;

class InMemoryMetastoreCursor implements MetastoreCursor {

    private final ScheduledExecutorService scheduler;
    private final Iterator<Map.Entry<String, Versioned<Value>>> iter;
    private final Set<String> fields;

    public InMemoryMetastoreCursor(SortedMap<String, Versioned<Value>> map, Set<String> fields,
            ScheduledExecutorService scheduler) {
        // copy an map for iterator to avoid concurrent modification problem.
        this.iter = ImmutableSortedMap.copyOfSorted(map).entrySet().iterator();
        this.fields = fields;
        this.scheduler = scheduler;
    }

    @Override
    public boolean hasMoreEntries() {
        return iter.hasNext();
    }

    @Override
    public Iterator<MetastoreTableItem> readEntries(int numEntries)
    throws MSException {
        if (numEntries < 0) {
            throw MSException.create(Code.IllegalOp);
        }
        return unsafeReadEntries(numEntries);
    }

    @Override
    public void asyncReadEntries(final int numEntries, final ReadEntriesCallback cb, final Object ctx) {
        scheduler.submit(new Runnable() {
            @Override
            public void run() {
                if (numEntries < 0) {
                    cb.complete(Code.IllegalOp.getCode(), null, ctx);
                    return;
                }
                Iterator<MetastoreTableItem> result = unsafeReadEntries(numEntries);
                cb.complete(Code.OK.getCode(), result, ctx);
            }
        });
    }

    private Iterator<MetastoreTableItem> unsafeReadEntries(int numEntries) {
        List<MetastoreTableItem> entries = new ArrayList<MetastoreTableItem>();
        int nCount = 0;
        while (iter.hasNext() && nCount < numEntries) {
            Map.Entry<String, Versioned<Value>> entry = iter.next();
            Versioned<Value> value = entry.getValue();
            Versioned<Value> vv = cloneValue(value.getValue(), value.getVersion(), fields);
            String key = entry.getKey();
            entries.add(new MetastoreTableItem(key, vv));
            ++nCount;
        }
        return entries.iterator();
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
