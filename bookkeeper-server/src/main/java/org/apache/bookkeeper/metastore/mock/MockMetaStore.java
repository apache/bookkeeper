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
package org.apache.bookkeeper.metastore.mock;

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.metastore.MetaStore;
import org.apache.bookkeeper.metastore.MetastoreException;
import org.apache.bookkeeper.metastore.MetastoreTable;
import org.apache.bookkeeper.metastore.MetastoreScannableTable;
import org.apache.commons.configuration.Configuration;

public class MockMetaStore implements MetaStore {

    static final int CUR_VERSION = 1;

    static Map<String, MockMetastoreTable> tables =
        new HashMap<String, MockMetastoreTable>();

    // for test
    public static void reset() {
        tables.clear();
    }

    @Override
    public String getName() {
        return getClass().getName();
    }

    @Override
    public int getVersion() {
        return CUR_VERSION;
    }

    @Override
    public void init(Configuration conf, int msVersion)
    throws MetastoreException {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public MetastoreTable createTable(String name) {
        return createMockTable(name);
    }

    @Override
    public MetastoreScannableTable createScannableTable(String name) {
        return createMockTable(name);
    }

    private MockMetastoreTable createMockTable(String name) {
        MockMetastoreTable t = tables.get(name);
        if (t == null) {
            t = new MockMetastoreTable(this, name);
            tables.put(name, t);
        }
        return t;
    }

}
