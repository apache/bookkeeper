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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

/**
 * An in-memory implementation of the MetaStore interface.
 */
public class InMemoryMetaStore implements MetaStore {

    static final int CUR_VERSION = 1;

    static Map<String, InMemoryMetastoreTable> tables =
        new HashMap<String, InMemoryMetastoreTable>();

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
        return createInMemoryTable(name);
    }

    @Override
    public MetastoreScannableTable createScannableTable(String name) {
        return createInMemoryTable(name);
    }

    private InMemoryMetastoreTable createInMemoryTable(String name) {
        InMemoryMetastoreTable t = tables.get(name);
        if (t == null) {
            t = new InMemoryMetastoreTable(this, name);
            tables.put(name, t);
        }
        return t;
    }

}
