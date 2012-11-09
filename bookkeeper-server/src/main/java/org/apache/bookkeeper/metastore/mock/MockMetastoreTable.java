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

import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.metastore.MSException.Code;
import org.apache.bookkeeper.metastore.MetastoreCallback;
import org.apache.bookkeeper.metastore.MetastoreCursor;
import org.apache.bookkeeper.metastore.MetastoreScannableTable;
import org.apache.bookkeeper.metastore.Value;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

public class MockMetastoreTable implements MetastoreScannableTable {

    public static class MockVersion implements Version {
        int version;

        public MockVersion(int v) {
            this.version = v;
        }

        public MockVersion(MockVersion v) {
            this.version = v.version;
        }

        public synchronized MockVersion incrementVersion() {
            ++version;
            return this;
        }

        @Override
        public Occurred compare(Version v) {
            if (null == v) {
                throw new NullPointerException("Version is not allowed to be null.");
            }
            if (v == Version.NEW) {
                return Occurred.AFTER;
            } else if (v == Version.ANY) {
                return Occurred.CONCURRENTLY;
            } else if (!(v instanceof MockVersion)) {
                throw new IllegalArgumentException("Invalid version type");
            }
            MockVersion mv = (MockVersion)v;
            int res = version - mv.version;
            if (res == 0) {
                return Occurred.CONCURRENTLY;
            } else if (res < 0) {
                return Occurred.BEFORE;
            } else {
                return Occurred.AFTER;
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (null == obj ||
                !(obj instanceof MockVersion)) {
                return false;
            }
            MockVersion v = (MockVersion)obj;
            return 0 == (version - v.version);
        }

        @Override
        public String toString() {
            return "version=" + version;
        }

        @Override
        public int hashCode() {
            return version;
        }
    }

    private String name;
    private TreeMap<String, Versioned<Value>> map = null;
    private ScheduledExecutorService scheduler;

    public MockMetastoreTable(MockMetaStore metastore, String name) {
        this.map = new TreeMap<String, Versioned<Value>>();
        this.name = name;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public String getName () {
        return this.name;
    }

    static Versioned<Value> cloneValue(Value value, Version version, Set<String> fields) {
        if (null != value) {
            Value newValue = new Value();
            if (ALL_FIELDS == fields) {
                fields = value.getFields();
            }
            for (String f : fields) {
                newValue.setField(f, value.getField(f));
            }
            value = newValue;
        }

        if (null == version) {
            throw new NullPointerException("Version isn't allowed to be null.");
        }
        if (Version.ANY != version && Version.NEW != version) {
            if (version instanceof MockVersion) {
                version = new MockVersion(((MockVersion)version).version);
            } else {
                throw new IllegalStateException("Wrong version type.");
            }
        }
        return new Versioned<Value>(value, version);
    }

    @Override
    public void get(final String key, final MetastoreCallback<Versioned<Value>> cb, final Object ctx) {
        scheduler.submit(new Runnable() {
            @Override
            public void run() {
                scheduleGet(key, ALL_FIELDS, cb, ctx);
            }
        });
    }

    @Override
    public void get(final String key, final Set<String> fields, final MetastoreCallback<Versioned<Value>> cb,
            final Object ctx) {
        scheduler.submit(new Runnable() {
            @Override
            public void run() {
                scheduleGet(key, fields, cb, ctx);
            }
        });
    }

    public synchronized void scheduleGet(String key, Set<String> fields, MetastoreCallback<Versioned<Value>> cb,
            Object ctx) {
        if (null == key) {
            cb.complete(Code.IllegalOp.getCode(), null, ctx);
            return;
        }
        Versioned<Value> vv = mockGet(key);
        int rc = null == vv ? Code.NoKey.getCode() : Code.OK.getCode();
        if (vv != null) {
            vv = cloneValue(vv.getValue(), vv.getVersion(), fields);
        }
        cb.complete(rc, vv, ctx);
    }

    @Override
    public void put(final String key, final Value value, final Version version, final MetastoreCallback<Version> cb,
            final Object ctx) {
        scheduler.submit(new Runnable() {
            @Override
            public void run() {
                if (null == key || null == value || null == version) {
                    cb.complete(Code.IllegalOp.getCode(), null, ctx);
                    return;
                }
                Result<Version> result = mockPut(key, value, version);
                cb.complete(result.code.getCode(), result.value, ctx);
            }
        });
    }

    @Override
    public void remove(final String key, final Version version, final MetastoreCallback<Void> cb, final Object ctx) {
        scheduler.submit(new Runnable() {
            @Override
            public void run() {
                if (null == key || null == version) {
                    cb.complete(Code.IllegalOp.getCode(), null, ctx);
                    return;
                }
                Code code = mockRemove(key, version);
                cb.complete(code.getCode(), null, ctx);
            }
        });
    }

    @Override
    public void openCursor(MetastoreCallback<MetastoreCursor> cb, Object ctx) {
        openCursor(EMPTY_START_KEY, true, EMPTY_END_KEY, true, Order.ASC,
                   ALL_FIELDS, cb, ctx);
    }

    @Override
    public void openCursor(Set<String> fields,
                           MetastoreCallback<MetastoreCursor> cb, Object ctx) {
        openCursor(EMPTY_START_KEY, true, EMPTY_END_KEY, true, Order.ASC,
                   fields, cb, ctx);
    }

    @Override
    public void openCursor(String firstKey, boolean firstInclusive,
                           String lastKey, boolean lastInclusive,
                           Order order, MetastoreCallback<MetastoreCursor> cb,
                           Object ctx) {
        openCursor(firstKey, firstInclusive, lastKey, lastInclusive,
                   order, ALL_FIELDS, cb, ctx);
    }

    @Override
    public void openCursor(final String firstKey, final boolean firstInclusive,
                           final String lastKey, final boolean lastInclusive,
                           final Order order, final Set<String> fields,
                           final MetastoreCallback<MetastoreCursor> cb, final Object ctx) {
        scheduler.submit(new Runnable() {
            @Override
            public void run() {
                Result<MetastoreCursor> result = mockOpenCursor(firstKey, firstInclusive, lastKey, lastInclusive,
                        order, fields);
                cb.complete(result.code.getCode(), result.value, ctx);
            }
        });
    }

    private synchronized Versioned<Value> mockGet(String key) {
        return map.get(key);
    }

    private synchronized Code mockRemove(String key, Version version) {
        Versioned<Value> vv = map.get(key);
        if (null == vv) {
            return Code.NoKey;
        }
        if (Version.Occurred.CONCURRENTLY != vv.getVersion().compare(version)) {
            return Code.BadVersion;
        }
        map.remove(key);
        return Code.OK;
    }

    static class Result<T> {
        Code code;
        T value;

        public Result(Code code, T value) {
            this.code = code;
            this.value = value;
        }
    }

    private synchronized Result<Version> mockPut(String key, Value value, Version version) {
        Versioned<Value> vv = map.get(key);
        if (vv == null) {
            if (Version.NEW != version) {
                return new Result<Version>(Code.NoKey, null);
            }
            vv = cloneValue(value, version, ALL_FIELDS);
            vv.setVersion(new MockVersion(0));
            map.put(key, vv);
            return new Result<Version>(Code.OK, new MockVersion(0));
        }
        if (Version.NEW == version) {
            return new Result<Version>(Code.KeyExists, null);
        }
        if (Version.Occurred.CONCURRENTLY != vv.getVersion().compare(version)) {
            return new Result<Version>(Code.BadVersion, null);
        }
        vv.setVersion(((MockVersion)vv.getVersion()).incrementVersion());
        vv.setValue(vv.getValue().merge(value));
        return new Result<Version>(Code.OK, new MockVersion((MockVersion)vv.getVersion()));
    }

    private synchronized Result<MetastoreCursor> mockOpenCursor(
            String firstKey, boolean firstInclusive,
            String lastKey, boolean lastInclusive,
            Order order, Set<String> fields) {
        if (0 == map.size()) {
            return new Result<MetastoreCursor>(Code.OK, MetastoreCursor.EMPTY_CURSOR);
        }

        boolean isLegalCursor = false;
        NavigableMap<String, Versioned<Value>> myMap = null;
        if (Order.ASC == order) {
            myMap = map;
            if (EMPTY_END_KEY == lastKey ||
                lastKey.compareTo(myMap.lastKey()) > 0) {
                lastKey = myMap.lastKey();
                lastInclusive = true;
            }
            if (EMPTY_START_KEY == firstKey ||
                firstKey.compareTo(myMap.firstKey()) < 0) {
                firstKey = myMap.firstKey();
                firstInclusive = true;
            }
            if (firstKey.compareTo(lastKey) <= 0) {
                isLegalCursor = true;
            }
        } else if (Order.DESC == order) {
            myMap = map.descendingMap();
            if (EMPTY_START_KEY == lastKey ||
                lastKey.compareTo(myMap.lastKey()) < 0) {
                lastKey = myMap.lastKey();
                lastInclusive = true;
            }
            if (EMPTY_END_KEY == firstKey ||
                firstKey.compareTo(myMap.firstKey()) > 0) {
                firstKey = myMap.firstKey();
                firstInclusive = true;
            }
            if (firstKey.compareTo(lastKey) >= 0) {
                isLegalCursor = true;
            }
        }

        if (!isLegalCursor || null == myMap) {
            return new Result<MetastoreCursor>(Code.IllegalOp, null);
        }
        MetastoreCursor cursor = new MockMetastoreCursor(
                myMap.subMap(firstKey, firstInclusive, lastKey, lastInclusive), fields, scheduler);
        return new Result<MetastoreCursor>(Code.OK, cursor);
    }

    @Override
    public void close() {
        // do nothing
    }
}
