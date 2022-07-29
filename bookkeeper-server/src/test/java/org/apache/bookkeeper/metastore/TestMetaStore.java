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

import static org.apache.bookkeeper.metastore.MetastoreScannableTable.EMPTY_END_KEY;
import static org.apache.bookkeeper.metastore.MetastoreScannableTable.EMPTY_START_KEY;
import static org.apache.bookkeeper.metastore.MetastoreTable.ALL_FIELDS;
import static org.apache.bookkeeper.metastore.MetastoreTable.NON_FIELDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.bookkeeper.metastore.InMemoryMetastoreTable.MetadataVersion;
import org.apache.bookkeeper.metastore.MSException.Code;
import org.apache.bookkeeper.metastore.MetastoreScannableTable.Order;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the metastore.
 */
public class TestMetaStore {
    private static final Logger logger = LoggerFactory.getLogger(TestMetaStore.class);

    protected static final String TABLE = "myTable";
    protected static final String RECORDID = "test";
    protected static final String FIELD_NAME = "name";
    protected static final String FIELD_COUNTER = "counter";

    protected String getFieldFromValue(Value value, String field) {
        byte[] v = value.getField(field);
        return v == null ? null : new String(v);
    }

    protected static Value makeValue(String name, Integer counter) {
        Value data = new Value();

        if (name != null) {
            data.setField(FIELD_NAME, name.getBytes());
        }

        if (counter != null) {
            data.setField(FIELD_COUNTER, counter.toString().getBytes());
        }

        return data;
    }

    /**
     * A metastore record.
     */
    protected class Record {
        String name;
        Integer counter;
        Version version;

        public Record() {
        }

        public Record(String name, Integer counter, Version version) {
            this.name = name;
            this.counter = counter;
            this.version = version;
        }

        public Record(Versioned<Value> vv) {
            version = vv.getVersion();

            Value value = vv.getValue();
            if (value == null) {
                return;
            }

            name = getFieldFromValue(value, FIELD_NAME);
            String c = getFieldFromValue(value, FIELD_COUNTER);
            if (c != null) {
                counter = Integer.parseInt(c);
            }
        }

        public Version getVersion() {
            return version;
        }

        public Value getValue() {
            return TestMetaStore.makeValue(name, counter);
        }

        public Versioned<Value> getVersionedValue() {
            return new Versioned<Value>(getValue(), version);
        }

        public void merge(String name, Integer counter, Version version) {
            if (name != null) {
                this.name = name;
            }
            if (counter != null) {
                this.counter = counter;
            }
            if (version != null) {
                this.version = version;
            }
        }

        public void merge(Record record) {
            merge(record.name, record.counter, record.version);
        }

        public void checkEqual(Versioned<Value> vv) {
            Version v = vv.getVersion();
            Value value = vv.getValue();

            assertEquals(name, getFieldFromValue(value, FIELD_NAME));

            String c = getFieldFromValue(value, FIELD_COUNTER);
            if (counter == null) {
                assertNull(c);
            } else {
                assertEquals(counter.toString(), c);
            }

            assertTrue(isEqualVersion(version, v));
        }

    }

    protected MetaStore metastore;
    protected MetastoreScannableTable myActualTable;
    protected MetastoreScannableTableAsyncToSyncConverter myTable;

    protected String getMetaStoreName() {
        return InMemoryMetaStore.class.getName();
    }

    protected Configuration getConfiguration() {
        return new CompositeConfiguration();
    }

    protected Version newBadVersion() {
        return new MetadataVersion(-1);
    }

    protected Version nextVersion(Version version) {
        if (Version.NEW == version) {
            return new MetadataVersion(0);
        }
        if (Version.ANY == version) {
            return Version.ANY;
        }
        assertTrue(version instanceof MetadataVersion);
        return new MetadataVersion(((MetadataVersion) version).incrementVersion());
    }

    private void checkVersion(Version v) {
        assertNotNull(v);
        if (v != Version.NEW && v != Version.ANY) {
            assertTrue(v instanceof MetadataVersion);
        }
    }

    protected boolean isEqualVersion(Version v1, Version v2) {
        checkVersion(v1);
        checkVersion(v2);
        return v1.compare(v2) == Version.Occurred.CONCURRENTLY;
    }

    @Before
    public void setUp() throws Exception {
        metastore = MetastoreFactory.createMetaStore(getMetaStoreName());
        Configuration config = getConfiguration();
        metastore.init(config, metastore.getVersion());

        myActualTable = metastore.createScannableTable(TABLE);
        myTable = new MetastoreScannableTableAsyncToSyncConverter(myActualTable);

        // setup a clean environment
        clearTable();
    }

    @After
    public void tearDown() throws Exception {
        // also clear table after test
        clearTable();

        myActualTable.close();
        metastore.close();
    }

    void checkExpectedValue(Versioned<Value> vv, String expectedName,
                            Integer expectedCounter, Version expectedVersion) {
        Record expected = new Record(expectedName, expectedCounter, expectedVersion);
        expected.checkEqual(vv);
    }

    protected Integer getRandom() {
        return (int) (Math.random() * 65536);
    }

    protected Versioned<Value> getRecord(String recordId) throws Exception {
        try {
            return myTable.get(recordId);
        } catch (MSException.NoKeyException nke) {
            return null;
        }
    }

    /**
     * get record with specific fields, assume record EXIST!
     */
    protected Versioned<Value> getExistRecordFields(String recordId, Set<String> fields)
    throws Exception {
        Versioned<Value> retValue = myTable.get(recordId, fields);
        return retValue;
    }

    /**
     * put and check fields.
     */
    protected void putAndCheck(String recordId, String name,
                                      Integer counter, Version version,
                                      Record expected, Code expectedCode)
    throws Exception {
        Version retVersion = null;
        Code code = Code.OperationFailure;
        try {
            retVersion = myTable.put(recordId, makeValue(name, counter), version);
            code = Code.OK;
        } catch (MSException.BadVersionException bve) {
            code = Code.BadVersion;
        } catch (MSException.NoKeyException nke) {
            code = Code.NoKey;
        } catch (MSException.KeyExistsException kee) {
            code = Code.KeyExists;
        }
        assertEquals(expectedCode, code);

        // get and check all fields of record
        if (Code.OK == code) {
            assertTrue(isEqualVersion(retVersion, nextVersion(version)));
            expected.merge(name, counter, retVersion);
        }

        Versioned<Value> existedVV = getRecord(recordId);
        if (null == expected) {
            assertNull(existedVV);
        } else {
            expected.checkEqual(existedVV);
        }
    }

    protected void clearTable() throws Exception {
        MetastoreCursor cursor = myTable.openCursor();
        if (!cursor.hasMoreEntries()) {
            return;
        }
        while (cursor.hasMoreEntries()) {
            Iterator<MetastoreTableItem> iter = cursor.readEntries(99);
            while (iter.hasNext()) {
                MetastoreTableItem item = iter.next();
                String key = item.getKey();
                myTable.remove(key, Version.ANY);
            }
        }
        cursor.close();
    }

    /**
     * Test (get, get partial field, remove) on non-existent element.
     */
    @Test
    public void testNonExistent() throws Exception {
        // get
        try {
            myTable.get(RECORDID);
            fail("Should fail to get a non-existent key");
        } catch (MSException.NoKeyException nke) {
        }

        // get partial field
        Set<String> fields =
            new HashSet<String>(Collections.singletonList(FIELD_COUNTER));
        try {
            myTable.get(RECORDID, fields);
            fail("Should fail to get a non-existent key with specified fields");
        } catch (MSException.NoKeyException nke) {
        }

        // remove
        try {
            myTable.remove(RECORDID, Version.ANY);
            fail("Should fail to delete a non-existent key");
        } catch (MSException.NoKeyException nke) {
        }
    }

    /**
     * Test usage of get operation on (full and partial) fields.
     */
    @Test
    public void testGet() throws Exception {
        Versioned<Value> vv;

        final Set<String> fields =
            new HashSet<String>(Collections.singletonList(FIELD_NAME));

        final String name = "get";
        final Integer counter = getRandom();

        // put test item
        Version version = myTable.put(RECORDID, makeValue(name, counter), Version.NEW);
        assertNotNull(version);

        // fetch with all fields
        vv = getExistRecordFields(RECORDID, ALL_FIELDS);
        checkExpectedValue(vv, name, counter, version);

        // partial get name
        vv = getExistRecordFields(RECORDID, fields);
        checkExpectedValue(vv, name, null, version);

        // non fields
        vv = getExistRecordFields(RECORDID, NON_FIELDS);
        checkExpectedValue(vv, null, null, version);

        // get null key should fail
        try {
            getExistRecordFields(null, NON_FIELDS);
            fail("Should fail to get null key with NON fields");
        } catch (MSException.IllegalOpException ioe) {
        }
        try {
            getExistRecordFields(null, ALL_FIELDS);
            fail("Should fail to get null key with ALL fields.");
        } catch (MSException.IllegalOpException ioe) {
        }
        try {
            getExistRecordFields(null, fields);
            fail("Should fail to get null key with fields " + fields);
        } catch (MSException.IllegalOpException ioe) {
        }
    }

    /**
     * Test usage of put operation with (full and partial) fields.
     */
    @Test
    public void testPut() throws Exception {
        final Integer counter = getRandom();
        final String name = "put";

        Version version;

        /**
         * test correct version put
         */
        // put test item
        version = myTable.put(RECORDID, makeValue(name, counter), Version.NEW);
        assertNotNull(version);
        Record expected = new Record(name, counter, version);

        // correct version put with only name field changed
        putAndCheck(RECORDID, "name1", null, expected.getVersion(), expected, Code.OK);

        // correct version put with only counter field changed
        putAndCheck(RECORDID, null, counter + 1, expected.getVersion(), expected, Code.OK);

        // correct version put with all fields filled
        putAndCheck(RECORDID, "name2", counter + 2, expected.getVersion(), expected, Code.OK);

        // test put exist entry with Version.ANY
        checkPartialPut("put exist entry with Version.ANY", Version.ANY, expected, Code.OK);

        /**
         * test bad version put
         */
        // put to existed entry with Version.NEW
        badVersionedPut(Version.NEW, Code.KeyExists);
        // put to existed entry with bad version
        badVersionedPut(newBadVersion(), Code.BadVersion);

        // remove the entry
        myTable.remove(RECORDID, Version.ANY);

        // put to non-existent entry with bad version
        badVersionedPut(newBadVersion(), Code.NoKey);
        // put to non-existent entry with Version.ANY
        badVersionedPut(Version.ANY, Code.NoKey);

        /**
         * test illegal arguments
         */
        illegalPut(null, Version.NEW);
        illegalPut(makeValue("illegal value", getRandom()), null);
        illegalPut(null, null);
    }

    protected void badVersionedPut(Version badVersion, Code expectedCode) throws Exception {
        Versioned<Value> vv = getRecord(RECORDID);
        Record expected = null;

        if (expectedCode != Code.NoKey) {
            assertNotNull(vv);
            expected = new Record(vv);
        }

        checkPartialPut("badVersionedPut", badVersion, expected, expectedCode);
    }

    protected void checkPartialPut(String name, Version version, Record expected, Code expectedCode)
            throws Exception {
        Integer counter;

        // bad version put with all fields filled
        counter = getRandom();
        putAndCheck(RECORDID, name + counter, counter, version, expected, expectedCode);

        // bad version put with only name field changed
        counter = getRandom();
        putAndCheck(RECORDID, name + counter, null, version, expected, expectedCode);

        // bad version put with only counter field changed
        putAndCheck(RECORDID, null, counter, version, expected, expectedCode);
    }

    protected void illegalPut(Value value, Version version) throws MSException {
        try {
            myTable.put(RECORDID, value, version);
            fail("Should fail to do versioned put with illegal arguments");
        } catch (MSException.IllegalOpException ioe) {
        }
    }

    /**
     * Test usage of (unconditional remove, BadVersion remove, CorrectVersion
     * remove) operation.
     */
    @Test
    public void testRemove() throws Exception {
        final Integer counter = getRandom();
        final String name = "remove";
        Version version;

        // insert test item
        version = myTable.put(RECORDID, makeValue(name, counter), Version.NEW);
        assertNotNull(version);

        // test unconditional remove
        myTable.remove(RECORDID, Version.ANY);

        // insert test item
        version = myTable.put(RECORDID, makeValue(name, counter), Version.NEW);
        assertNotNull(version);

        // test remove with bad version
        try {
            myTable.remove(RECORDID, Version.NEW);
            fail("Should fail to remove a given key with bad version");
        } catch (MSException.BadVersionException bve) {
        }
        try {
            myTable.remove(RECORDID, newBadVersion());
            fail("Should fail to remove a given key with bad version");
        } catch (MSException.BadVersionException bve) {
        }

        // test remove with correct version
        myTable.remove(RECORDID, version);
    }

    protected void openCursorTest(MetastoreCursor cursor, Map<String, Value> expectedValues,
                                  int numEntriesPerScan) throws Exception {
        try {
            Map<String, Value> entries = Maps.newHashMap();
            while (cursor.hasMoreEntries()) {
                Iterator<MetastoreTableItem> iter = cursor.readEntries(numEntriesPerScan);
                while (iter.hasNext()) {
                    MetastoreTableItem item = iter.next();
                    entries.put(item.getKey(), item.getValue().getValue());
                }
            }
            MapDifference<String, Value> diff = Maps.difference(expectedValues, entries);
            assertTrue(diff.areEqual());
        } finally {
            cursor.close();
        }
    }

    void openRangeCursorTest(String firstKey, boolean firstInclusive,
                             String lastKey, boolean lastInclusive,
                             Order order, Set<String> fields,
                             Iterator<Map.Entry<String, Value>> expectedValues,
                             int numEntriesPerScan) throws Exception {
        try (MetastoreCursor cursor =
                     myTable.openCursor(firstKey, firstInclusive, lastKey, lastInclusive, order, fields)) {
            while (cursor.hasMoreEntries()) {
                Iterator<MetastoreTableItem> iter = cursor.readEntries(numEntriesPerScan);
                while (iter.hasNext()) {
                    assertTrue(expectedValues.hasNext());
                    MetastoreTableItem item = iter.next();
                    Map.Entry<String, Value> expectedItem = expectedValues.next();
                    assertEquals(expectedItem.getKey(), item.getKey());
                    assertEquals(expectedItem.getValue(), item.getValue().getValue());
                }
            }
            assertFalse(expectedValues.hasNext());
        }
    }

    /**
     * Test usage of (scan) operation on (full and partial) fields.
     */
    @Test
    public void testOpenCursor() throws Exception {

        TreeMap<String, Value> allValues = Maps.newTreeMap();
        TreeMap<String, Value> partialValues = Maps.newTreeMap();
        TreeMap<String, Value> nonValues = Maps.newTreeMap();

        Set<String> counterFields = Sets.newHashSet(FIELD_COUNTER);

        for (int i = 5; i < 24; i++) {
            char c = (char) ('a' + i);
            String key = String.valueOf(c);
            Value v = makeValue("value" + i, i);
            Value cv = v.project(counterFields);
            Value nv = v.project(NON_FIELDS);

            myTable.put(key, new Value(v), Version.NEW);
            allValues.put(key, v);
            partialValues.put(key, cv);
            nonValues.put(key, nv);
        }

        // test open cursor
        MetastoreCursor cursor = myTable.openCursor(ALL_FIELDS);
        openCursorTest(cursor, allValues, 7);

        cursor = myTable.openCursor(counterFields);
        openCursorTest(cursor, partialValues, 7);

        cursor = myTable.openCursor(NON_FIELDS);
        openCursorTest(cursor, nonValues, 7);

        // test order inclusive exclusive
        Iterator<Map.Entry<String, Value>> expectedIterator;

        expectedIterator = allValues.subMap("l", true, "u", true).entrySet().iterator();
        openRangeCursorTest("l", true, "u", true, Order.ASC, ALL_FIELDS, expectedIterator, 7);

        expectedIterator = allValues.descendingMap().subMap("u", true, "l", true)
                           .entrySet().iterator();
        openRangeCursorTest("u", true, "l", true, Order.DESC, ALL_FIELDS, expectedIterator, 7);

        expectedIterator = allValues.subMap("l", false, "u", false).entrySet().iterator();
        openRangeCursorTest("l", false, "u", false, Order.ASC, ALL_FIELDS, expectedIterator, 7);

        expectedIterator = allValues.descendingMap().subMap("u", false, "l", false)
                           .entrySet().iterator();
        openRangeCursorTest("u", false, "l", false, Order.DESC, ALL_FIELDS, expectedIterator, 7);

        expectedIterator = allValues.subMap("l", true, "u", false).entrySet().iterator();
        openRangeCursorTest("l", true, "u", false, Order.ASC, ALL_FIELDS, expectedIterator, 7);

        expectedIterator = allValues.descendingMap().subMap("u", true, "l", false)
                           .entrySet().iterator();
        openRangeCursorTest("u", true, "l", false, Order.DESC, ALL_FIELDS, expectedIterator, 7);

        expectedIterator = allValues.subMap("l", false, "u", true).entrySet().iterator();
        openRangeCursorTest("l", false, "u", true, Order.ASC, ALL_FIELDS, expectedIterator, 7);

        expectedIterator = allValues.descendingMap().subMap("u", false, "l", true)
                           .entrySet().iterator();
        openRangeCursorTest("u", false, "l", true, Order.DESC, ALL_FIELDS, expectedIterator, 7);

        // test out of range
        String firstKey = "f";
        String lastKey = "x";
        expectedIterator = allValues.subMap(firstKey, true, lastKey, true)
                           .entrySet().iterator();
        openRangeCursorTest("a", true, "z", true, Order.ASC, ALL_FIELDS, expectedIterator, 7);

        expectedIterator = allValues.subMap("l", true, lastKey, true).entrySet().iterator();
        openRangeCursorTest("l", true, "z", true, Order.ASC, ALL_FIELDS, expectedIterator, 7);

        expectedIterator = allValues.subMap(firstKey, true, "u", true).entrySet().iterator();
        openRangeCursorTest("a", true, "u", true, Order.ASC, ALL_FIELDS, expectedIterator, 7);

        // test EMPTY_START_KEY and EMPTY_END_KEY
        expectedIterator = allValues.subMap(firstKey, true, "u", true).entrySet().iterator();
        openRangeCursorTest(EMPTY_START_KEY, true, "u", true, Order.ASC, ALL_FIELDS,
                            expectedIterator, 7);

        expectedIterator = allValues.descendingMap().subMap(lastKey, true, "l", true)
                           .entrySet().iterator();
        openRangeCursorTest(EMPTY_END_KEY, true, "l", true, Order.DESC, ALL_FIELDS,
                            expectedIterator, 7);

        // test illegal arguments
        try {
            myTable.openCursor("a", true, "z", true, Order.DESC, ALL_FIELDS);
            fail("Should fail with wrong range");
        } catch (MSException.IllegalOpException ioe) {
        }
        try {
            myTable.openCursor("z", true, "a", true, Order.ASC, ALL_FIELDS);
            fail("Should fail with wrong range");
        } catch (MSException.IllegalOpException ioe) {
        }
        try {
            myTable.openCursor("a", true, "z", true, null, ALL_FIELDS);
            fail("Should fail with null order");
        } catch (MSException.IllegalOpException ioe) {
        }
    }

}
