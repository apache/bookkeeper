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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.metastore.MSException.Code;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Converts async calls to sync calls for MetastoreTable. Currently not
 * intended to be used other than for simple functional tests, however,
 * could be developed into a sync API.
 */
public class MetastoreTableAsyncToSyncConverter {

    static class HeldValue<T> implements MetastoreCallback<T> {
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        private int code;
        private T value = null;

        void waitCallback() throws MSException {
            try {
                countDownLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw MSException.create(Code.InterruptedException);
            }

            if (Code.OK.getCode() != code) {
                throw MSException.create(Code.get(code));
            }
        }

        public T getValue() {
            return value;
        }

        @Override
        public void complete(int rc, T value, Object ctx) {
            this.code = rc;
            this.value = value;
            countDownLatch.countDown();
        }
    }

    protected MetastoreTable table;

    public MetastoreTableAsyncToSyncConverter(MetastoreTable table) {
        this.table = table;
    }

    public Versioned<Value> get(String key) throws MSException {
        HeldValue<Versioned<Value>> retValue =
            new HeldValue<Versioned<Value>>();

        // make the actual async call
        this.table.get(key, retValue, null);

        retValue.waitCallback();
        return retValue.getValue();
    }

    public Versioned<Value> get(String key, Set<String> fields)
    throws MSException {
        HeldValue<Versioned<Value>> retValue =
            new HeldValue<Versioned<Value>>();

        // make the actual async call
        this.table.get(key, fields, retValue, null);

        retValue.waitCallback();
        return retValue.getValue();
    }

    public void remove(String key, Version version) throws MSException {
        HeldValue<Void> retValue = new HeldValue<Void>();

        // make the actual async call
        this.table.remove(key, version, retValue, null);

        retValue.waitCallback();
    }

    public Version put(String key, Value value, Version version)
    throws MSException {
        HeldValue<Version> retValue = new HeldValue<Version>();

        // make the actual async call
        this.table.put(key, value, version, retValue, null);

        retValue.waitCallback();
        return retValue.getValue();
    }

    public MetastoreCursor openCursor() throws MSException {
        HeldValue<MetastoreCursor> retValue = new HeldValue<MetastoreCursor>();
        // make the actual async call
        this.table.openCursor(retValue, null);
        retValue.waitCallback();
        return retValue.getValue();
    }

    public MetastoreCursor openCursor(Set<String> fields) throws MSException {
        HeldValue<MetastoreCursor> retValue = new HeldValue<MetastoreCursor>();
        // make the actual async call
        this.table.openCursor(fields, retValue, null);
        retValue.waitCallback();
        return retValue.getValue();
    }

}
