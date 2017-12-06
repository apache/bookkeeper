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

import org.apache.bookkeeper.versioning.Versioned;

/**
 * Identify an item in a metastore table.
 */
public class MetastoreTableItem {

    private String key;
    private Versioned<Value> value;

    public MetastoreTableItem(String key, Versioned<Value> value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Get the key of the table item.
     *
     * @return key of table item.
     */
    public String getKey() {
        return key;
    }

    /**
     * Set the key of the item.
     *
     * @param key Key
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Get the value of the item.
     *
     * @return value of the item.
     */
    public Versioned<Value> getValue() {
        return value;
    }

    /**
     * Set the value of the item.
     *
     * @param value of the item.
     */
    public void setValue(Versioned<Value> value) {
        this.value = value;
    }
}
