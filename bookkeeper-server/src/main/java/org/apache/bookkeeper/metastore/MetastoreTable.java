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

import java.util.Collections;
import java.util.Set;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Metastore Table interface.
 */
public interface MetastoreTable {

    // select all fields when reading or scanning entries
    Set<String> ALL_FIELDS = null;
    // select non fields to return when reading/scanning entries
    Set<String> NON_FIELDS = Collections.emptySet();

    /**
     * Get table name.
     *
     * @return table name
     */
    String getName();

    /**
     * Get all fields of a key.
     *
     * <p>
     * Return Code:<ul>
     * <li>{@link MSException.Code.OK}: success returning the key</li>
     * <li>{@link MSException.Code.NoKey}: no key found</li>
     * <li>{@link MSException.Code.IllegalOp}/{@link MSException.Code.ServiceDown}: other issues</li>
     * </ul></p>
     *
     * @param key
     *          Key Name
     * @param cb
     *          Callback to return all fields of the key
     * @param ctx
     *          Callback context
     */
    void get(String key, MetastoreCallback<Versioned<Value>> cb, Object ctx);

    /**
     * Get all fields of a key.
     *
     * <p>
     * Return Code:<ul>
     * <li>{@link MSException.Code.OK}: success returning the key</li>
     * <li>{@link MSException.Code.NoKey}: no key found</li>
     * <li>{@link MSException.Code.IllegalOp}/{@link MSException.Code.ServiceDown}: other issues</li>
     * </ul></p>
     *
     * @param key
     *          Key Name
     * @param watcher
     *          Watcher object to receive notifications
     * @param cb
     *          Callback to return all fields of the key
     * @param ctx
     *          Callback context
     */
    void get(String key, MetastoreWatcher watcher, MetastoreCallback<Versioned<Value>> cb, Object ctx);

    /**
     * Get specified fields of a key.
     *
     * <p>
     * Return Code:<ul>
     * <li>{@link MSException.Code.OK}: success returning the key</li>
     * <li>{@link MSException.Code.NoKey}: no key found</li>
     * <li>{@link MSException.Code.IllegalOp}/{@link MSException.Code.ServiceDown}: other issues</li>
     * </ul></p>
     *
     * @param key
     *          Key Name
     * @param fields
     *          Fields to return
     * @param cb
     *          Callback to return specified fields of the key
     * @param ctx
     *          Callback context
     */
    void get(String key, Set<String> fields, MetastoreCallback<Versioned<Value>> cb, Object ctx);

    /**
     * Update a key according to its version.
     *
     * <p>
     * Return Code:<ul>
     * <li>{@link MSException.Code.OK}: success updating the key</li>
     * <li>{@link MSException.Code.BadVersion}: failed to update the key due to bad version</li>
     * <li>{@link MSException.Code.NoKey}: no key found to update data, if not provided {@link Version.NEW}</li>
     * <li>{@link MSException.Code.KeyExists}: entry exists providing {@link Version.NEW}</li>
     * <li>{@link MSException.Code.IllegalOp}/{@link MSException.Code.ServiceDown}: other issues</li>
     * </ul></p>
     *
     * <p>The key is updated only when the version matches its current version.
     * In particular, if the provided version is:<ul>
     * <li>{@link Version.ANY}: update the data without comparing its version.
     *      <b>Note this usage is not encouraged since it may mess up data consistency.</b></li>
     * <li>{@link Version.NEW}: create the entry if it doesn't exist before;
     *      Otherwise return {@link MSException.Code.KeyExists}.</li>
     * </ul>
     *
     * @param key
     *          Key Name
     * @param value
     *          Value to update.
     * @param version
     *          Version specified to update.
     * @param cb
     *          Callback to return new version after updated.
     * @param ctx
     *          Callback context
     */
    void put(String key, Value value, Version version, MetastoreCallback<Version> cb, Object ctx);

    /**
     * Remove a key by its version.
     *
     * <p>The key is removed only when the version matches its current version.
     * If <code>version</code> is {@link Version.ANY}, the key would be removed directly.
     *
     * <p>
     * Return Code:<ul>
     * <li>{@link MSException.Code.OK}: success updating the key</li>
     * <li>{@link MSException.Code.NoKey}: if the key doesn't exist.</li>
     * <li>{@link MSException.Code.BadVersion}: failed to delete the key due to bad version</li>
     * <li>{@link MSException.Code.IllegalOp}/{@link MSException.Code.ServiceDown}: other issues</li>
     * </ul></p>
     *
     * @param key
     *          Key Name.
     * @param version
     *          Version specified to remove.
     * @param cb
     *          Callback to return all fields of the key
     * @param ctx
     *          Callback context
     */
    void remove(String key, Version version, MetastoreCallback<Void> cb, Object ctx);

    /**
     * Open a cursor to loop over all the entries of the table,
     * which returns all fields for each entry.
     * The returned cursor doesn't need to guarantee any order,
     * since the underlying might be a hash table or an order table.
     *
     * @param cb
     *          Callback to return an opened cursor
     * @param ctx
     *          Callback context
     */
    void openCursor(MetastoreCallback<MetastoreCursor> cb, Object ctx);

    /**
     * Open a cursor to loop over all the entries of the table,
     * which returns the specified <code>fields</code> for each entry.
     * The returned cursor doesn't need to guarantee any order,
     * since the underlying might be a hash table or an order table.
     *
     * @param fields
     *          Fields to select
     * @param cb
     *          Callback to return an opened cursor
     * @param ctx
     *          Callback context
     */
    void openCursor(Set<String> fields, MetastoreCallback<MetastoreCursor> cb, Object ctx);

    /**
     * Close the table.
     */
    void close();
}
