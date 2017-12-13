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

import org.apache.commons.configuration.Configuration;

/**
 * Metadata Store Interface.
 */
public interface MetaStore {
    /**
     * Return the name of the plugin.
     *
     * @return the plugin name.
     */
    String getName();

    /**
     * Get the plugin verison.
     *
     * @return the plugin version.
     */
    int getVersion();

    /**
     * Initialize the meta store.
     *
     * @param config
     *          Configuration object passed to metastore
     * @param msVersion
     *          Version to initialize the metastore
     * @throws MetastoreException when failed to initialize
     */
    void init(Configuration config, int msVersion) throws MetastoreException;

    /**
     * Close the meta store.
     */
    void close();

    /**
     * Create a metastore table.
     *
     * @param name
     *          Table name.
     * @return a metastore table
     * @throws MetastoreException when failed to create the metastore table.
     */
    MetastoreTable createTable(String name) throws MetastoreException;

    /**
     * Create a scannable metastore table.
     *
     * @param name
     *          Table name.
     * @return a metastore scannable table
     * @throws MetastoreException when failed to create the metastore table.
     */
    MetastoreScannableTable createScannableTable(String name) throws MetastoreException;

}
