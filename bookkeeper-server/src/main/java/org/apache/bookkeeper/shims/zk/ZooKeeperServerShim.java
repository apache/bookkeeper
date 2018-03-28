/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.shims.zk;

import java.io.File;
import java.io.IOException;

/**
 * In order to be compatible with multiple versions of ZooKeeper.
 * All parts of the ZooKeeper Server that are not cross-version
 * compatible are encapsulated in an implementation of this class.
 */
public interface ZooKeeperServerShim {

    /**
     * Initialize zookeeper server.
     *
     * @param snapDir
     *          Snapshot Dir.
     * @param logDir
     *          Log Dir.
     * @param zkPort
     *          ZooKeeper Port.
     * @param maxCC
     *          Max Concurrency for Client.
     * @throws IOException when failed to initialize zookeeper server.
     */
    void initialize(File snapDir, File logDir, int zkPort, int maxCC) throws IOException;

    /**
     * Start the zookeeper server.
     *
     * @throws IOException when failed to start zookeeper server.
     */
    void start() throws IOException;

    /**
     * Stop the zookeeper server.
     */
    void stop();

}
