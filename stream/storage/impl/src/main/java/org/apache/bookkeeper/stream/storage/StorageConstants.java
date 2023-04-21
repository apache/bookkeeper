/*
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
 */
package org.apache.bookkeeper.stream.storage;

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;

/**
 * Defines the storage constants.
 */
public final class StorageConstants {

    private StorageConstants() {}

    //
    // metadata related
    //

    public static final String CONTROLLER_PATH = "controller";
    public static final String SERVERS_PATH = "servers";
    public static final String CLUSTER_METADATA_PATH = "metadata";
    public static final String CLUSTER_ASSIGNMENT_PATH = "assignment";
    public static final String SEGMENTS_PATH = "segments";
    public static final String STORAGE_PATH = "storage";

    //
    // ZooKeeper metadata related.
    //

    public static final String ZK_METADATA_ROOT_PATH = "/stream";

    public static String getControllerPath(String rootPath) {
        return rootPath + "/" + CONTROLLER_PATH;
    }

    public static String getServersPath(String rootPath) {
        return rootPath + "/" + SERVERS_PATH;
    }

    public static String getWritableServersPath(String rootPath) {
        return getServersPath(rootPath) + "/" + AVAILABLE_NODE;
    }

    public static String getClusterMetadataPath(String rootPath) {
        return rootPath + "/" + CLUSTER_METADATA_PATH;
    }

    public static String getClusterAssignmentPath(String rootPath) {
        return rootPath + "/" + CLUSTER_ASSIGNMENT_PATH;
    }

    public static String getSegmentsRootPath(String rootPath) {
        return rootPath + "/" + SEGMENTS_PATH;
    }

    public static String getStoragePath(String rootPath) {
        return rootPath + "/" + STORAGE_PATH;
    }

}
