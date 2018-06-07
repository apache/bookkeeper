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

package org.apache.bookkeeper.stream.storage.api.cluster;

import java.net.URI;

/**
 * Initializing cluster metadata.
 */
public interface ClusterInitializer {

    /**
     * Retrieves whether the initializer thinks that it can initialize the metadata service
     * specified by the given {@code metadataServiceUri}. Typically the implementations will
     * return <tt>true</tt> if they understand the subprotocol specified in the URI and
     * <tt>false</tt> if they do not.
     *
     * @param metatadataServiceUri the metadata service uri
     * @return <tt>true</tt> if the implementation understands the given URI; <tt>false</tt> otherwise.
     */
    boolean acceptsURI(URI metatadataServiceUri);

    /**
     * Create a new cluster under metadata service specified by {@code metadataServiceUri}.
     *
     * @param metadataServiceUri metadata service uri
     * @param numStorageContainers number storage containers
     * @return true if successfully initialized cluster; otherwise false.
     */
    boolean initializeCluster(URI metadataServiceUri, int numStorageContainers);

}
