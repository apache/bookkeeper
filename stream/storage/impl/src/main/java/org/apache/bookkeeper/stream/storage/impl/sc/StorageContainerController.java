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
package org.apache.bookkeeper.stream.storage.impl.sc;

import java.util.Set;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssignmentData;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;

/**
 * Storage container controller is used for assigning containers to servers.
 */
public interface StorageContainerController {

    /**
     * Compute the ideal container assignment state based on hosts alive in the cluster.
     *
     * @param clusterMetadata cluster metadata
     * @param currentState current container assignment state
     * @param currentCluster current servers alive in the cluster
     * @return the compute ideal assignment state
     */
    ClusterAssignmentData computeIdealState(ClusterMetadata clusterMetadata,
                                            ClusterAssignmentData currentState,
                                            Set<BookieSocketAddress> currentCluster);

}
