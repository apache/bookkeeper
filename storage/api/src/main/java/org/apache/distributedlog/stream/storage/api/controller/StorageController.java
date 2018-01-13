/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.stream.storage.api.controller;

import java.util.Optional;
import org.apache.distributedlog.stream.proto.common.Endpoint;

/**
 * Interface for managing the storage cluster.
 */
public interface StorageController extends AutoCloseable {

  /**
   * Create the cluster.
   *
   * @param clusterName cluster name.
   * @param numStorageContainers num storage containers.
   * @param numReplicas num replicas per storage container.
   */
  void createCluster(String clusterName,
                     int numStorageContainers,
                     int numReplicas);

  /**
   * Add a node to a cluster.
   *
   * @param clusterName cluster name.
   * @param endpointName node endpoint name.
   */
  void addNode(String clusterName,
               Endpoint endpoint,
               Optional<String> endpointName);

}
