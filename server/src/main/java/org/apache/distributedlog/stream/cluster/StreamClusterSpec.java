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

package org.apache.distributedlog.stream.cluster;

import org.apache.commons.configuration.CompositeConfiguration;
import org.inferred.freebuilder.FreeBuilder;

/**
 * Spec to build {@link StreamCluster}.
 */
@FreeBuilder
public interface StreamClusterSpec {

  /**
   * Returns the number of servers to run in this cluster.
   *
   * @return the number of servers to run in this cluster.
   */
  int numServers();

  /**
   * Returns the configuration used by the servers across the cluster.
   *
   * @return the configuration used by the servers across the cluster.
   */
  CompositeConfiguration baseConf();

  /**
   * Returns the zookeeper servers used in this cluster.
   *
   * @return the zookeeper servers used in this cluster.
   */
  String zkServers();

  /**
   * Returns if should start zookeeper.
   *
   * @return true if should start zookeeper, otherwise false.
   */
  boolean shouldStartZooKeeper();

  /**
   * Returns the zookeeper server port used in this cluster.
   *
   * @return the zookeeper server port used in this cluster.
   */
  int zkPort();

  /**
   * Returns the bookie server port used in this cluster.
   *
   * @return the bookie server port used in this cluster.
   */
  int initialBookiePort();

  /**
   * Returns the gRPC server port used in this cluster.
   *
   * @return the gRPC server port used in this cluster.
   */
  int initialGrpcPort();

  /**
   * Builder to build stream cluster spec.
   */
  class Builder extends StreamClusterSpec_Builder {

    Builder() {
      numServers(3);
      zkServers("127.0.0.1");
      zkPort(2181);
      shouldStartZooKeeper(true);
      initialBookiePort(3181);
      initialGrpcPort(4181);
    }

  }

  static Builder newBuilder() {
    return new Builder();
  }

}
