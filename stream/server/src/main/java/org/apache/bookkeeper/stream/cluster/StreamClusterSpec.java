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

package org.apache.bookkeeper.stream.cluster;

import java.io.File;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.commons.configuration.CompositeConfiguration;

/**
 * Spec to build {@link StreamCluster}.
 */
@Builder
@Accessors(fluent = true)
@Getter
@Setter
public class StreamClusterSpec {

    /**
     * Returns the number of servers to run in this cluster.
     *
     * @return the number of servers to run in this cluster.
     */
    @Default
    int numServers = 3;

    /**
     * Returns the configuration used by the servers across the cluster.
     *
     * @return the configuration used by the servers across the cluster.
     */
    CompositeConfiguration baseConf;

    /**
     * Returns the metadata service uri that the servers will connect to.
     *
     * @return the metadata service uri that the servers will connect to.
     */
    @Default
    ServiceURI metadataServiceUri = null;

    /**
     * Returns if should start zookeeper.
     *
     * @return true if should start zookeeper, otherwise false.
     */
    @Default
    boolean shouldStartZooKeeper = true;

    /**
     * Returns the zookeeper server port used in this cluster.
     *
     * @return the zookeeper server port used in this cluster.
     */
    @Default
    int zkPort = 2181;

    /**
     * Returns the bookie server port used in this cluster.
     *
     * @return the bookie server port used in this cluster.
     */
    @Default
    int initialBookiePort = 3181;

    /**
     * Returns the gRPC server port used in this cluster.
     *
     * @return the gRPC server port used in this cluster.
     */
    @Default
    int initialGrpcPort = 4181;

    /**
     * The root dir used by the stream storage to store data.
     *
     * @return the root dir used by the stream storage to store data.
     */
    @Default
    File storageRootDir = new File("data/bookkeeper");

}
