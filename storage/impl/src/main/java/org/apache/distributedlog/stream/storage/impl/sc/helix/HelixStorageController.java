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

package org.apache.distributedlog.stream.storage.impl.sc.helix;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.apache.distributedlog.stream.storage.api.controller.StorageController;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;

/**
 * A storage controller client based on Helix.
 */
@Slf4j
public class HelixStorageController implements StorageController {

  static final String RESOURCE_NAME = "storagecontainers";

  private final ZKHelixAdmin admin;

  public HelixStorageController(String zkServers) {
    this.admin = new ZKHelixAdmin(zkServers);
  }

  @VisibleForTesting
  ZKHelixAdmin getAdmin() {
    return admin;
  }

  static long getStorageContainerFromPartitionName(String partitionName) {
    return Long.parseLong(partitionName.replace(RESOURCE_NAME + "_", ""));
  }

  @Override
  public void createCluster(String clusterName,
                            int numStorageContainers,
                            int numReplicas) {
    List<String> clusters = admin.getClusters();
    if (clusters.contains(clusterName)) {
      return;
    }
    log.info("Creating new cluster : {}", clusterName);

    admin.addCluster(clusterName);
    admin.addStateModelDef(clusterName, "WriteRead", WriteReadSMD.build());
    admin.addResource(
      clusterName,
      RESOURCE_NAME,
      numStorageContainers,
      WriteReadSMD.NAME,
      "FULL_AUTO");
    admin.rebalance(clusterName, RESOURCE_NAME, 3);

    log.info("Created new cluster : {}", clusterName);
  }

  static String getEndpointName(Endpoint endpoint) {
    return endpoint.getHostname() + "_" + endpoint.getPort();
  }

  @Override
  public void addNode(String clusterName,
                      Endpoint endpoint,
                      Optional<String> endpointNameOptional) {
    String endpointName = endpointNameOptional.orElse(getEndpointName(endpoint));
    if (admin.getInstancesInCluster(clusterName)
        .contains(endpointName)) {
      log.info("Instance {} already exists in cluster {}, skip creating the instance",
        endpointName, clusterName);
      return;
    }
    log.info("Adding a new instance {} ({}) to the cluster {}.",
      new Object[] { endpointName, endpoint, clusterName });
    InstanceConfig config = new InstanceConfig(endpointName);
    config.setHostName(endpoint.getHostname());
    config.setPort(Integer.toString(endpoint.getPort()));
    admin.addInstance(clusterName, config);
  }

  @Override
  public void close() {
    admin.close();
  }
}
