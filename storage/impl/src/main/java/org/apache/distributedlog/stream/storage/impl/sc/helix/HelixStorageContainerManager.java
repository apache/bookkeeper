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

import static org.apache.distributedlog.stream.storage.impl.sc.helix.HelixStorageController.RESOURCE_NAME;
import static org.apache.distributedlog.stream.storage.impl.sc.helix.HelixStorageController.getEndpointName;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.stream.client.utils.NetUtils;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainerManager;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainerRegistry;
import org.apache.distributedlog.stream.storage.conf.StorageConfiguration;
import org.apache.distributedlog.stream.storage.exceptions.StorageRuntimeException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.spectator.RoutingTableProvider;

/**
 * A storage container manager implemented using Helix.
 */
@Slf4j
public class HelixStorageContainerManager
    extends AbstractLifecycleComponent<StorageConfiguration>
    implements StorageContainerManager {

  private final String zkServers;
  private final String clusterName;
  private final StorageContainerRegistry registry;
  private final Endpoint endpoint;
  private final Optional<String> endpointName;
  private final HelixManager manager;
  private final RoutingTableProvider rtProvider;

  public HelixStorageContainerManager(String zkServers,
                                      String clusterName,
                                      StorageConfiguration conf,
                                      StorageContainerRegistry registry,
                                      Endpoint endpoint,
                                      Optional<String> endpointName,
                                      StatsLogger statsLogger) {
    super("helix-storage-container-manager", conf, statsLogger);
    this.zkServers = zkServers;
    this.clusterName = clusterName;
    this.registry = registry;
    this.endpoint = endpoint;
    this.endpointName = endpointName;
    // create the helix manager
    this.manager = HelixManagerFactory.getZKHelixManager(
      clusterName,
      endpointName.orElse(getEndpointName(endpoint)),
      InstanceType.CONTROLLER_PARTICIPANT,
      zkServers);
    this.rtProvider = new RoutingTableProvider();
  }

  @Override
  public Endpoint getStorageContainer(long scId) {
    List<InstanceConfig> instances = this.rtProvider.getInstances(
      RESOURCE_NAME, RESOURCE_NAME + "_" + scId, "WRITE");
    if (instances.isEmpty()) {
      return null;
    } else {
      InstanceConfig instance = instances.get(0);
      return NetUtils.createEndpoint(
        instance.getHostName(),
        Integer.parseInt(instance.getPort()));
    }
  }

  @Override
  protected void doStart() {
    // create the controller
    try (HelixStorageController controller = new HelixStorageController(zkServers)) {
      controller.addNode(clusterName, endpoint, endpointName);
    }
    StateMachineEngine sme = this.manager.getStateMachineEngine();
    StateModelFactory<StateModel> smFactory = new WriteReadStateModelFactory(registry);
    sme.registerStateModelFactory(WriteReadSMD.NAME, smFactory);
    try {
      manager.connect();
      manager.addExternalViewChangeListener(rtProvider);
    } catch (Exception e) {
      throw new StorageRuntimeException(e);
    }
  }

  @Override
  protected void doStop() {
    manager.disconnect();
  }

  @Override
  protected void doClose() throws IOException {

  }
}
