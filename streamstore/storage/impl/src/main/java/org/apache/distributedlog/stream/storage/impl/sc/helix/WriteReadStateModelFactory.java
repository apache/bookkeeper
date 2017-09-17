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

import static org.apache.distributedlog.stream.storage.impl.sc.helix.HelixStorageController.getStorageContainerFromPartitionName;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainerRegistry;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

/**
 * Storage Container State Model Factory.
 *
 * <p>It is used by helix for managing the state transition.
 */
@Slf4j
public class WriteReadStateModelFactory extends StateModelFactory<StateModel> {

  private final StorageContainerRegistry registry;

  public WriteReadStateModelFactory(StorageContainerRegistry registry) {
    this.registry = registry;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    return new WriteReadStateModel(registry);
  }

  /**
   * The state model for storage container.
   */
  @StateModelInfo(states = "{'OFFLINE','READ','WRITE'}", initialState = "OFFLINE")
  public static class WriteReadStateModel extends StateModel {

    private final StorageContainerRegistry registry;

    WriteReadStateModel(StorageContainerRegistry registry) {
      this.registry = registry;
    }

    @Transition(from = "OFFLINE", to = "READ")
    public void offlineToRead(Message msg,
                              NotificationContext ctx) {
      log.info("----- [OFFLINE --> READ] {} / {}", msg.getResourceName(), msg.getPartitionName());
      // do nothing now
    }

    @Transition(from = "READ", to = "WRITE")
    public void readToWrite(Message msg,
                            NotificationContext ctx) throws Exception {
      log.info("----- [READ --> WRITE] {} / {}", msg.getResourceName(), msg.getPartitionName());
      try {
        FutureUtils.result(registry.startStorageContainer(
          getStorageContainerFromPartitionName(msg.getPartitionName())));
      } catch (Exception e) {
        log.error("----- [READ --> WRITE] {} / {} failed",
          new Object[] { msg.getResourceName(), msg.getPartitionName(), e });
        throw e;
      }
    }

    @Transition(from = "WRITE", to = "READ")
    public void writeToRead(Message msg,
                            NotificationContext ctx) throws Exception {
      log.info("----- [WRITE --> READ] {} / {}", msg.getResourceName(), msg.getPartitionName());
      try {
        FutureUtils.result(registry.stopStorageContainer(
          getStorageContainerFromPartitionName(msg.getPartitionName())));
      } catch (Exception e) {
        log.error("----- [WRITE --> READ] {} / {} failed",
          new Object[] { msg.getResourceName(), msg.getPartitionName(), e });
        throw e;
      }
    }

    @Transition(from = "READ", to = "OFFLINE")
    public void readToOffline(Message msg,
                              NotificationContext ctx) {
      log.info("----- [READ --> OFFLINE] {} / {}", msg.getResourceName(), msg.getPartitionName());
      // do nothing now
    }

  }
}
