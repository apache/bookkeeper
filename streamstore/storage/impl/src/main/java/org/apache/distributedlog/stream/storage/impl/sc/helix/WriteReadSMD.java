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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.StateModelDefinition;

/**
 * The state model definition for write-read transition.
 */
public final class WriteReadSMD extends StateModelDefinition {

  public static final String NAME = "WriteRead";

  /**
   * The state for write and read.
   */
  public enum States {
    WRITE,
    READ,
    OFFLINE
  }

  public WriteReadSMD() {
    super(generateConfigForWriteRead());
  }

  /**
   * Build Write-Read state model definition.
   *
   * @return State model definition.
   */
  public static StateModelDefinition build() {
    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(NAME);
    // init state
    builder.initialState(States.OFFLINE.name());

    // add states
    builder.addState(States.WRITE.name(), 0);
    builder.addState(States.READ.name(), 1);
    builder.addState(States.OFFLINE.name(), 2);
    for (HelixDefinedState state : HelixDefinedState.values()) {
      builder.addState(state.name());
    }

    // add transitions
    builder.addTransition(States.WRITE.name(), States.READ.name(), 0);
    builder.addTransition(States.READ.name(), States.WRITE.name(), 1);
    builder.addTransition(States.OFFLINE.name(), States.READ.name(), 2);
    builder.addTransition(States.READ.name(), States.OFFLINE.name(), 3);
    builder.addTransition(States.OFFLINE.name(), HelixDefinedState.DROPPED.name());

    // bounds
    builder.upperBound(States.WRITE.name(), 1);
    builder.dynamicUpperBound(States.READ.name(), "R");

    return builder.build();
  }

  /**
   * Generate Write-slave state model definition.
   *
   * @return ZNRecord.
   */
  public static ZNRecord generateConfigForWriteRead() {
    ZNRecord record = new ZNRecord("WriteRead");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("WRITE");
    statePriorityList.add("READ");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("DROPPED");
    statePriorityList.add("ERROR");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
        statePriorityList);
    for (String state : statePriorityList) {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("WRITE")) {
        metadata.put("count", "1");
        record.setMapField(key, metadata);
      } else if (state.equals("READ")) {
        metadata.put("count", "R");
        record.setMapField(key, metadata);
      } else if (state.equals("OFFLINE")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      } else if (state.equals("DROPPED")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      } else if (state.equals("ERROR")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
    }
    for (String state : statePriorityList) {
      String key = state + ".next";
      if (state.equals("WRITE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("READ", "READ");
        metadata.put("OFFLINE", "READ");
        metadata.put("DROPPED", "READ");
        record.setMapField(key, metadata);
      } else if (state.equals("READ")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("WRITE", "WRITE");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      } else if (state.equals("OFFLINE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("READ", "READ");
        metadata.put("WRITE", "READ");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      } else if (state.equals("ERROR")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        record.setMapField(key, metadata);
      }
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("WRITE-READ");
    stateTransitionPriorityList.add("READ-WRITE");
    stateTransitionPriorityList.add("OFFLINE-READ");
    stateTransitionPriorityList.add("READ-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-DROPPED");
    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
        stateTransitionPriorityList);
    return record;
  }
}
