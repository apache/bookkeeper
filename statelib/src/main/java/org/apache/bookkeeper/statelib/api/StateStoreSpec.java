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

package org.apache.bookkeeper.statelib.api;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Singular;
import org.apache.bookkeeper.common.coder.Coder;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;

/**
 * Specification for a state store.
 */
@Builder
@Getter
public class StateStoreSpec {

    private String name;
    private Coder<?> keyCoder;
    private Coder<?> valCoder;
    private File localStateStoreDir;
    private String stream;
    private boolean isReadonly;
    private ScheduledExecutorService writeIOScheduler;
    private ScheduledExecutorService readIOScheduler;
    private ScheduledExecutorService checkpointIOScheduler;
    private CheckpointStore checkpointStore;
    @Default
    private Duration checkpointDuration = Duration.ofMinutes(1);
    @Singular
    private Map<String, Object> configs;

}
