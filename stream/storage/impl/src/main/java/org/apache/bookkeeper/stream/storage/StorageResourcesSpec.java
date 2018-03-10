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
package org.apache.bookkeeper.stream.storage;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Spec to define resources used by storage service.
 */
@Builder
@Accessors(fluent = true)
@Getter
@Setter
public class StorageResourcesSpec {

    @Default
    private int numSchedulerThreads = Runtime.getRuntime().availableProcessors() * 2;
    @Default
    private int numIOWriteThreads = Runtime.getRuntime().availableProcessors() * 2;
    @Default
    private int numIOReadThreads = Runtime.getRuntime().availableProcessors() * 2;
    @Default
    private int numCheckpointThreads = Runtime.getRuntime().availableProcessors() * 2;

}
