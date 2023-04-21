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

package org.apache.bookkeeper.tests.integration.topologies;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Spec to build {@link BKCluster}.
 */
@Builder
@Accessors(fluent = true)
@Getter
@Setter
public class BKClusterSpec {

    /**
     * Returns the cluster name.
     *
     * @return the cluster name.
     */
    String clusterName;

    /**
     * Returns the number of bookies.
     *
     * @return the number of bookies;
     */
    @Default
    int numBookies = 0;

    /**
     * Returns the extra server components.
     *
     * @return the extra server components.
     */
    @Default
    String extraServerComponents = "";

    /**
     * Returns the flag whether to enable/disable container log.
     *
     * @return the flag whether to enable/disable container log.
     */
    @Default
    boolean enableContainerLog = false;

}
