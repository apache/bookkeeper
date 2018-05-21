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

package org.apache.bookkeeper.tests.integration.stream;

import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.utils.NetUtils;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.tests.integration.cluster.BookKeeperClusterTestBase;
import org.apache.bookkeeper.tests.integration.topologies.BKClusterSpec;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Similar as {@link org.apache.bookkeeper.tests.integration.cluster.BookKeeperClusterTestBase},
 * but enabled stream storage for testing stream storage related features.
 */
@Slf4j
public abstract class StreamClusterTestBase extends BookKeeperClusterTestBase {

    @BeforeClass
    public static void setupCluster() throws Exception {
        BKClusterSpec spec = BKClusterSpec.builder()
            .clusterName(RandomStringUtils.randomAlphabetic(8))
            .numBookies(3)
            .extraServerComponents("org.apache.bookkeeper.stream.server.StreamStorageLifecycleComponent")
            .build();
        BookKeeperClusterTestBase.setupCluster(spec);
    }

    @AfterClass
    public static void teardownCluster() {
        BookKeeperClusterTestBase.teardownCluster();
    }

    protected static int getNumBookies() {
        return bkCluster.getBookieContainers().size();
    }

    protected static List<Endpoint> getExsternalStreamEndpoints() {
        return bkCluster.getBookieContainers().values().stream()
            .map(container ->
                NetUtils.parseEndpoint(container.getExternalGrpcEndpointStr()))
            .collect(Collectors.toList());
    }

    protected static List<Endpoint> getInternalStreamEndpoints() {
        return bkCluster.getBookieContainers().values().stream()
            .map(container ->
                NetUtils.parseEndpoint(container.getInternalGrpcEndpointStr()))
            .collect(Collectors.toList());
    }


}
