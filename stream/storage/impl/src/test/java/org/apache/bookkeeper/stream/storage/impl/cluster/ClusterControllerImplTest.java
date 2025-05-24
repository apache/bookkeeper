/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.stream.storage.impl.cluster;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterController;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterControllerLeader;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterControllerLeaderSelector;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterMetadataStore;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerController;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link ClusterControllerImpl}.
 */
public class ClusterControllerImplTest {

    private ClusterControllerLeaderSelector leaderSelector;

    private ClusterController service;

    @Before
    public void setup() {
        this.leaderSelector = mock(ClusterControllerLeaderSelector.class);
        this.service = new ClusterControllerImpl(
            mock(ClusterMetadataStore.class),
            mock(RegistrationClient.class),
            mock(StorageContainerController.class),
            leaderSelector,
            new StorageConfiguration(new CompositeConfiguration()));
    }

    @Test
    public void testInitialize() {
        verify(leaderSelector, times(1))
            .initialize(any(ClusterControllerLeader.class));
    }

    @Test
    public void testStart() {
        service.start();
        verify(leaderSelector, times(1)).start();
    }

    @Test
    public void testStop() {
        service.start();
        service.stop();
        verify(leaderSelector, times(1)).close();
    }

}
