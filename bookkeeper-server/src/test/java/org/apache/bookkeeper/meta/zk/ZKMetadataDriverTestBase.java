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
package org.apache.bookkeeper.meta.zk;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Unit test of {@link ZKMetadataDriverBase}.
 */
public abstract class ZKMetadataDriverTestBase {

    protected AbstractConfiguration<?> conf;
    protected String ledgersRootPath;
    protected String metadataServiceUri;
    protected ZooKeeperClient.Builder mockZkBuilder;
    protected ZooKeeperClient mockZkc;
    protected MockedStatic<ZooKeeperClient> zooKeeperClientMockedStatic;

    public void setup(AbstractConfiguration<?> conf) throws Exception {
        ledgersRootPath = "/path/to/ledgers";
        metadataServiceUri = "zk://127.0.0.1" + ledgersRootPath;
        this.conf = conf;
        conf.setMetadataServiceUri(metadataServiceUri);

        this.mockZkBuilder = mock(ZooKeeperClient.Builder.class);
        when(mockZkBuilder.connectString(eq("127.0.0.1"))).thenReturn(mockZkBuilder);
        when(mockZkBuilder.sessionTimeoutMs(anyInt())).thenReturn(mockZkBuilder);
        when(mockZkBuilder.operationRetryPolicy(any(RetryPolicy.class)))
            .thenReturn(mockZkBuilder);
        when(mockZkBuilder.requestRateLimit(anyDouble())).thenReturn(mockZkBuilder);
        when(mockZkBuilder.watchers(any())).thenReturn(mockZkBuilder);
        when(mockZkBuilder.statsLogger(any(StatsLogger.class))).thenReturn(mockZkBuilder);

        this.mockZkc = mock(ZooKeeperClient.class);
        when(mockZkc.exists(anyString(), eq(false)))
            .thenReturn(null);

        when(mockZkBuilder.build()).thenReturn(mockZkc);

        zooKeeperClientMockedStatic = Mockito.mockStatic(ZooKeeperClient.class);
        zooKeeperClientMockedStatic.when(() -> ZooKeeperClient.newBuilder()).thenReturn(mockZkBuilder);
    }

    public void teardown() {
        zooKeeperClientMockedStatic.close();
    }

}
