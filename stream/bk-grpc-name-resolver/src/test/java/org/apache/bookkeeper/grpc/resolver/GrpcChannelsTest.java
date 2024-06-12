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

package org.apache.bookkeeper.grpc.resolver;

import static org.junit.Assert.assertTrue;

import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.utils.GrpcChannels;
import org.junit.Test;

/**
 * Unit test {@link org.apache.bookkeeper.clients.utils.GrpcChannels} with registration based name resolver.
 */
public class GrpcChannelsTest {

    @Test
    public void testZKServiceUri() {
        String serviceUri = "zk://127.0.0.1/stream/servers";
        ManagedChannelBuilder builder = GrpcChannels.createChannelBuilder(
            serviceUri,
            StorageClientSettings.newBuilder().serviceUri(serviceUri).build());
        assertTrue(builder instanceof NettyChannelBuilder);
    }

}
