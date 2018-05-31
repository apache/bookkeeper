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

package org.apache.bookkeeper.clients.impl.grpc;

import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolverProvider;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.common.net.ServiceURI;

/**
 * Utils to create grpc channels.
 */
public final class GrpcChannels {

    private static final String BACKEND_INPROCESS = "inprocess";

    private GrpcChannels() {}

    /**
     * Create a channel builder from <tt>serviceUri</tt> with client <tt>settings</tt>.
     *
     * @param serviceUri service uri
     * @param settings client settings
     * @return managed channel builder
     */
    public static ManagedChannelBuilder createChannelBuilder(String serviceUri,
                                                             StorageClientSettings settings) {
        ServiceURI uri = ServiceURI.create(serviceUri);

        ManagedChannelBuilder builder;
        if (uri.getServiceInfos().length > 0 && uri.getServiceInfos()[0].equals(BACKEND_INPROCESS)) {
            // this is an inprocess service, so build an inprocess channel.
            String serviceName = uri.getServiceHosts()[0];
            builder = InProcessChannelBuilder.forName(serviceName).directExecutor();
        } else {
            builder = ManagedChannelBuilder.forTarget(serviceUri)
                .nameResolverFactory(NameResolverProvider.asFactory());
        }
        if (settings.usePlaintext()) {
            builder = builder.usePlaintext();
        }
        return builder;
    }

}
