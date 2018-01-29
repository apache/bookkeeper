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
package org.apache.bookkeeper.stream.server.service;

import java.io.IOException;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.server.conf.StorageServerConfiguration;
import org.apache.bookkeeper.stream.server.grpc.GrpcServer;
import org.apache.bookkeeper.stream.server.grpc.GrpcServerSpec;

/**
 * Service to run {@link GrpcServer}.
 */
public class GrpcService extends AbstractLifecycleComponent<StorageServerConfiguration> {

    private final GrpcServerSpec spec;
    private GrpcServer server;

    public GrpcService(StorageServerConfiguration conf,
                       GrpcServerSpec spec,
                       StatsLogger statsLogger) {
        super("grpc-service", conf, statsLogger);
        this.spec = spec;
    }

    @Override
    protected void doStart() {
        this.server = GrpcServer.build(spec);
        this.server.start();
    }

    @Override
    protected void doStop() {
        this.server.stop();
    }

    @Override
    protected void doClose() throws IOException {
        this.server.close();
    }
}
