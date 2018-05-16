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

package org.apache.bookkeeper.stream.server.conf;

import org.apache.bookkeeper.common.conf.ComponentConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;

/**
 * KeyRange Server related configuration withSettings.
 */
public class StorageServerConfiguration extends ComponentConfiguration {

    private static final String COMPONENT_PREFIX = "storageserver" + DELIMITER;

    private static final String GRPC_PORT = "grpc.port";

    public static StorageServerConfiguration of(CompositeConfiguration conf) {
        return new StorageServerConfiguration(conf);
    }

    private StorageServerConfiguration(CompositeConfiguration conf) {
        super(conf, COMPONENT_PREFIX);
    }

    /**
     * Returns the grpc port that serves requests coming into the stream storage server.
     *
     * @return grpc port
     */
    public int getGrpcPort() {
        return getInt(GRPC_PORT, 4181);
    }
}
