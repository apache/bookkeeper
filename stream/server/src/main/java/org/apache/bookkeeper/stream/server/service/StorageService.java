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
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.storage.StorageContainerStoreBuilder;
import org.apache.bookkeeper.stream.storage.api.StorageContainerStore;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;

/**
 * Service to run the storage {@link StorageContainerStore}.
 */
@Slf4j
public class StorageService
    extends AbstractLifecycleComponent<StorageConfiguration>
    implements Supplier<StorageContainerStore> {

    private final StorageContainerStoreBuilder storeBuilder;
    private StorageContainerStore store;

    public StorageService(StorageConfiguration conf,
                          StorageContainerStoreBuilder storeBuilder,
                          StatsLogger statsLogger) {
        super("storage-service", conf, statsLogger);
        this.storeBuilder = storeBuilder;
    }

    @Override
    protected void doStart() {
        store = storeBuilder.build();
        store.start();
    }

    @Override
    protected void doStop() {
        store.stop();
    }

    @Override
    protected void doClose() throws IOException {
        store.close();
    }

    @Override
    public StorageContainerStore get() {
        return store;
    }
}
