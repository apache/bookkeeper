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

package org.apache.bookkeeper.stream.storage.impl.sc;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManager;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerRegistry;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;

/**
 * A local implementation of {@link StorageContainerManager}.
 */
public class LocalStorageContainerManager
    extends AbstractLifecycleComponent<StorageConfiguration>
    implements StorageContainerManager {

    private final Endpoint myEndpoint;
    private final int numStorageContainers;
    private final StorageContainerRegistry registry;

    public LocalStorageContainerManager(Endpoint myEndpoint,
                                        StorageConfiguration conf,
                                        StorageContainerRegistry scRegistry,
                                        int numStorageContainers) {
        super("local-storage-container-manager", conf, NullStatsLogger.INSTANCE);
        this.myEndpoint = myEndpoint;
        this.registry = scRegistry;
        this.numStorageContainers = numStorageContainers;
    }

    @Override
    public Endpoint getStorageContainer(long scId) {
        return myEndpoint;
    }

    @Override
    protected void doStart() {
        List<CompletableFuture<StorageContainer>> futures = Lists.newArrayListWithExpectedSize(numStorageContainers);
        for (int scId = 0; scId < numStorageContainers; scId++) {
            futures.add(this.registry.startStorageContainer(scId));
        }
        FutureUtils.collect(futures).join();
    }

    @Override
    protected void doStop() {
        List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(numStorageContainers);
        for (int scId = 0; scId < numStorageContainers; scId++) {
            futures.add(this.registry.stopStorageContainer(scId));
        }
        FutureUtils.collect(futures).join();
    }

    @Override
    protected void doClose() throws IOException {
        // do nothing
    }
}
