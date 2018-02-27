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

package org.apache.bookkeeper.stream.storage;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;

/**
 * Define a set of resources used for storage.
 */
@Accessors(fluent = true)
@Getter
public class StorageResources {

    public static StorageResources create() {
        return create(StorageResourcesSpec.builder().build());
    }

    public static StorageResources create(StorageResourcesSpec spec) {
        return new StorageResources(spec);
    }

    private static Resource<OrderedScheduler> createSchedulerResource(String name, int numThreads) {
        return new Resource<OrderedScheduler>() {
            @Override
            public OrderedScheduler create() {
                return OrderedScheduler.newSchedulerBuilder()
                    .numThreads(numThreads)
                    .name(name)
                    .build();
            }

            @Override
            public void close(OrderedScheduler scheduler) {
                scheduler.shutdown();
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }

    private final Resource<OrderedScheduler> scheduler;
    private final Resource<OrderedScheduler> ioWriteScheduler;
    private final Resource<OrderedScheduler> ioReadScheduler;
    private final Resource<OrderedScheduler> checkpointScheduler;
    private final Resource<HashedWheelTimer> timer;

    private StorageResources(StorageResourcesSpec spec) {
        this.scheduler = createSchedulerResource(
            "storage-scheduler", spec.numSchedulerThreads());
        this.ioWriteScheduler = createSchedulerResource(
            "io-write-scheduler", spec.numIOWriteThreads());
        this.ioReadScheduler = createSchedulerResource(
            "io-read-scheduler", spec.numIOReadThreads());
        this.checkpointScheduler = createSchedulerResource(
            "io-checkpoint-scheduler", spec.numCheckpointThreads());

        this.timer =
            new Resource<HashedWheelTimer>() {

                private static final String name = "storage-timer";

                @Override
                public HashedWheelTimer create() {
                    HashedWheelTimer timer = new HashedWheelTimer(
                        new ThreadFactoryBuilder()
                            .setNameFormat(name + "-%d")
                            .build(),
                        200,
                        TimeUnit.MILLISECONDS,
                        512,
                        true);
                    timer.start();
                    return timer;
                }

                @Override
                public void close(HashedWheelTimer instance) {
                    instance.stop();
                }

                @Override
                public String toString() {
                    return name;
                }
            };
    }

}
