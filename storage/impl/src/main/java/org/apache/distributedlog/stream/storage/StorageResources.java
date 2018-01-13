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

package org.apache.distributedlog.stream.storage;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;

/**
 * Define a set of resources used for storage.
 */
public class StorageResources {

  public static StorageResources create() {
    return new StorageResources();
  }

  private final Resource<OrderedScheduler> scheduler;
  private final Resource<ScheduledExecutorService> snapshotScheduler;
  private final Resource<HashedWheelTimer> timer;

  private StorageResources() {
    this.scheduler =
      new Resource<OrderedScheduler>() {

        private static final String name = "storage-scheduler";

        @Override
        public OrderedScheduler create() {
          return OrderedScheduler.newSchedulerBuilder()
            .numThreads(Runtime.getRuntime().availableProcessors() * 2)
            .name(name)
            .build();
        }

        @Override
        public void close(OrderedScheduler instance) {
          instance.shutdown();
        }

        @Override
        public String toString() {
          return name;
        }
      };

    this.snapshotScheduler =
      new Resource<ScheduledExecutorService>() {

        private static final String name = "storage-snapshot-scheduler";

        @Override
        public ScheduledExecutorService create() {
          return Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors() * 2,
            new ThreadFactoryBuilder()
              .setNameFormat(name + "-%d")
              .setDaemon(true)
              .build());
        }

        @Override
        public void close(ScheduledExecutorService instance) {
          instance.shutdown();
        }

        @Override
        public String toString() {
          return name;
        }
      };

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

  public Resource<OrderedScheduler> scheduler() {
    return scheduler;
  }

  public Resource<ScheduledExecutorService> snapshotScheduler() {
    return snapshotScheduler;
  }

  public Resource<HashedWheelTimer> timer() {
    return timer;
  }

}
