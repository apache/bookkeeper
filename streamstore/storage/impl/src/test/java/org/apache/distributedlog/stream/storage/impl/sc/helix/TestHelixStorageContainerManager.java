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

package org.apache.distributedlog.stream.storage.impl.sc.helix;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.distributedlog.ZooKeeperClusterTestCase;
import org.apache.distributedlog.stream.client.utils.NetUtils;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainer;
import org.apache.distributedlog.stream.storage.conf.StorageConfiguration;
import org.apache.distributedlog.stream.storage.impl.sc.StorageContainerRegistryImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test for {@link HelixStorageContainerManager}.
 */
@Slf4j
public class TestHelixStorageContainerManager extends ZooKeeperClusterTestCase {

  @Rule
  public TestName runtime = new TestName();

  private HelixStorageController controller;
  private OrderedScheduler scheduler;

  @Before
  public void setup() {
    controller = new HelixStorageController(zkServers);
    scheduler = OrderedScheduler.newSchedulerBuilder()
      .numThreads(1)
      .name("test-helix-storagecontainer-manager")
      .build();
  }

  @After
  public void tearDown() {
    if (null != controller) {
      controller.close();
    }
    if (null != scheduler) {
      scheduler.shutdown();
    }
  }

  @Test
  public void testCreateCluster() {
    String clusterName = runtime.getMethodName();

    controller.createCluster(clusterName, 1024, 3);
    List<String> clusters = controller.getAdmin().getClusters();
    assertTrue(clusters.contains(clusterName));

    // create an existing cluster
    controller.createCluster(clusterName, 1024, 3);
    clusters = controller.getAdmin().getClusters();
    assertTrue(clusters.contains(clusterName));
  }

  @Test
  public void testAddNode() {
    String clusterName = runtime.getMethodName();
    controller.createCluster(clusterName, 1024, 3);
    List<String> instances = controller.getAdmin().getInstancesInCluster(clusterName);
    assertEquals(0, instances.size());

    Endpoint endpoint = NetUtils.createEndpoint("127.0.0.1", 4181);
    controller.addNode(clusterName, endpoint, Optional.empty());
    instances = controller.getAdmin().getInstancesInCluster(clusterName);
    assertEquals(1, instances.size());
    assertTrue(instances.contains(HelixStorageController.getEndpointName(endpoint)));

    // add the instance again
    controller.addNode(clusterName, endpoint, Optional.empty());
    instances = controller.getAdmin().getInstancesInCluster(clusterName);
    assertEquals(1, instances.size());
    assertTrue(instances.contains(HelixStorageController.getEndpointName(endpoint)));

    // add a different instance
    Endpoint endpoint2 = NetUtils.createEndpoint("127.0.0.1", 4481);
    controller.addNode(clusterName, endpoint2, Optional.empty());
    instances = controller.getAdmin().getInstancesInCluster(clusterName);
    assertEquals(2, instances.size());
    assertTrue(instances.contains(HelixStorageController.getEndpointName(endpoint)));
    assertTrue(instances.contains(HelixStorageController.getEndpointName(endpoint2)));
  }

  private StorageContainerRegistryImpl createRegistry() {
    return new StorageContainerRegistryImpl(
      (scId) -> {
        StorageContainer sc = mock(StorageContainer.class);
        when(sc.getId()).thenReturn(scId);
        when(sc.start()).thenReturn(FutureUtils.value(null));
        when(sc.stop()).thenReturn(FutureUtils.value(null));
        return sc;
      },
      scheduler);
  }

  private HelixStorageContainerManager createManager(String clusterName,
                                                     StorageConfiguration conf,
                                                     StorageContainerRegistryImpl registry,
                                                     Endpoint endpoint) {
    return new HelixStorageContainerManager(
      zkServers,
      clusterName,
      conf,
      registry,
      endpoint,
      Optional.empty(),
      NullStatsLogger.INSTANCE);
  }

  @Ignore
  @Test
  public void testStorageContainerManager() throws Exception {
    String clusterName = runtime.getMethodName();
    int numStorageContainers = 12;
    int numHosts = 3;
    controller.createCluster(clusterName, numStorageContainers, 3);

    StorageConfiguration conf = new StorageConfiguration(new CompositeConfiguration());
    Endpoint[] endpoints = new Endpoint[numHosts];
    StorageContainerRegistryImpl[] registries = new StorageContainerRegistryImpl[numHosts];
    HelixStorageContainerManager[] managers = new HelixStorageContainerManager[numHosts];

    int basePort = 80;
    for (int i = 0; i < numHosts; i++) {
      endpoints[i] = NetUtils.createEndpoint("127.0.0.1", basePort + i);
      registries[i] = createRegistry();
      managers[i] = createManager(
        clusterName, conf, registries[i], endpoints[i]);
    }

    managers[0].start();
    while (registries[0].getNumStorageContainers() < numStorageContainers) {
      TimeUnit.MILLISECONDS.sleep(20);
    }

    assertEquals(numStorageContainers, registries[0].getNumStorageContainers());
    assertEquals(0, registries[1].getNumStorageContainers());
    assertEquals(0, registries[2].getNumStorageContainers());

    // start the second node
    managers[1].start();
    while (registries[0].getNumStorageContainers() > numStorageContainers / 2) {
      TimeUnit.MILLISECONDS.sleep(20);
    }
    while (registries[1].getNumStorageContainers() < numStorageContainers / 2) {
      TimeUnit.MILLISECONDS.sleep(20);
    }

    assertEquals(numStorageContainers / 2, registries[0].getNumStorageContainers());
    assertEquals(numStorageContainers / 2, registries[1].getNumStorageContainers());
    assertEquals(0, registries[2].getNumStorageContainers());

    // start the third node
    managers[2].start();
    while (registries[0].getNumStorageContainers() > numStorageContainers / 3) {
      TimeUnit.MILLISECONDS.sleep(20);
    }
    while (registries[1].getNumStorageContainers() > numStorageContainers / 3) {
      TimeUnit.MILLISECONDS.sleep(20);
    }
    while (registries[2].getNumStorageContainers() < numStorageContainers / 3) {
      TimeUnit.MILLISECONDS.sleep(20);
    }

    int totalStorageContainers = registries[0].getNumStorageContainers()
      + registries[1].getNumStorageContainers()
      + registries[2].getNumStorageContainers();
    assertEquals("Expected " + numStorageContainers + "But " + totalStorageContainers + " found",
      numStorageContainers, totalStorageContainers);
    assertEquals(numStorageContainers / 3, registries[0].getNumStorageContainers());
    assertEquals(numStorageContainers / 3, registries[1].getNumStorageContainers());
    assertEquals(numStorageContainers / 3, registries[2].getNumStorageContainers());

    for (int i = 0; i < 10; i++) {
      int nid = ThreadLocalRandom.current().nextInt(numHosts);
      long scId = ThreadLocalRandom.current().nextLong(numStorageContainers);
      Endpoint endpoint = managers[nid].getStorageContainer(scId);
      if (null != endpoint) {
        assertTrue(endpoint.equals(endpoints[0])
          || endpoint.equals(endpoints[1])
          || endpoint.equals(endpoints[2]));
      }
    }

    for (HelixStorageContainerManager manager : managers) {
      manager.close();
    }

  }



}
