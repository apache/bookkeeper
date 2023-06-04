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

package org.apache.bookkeeper.metadata.etcd.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.metadata.etcd.testing.EtcdContainer;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@Slf4j
public class TlsSmokeTest extends SmokeTest {

    @BeforeClass
    public static void setupCluster() throws Exception {
        etcdContainer = new EtcdContainer(RandomStringUtils.randomAlphabetic(8), true);
        etcdContainer.start();
        log.info("Successfully started etcd1 at {}", etcdContainer.getClientEndpoint());
        setupCluster(NUM_BOOKIES);
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        if (null != etcdContainer) {
            etcdContainer.stop();
            etcdContainer = null;
            log.info("Successfully stopped etcd.");
        }
    }

    @Override
    public void setUp() throws Exception {
        conf = new ClientConfiguration()
                .setMetadataServiceUri(etcdContainer.getExternalServiceUri())
                .setMetadataServiceConfig(getMetadataServiceConfig());
        bk = BookKeeper.newBuilder(conf).build();
    }

}
