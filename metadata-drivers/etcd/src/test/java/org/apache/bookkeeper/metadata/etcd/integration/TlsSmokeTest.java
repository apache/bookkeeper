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
