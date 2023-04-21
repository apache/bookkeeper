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
import java.net.URI;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.server.conf.DLConfiguration;
import org.apache.bookkeeper.stream.storage.StorageConstants;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.distributedlog.util.ConfUtils;
import org.apache.zookeeper.KeeperException.Code;

/**
 * A service to provide distributedlog namespace.
 *
 * <p>TODO: eliminate the direct usage of zookeeper here {@link https://github.com/apache/bookkeeper/issues/1331}
 */
@Slf4j
public class DLNamespaceProviderService
    extends AbstractLifecycleComponent<DLConfiguration>
    implements Supplier<Namespace> {

    private static URI initializeNamespace(ServerConfiguration bkServerConf,
                                           URI dlogUri) throws IOException {
        BKDLConfig dlConfig = new BKDLConfig(
            ZKMetadataDriverBase.resolveZkServers(bkServerConf),
            ZKMetadataDriverBase.resolveZkLedgersRootPath(bkServerConf));
        DLMetadata dlMetadata = DLMetadata.create(dlConfig);

        try {
            log.info("Initializing dlog namespace at {}", dlogUri);
            dlMetadata.create(dlogUri);
            log.info("Initialized dlog namespace at {}", dlogUri);
        } catch (ZKException e) {
            if (e.getKeeperExceptionCode() == Code.NODEEXISTS) {
                if (log.isDebugEnabled()) {
                    log.debug("Dlog uri is already bound at {}", dlogUri);
                }
                return dlogUri;
            }
            log.error("Failed to initialize dlog namespace at {}", dlogUri, e);
            throw e;
        }
        return dlogUri;
    }

    private final ServerConfiguration bkServerConf;
    @Getter
    private final DistributedLogConfiguration dlConf;
    private final DynamicDistributedLogConfiguration dlDynConf;
    @Getter
    private final URI dlogUri;
    private Namespace namespace;

    public DLNamespaceProviderService(ServerConfiguration bkServerConf,
                                      DLConfiguration conf,
                                      StatsLogger statsLogger) {
        super("namespace-provider", conf, statsLogger);

        this.dlogUri = URI.create(String.format("distributedlog://%s%s",
            ZKMetadataDriverBase.resolveZkServers(bkServerConf),
            StorageConstants.getStoragePath(StorageConstants.ZK_METADATA_ROOT_PATH)));
        this.bkServerConf = bkServerConf;
        this.dlConf = new DistributedLogConfiguration();
        this.dlConf.loadConf(conf);
        // disable write lock
        this.dlConf.setWriteLockEnabled(false);
        // setting the flush policy
        this.dlConf.setImmediateFlushEnabled(false);
        this.dlConf.setOutputBufferSize(512 * 1024);
        this.dlConf.setPeriodicFlushFrequencyMilliSeconds(2); // flush every 1 ms
        // explicit truncation is required
        this.dlConf.setExplicitTruncationByApplication(true);
        // rolling log segment concurrency is only 1
        this.dlConf.setLogSegmentRollingConcurrency(1);
        this.dlConf.setMaxLogSegmentBytes(256 * 1024 * 1024); // 256 MB
        this.dlDynConf = ConfUtils.getConstDynConf(dlConf);
    }

    @Override
    public Namespace get() {
        return namespace;
    }

    @Override
    protected void doStart() {
        URI uri;
        try {
            uri = initializeNamespace(bkServerConf, dlogUri);
            namespace = NamespaceBuilder.newBuilder()
                .statsLogger(getStatsLogger())
                .clientId("storage-server")
                .conf(dlConf)
                .dynConf(dlDynConf)
                .uri(uri)
                .build();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to build the distributedlog namespace at "
                + bkServerConf.getMetadataServiceUriUnchecked(), e);
        }
        log.info("Provided distributedlog namespace at {}.", uri);
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() throws IOException {
        if (null != namespace) {
            namespace.close();
        }
    }
}
