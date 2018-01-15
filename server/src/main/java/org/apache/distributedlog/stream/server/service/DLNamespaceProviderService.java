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
package org.apache.distributedlog.stream.server.service;

import java.io.IOException;
import java.net.URI;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.distributedlog.stream.server.conf.DLConfiguration;
import org.apache.distributedlog.util.ConfUtils;
import org.apache.zookeeper.KeeperException.Code;

/**
 * A service to provide distributedlog namespace.
 */
@Slf4j
public class DLNamespaceProviderService
    extends AbstractLifecycleComponent<DLConfiguration>
    implements Supplier<Namespace> {

  private static URI initializeNamespace(ServerConfiguration bkServerConf) throws IOException {
    BKDLConfig dlConfig = new BKDLConfig(
        bkServerConf.getZkServers(), bkServerConf.getZkLedgersRootPath());
    DLMetadata dlMetadata = DLMetadata.create(dlConfig);
    URI dlogUri = URI.create(String.format("distributedlog://%s/stream", bkServerConf));

    try {
        dlMetadata.create(dlogUri);
    } catch (ZKException e) {
        if (e.getKeeperExceptionCode() == Code.NODEEXISTS) {
            return dlogUri;
        }
        throw e;
    }
    return dlogUri;
  }

  private final ServerConfiguration bkServerConf;
  private final DistributedLogConfiguration dlConf;
  private Namespace namespace;

  public DLNamespaceProviderService(ServerConfiguration bkServerConf,
                                    DLConfiguration conf,
                                    StatsLogger statsLogger) {
    super("namespace-provider", conf, statsLogger);

    this.bkServerConf = bkServerConf;
    this.dlConf = new DistributedLogConfiguration();
    ConfUtils.loadConfiguration(this.dlConf, conf, conf.getComponentPrefix());
    // disable write lock
    this.dlConf.setWriteLockEnabled(false);
    // setting the flush policy
    this.dlConf.setImmediateFlushEnabled(false);
    this.dlConf.setOutputBufferSize(0);
    this.dlConf.setPeriodicFlushFrequencyMilliSeconds(2); // flush every 1 ms
    // explicit truncation is required
    this.dlConf.setExplicitTruncationByApplication(true);
    // rolling log segment concurrency is only 1
    this.dlConf.setLogSegmentRollingConcurrency(1);
    this.dlConf.setMaxLogSegmentBytes(256 * 1024 * 1024); // 256 MB
  }

  @Override
  public Namespace get() {
    return namespace;
  }

  @Override
  protected void doStart() {
    URI uri;
    try {
      uri = initializeNamespace(bkServerConf);
      namespace = NamespaceBuilder.newBuilder()
        .statsLogger(getStatsLogger())
        .clientId("storage-server")
        .conf(dlConf)
        .uri(uri)
        .build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to build the distributedlog namespace at " + bkServerConf.getZkServers(), e);
    }
    log.info("Provided distributedlog namespace at {}.", uri);
  }

  @Override
  protected void doStop() {}

  @Override
  protected void doClose() throws IOException {
    if (null != namespace) {
      namespace.close();
    }
  }
}
