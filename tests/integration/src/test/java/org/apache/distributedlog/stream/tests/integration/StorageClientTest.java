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

package org.apache.distributedlog.stream.tests.integration;

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_RETENTION_POLICY;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_SEGMENT_ROLLING_POLICY;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_SPLIT_POLICY;
import static org.junit.Assert.assertEquals;

import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.api.StorageClient;
import org.apache.distributedlog.clients.StorageClientBuilder;
import org.apache.distributedlog.clients.admin.StorageAdminClient;
import org.apache.distributedlog.clients.config.StorageClientSettings;
import org.apache.distributedlog.stream.proto.NamespaceConfiguration;
import org.apache.distributedlog.stream.proto.RangeKeyType;
import org.apache.distributedlog.stream.proto.StreamConfiguration;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Integration test for stream client test.
 */
@Slf4j
public class StorageClientTest extends StorageServerTestBase {

  @Rule
  public final TestName testName = new TestName();

  private String nsName;
  private String streamName;
  private StorageAdminClient adminClient;
  private StorageClient client;
  private final StreamConfiguration streamConf = StreamConfiguration.newBuilder()
    .setKeyType(RangeKeyType.HASH)
    .setInitialNumRanges(4)
    .setMinNumRanges(4)
    .setRetentionPolicy(DEFAULT_RETENTION_POLICY)
    .setRollingPolicy(DEFAULT_SEGMENT_ROLLING_POLICY)
    .setSplitPolicy(DEFAULT_SPLIT_POLICY)
    .build();
  private final NamespaceConfiguration colConf = NamespaceConfiguration.newBuilder()
    .setDefaultStreamConf(streamConf)
    .build();
  private URI defaultBackendUri;

  @Override
  protected void doSetup() throws Exception {
    defaultBackendUri = URI.create("distributedlog://" + cluster.getZkServers() + "/stream/storage");
    StorageClientSettings settings = StorageClientSettings.newBuilder()
      .addEndpoints(cluster.getRpcEndpoints().toArray(new Endpoint[cluster.getRpcEndpoints().size()]))
      .usePlaintext(true)
      .build();
    adminClient = StorageClientBuilder.newBuilder()
      .withSettings(settings)
      .buildAdmin();
    nsName = "test_namespace";
    FutureUtils.result(
      adminClient.createNamespace(nsName, colConf));
    client = StorageClientBuilder.newBuilder()
      .withSettings(settings)
      .withNamespace(nsName)
      .build();
    streamName = "test_stream";
    createStream(streamName);
  }

  @Override
  protected void doTeardown() throws Exception {
    if (null != client) {
      client.closeAsync();
    }
    if (null != adminClient) {
      adminClient.closeAsync();
    }
  }

  private void createStream(String streamName) throws Exception {
    FutureUtils.result(
      adminClient.createStream(
        nsName,
        streamName,
        streamConf));
  }

  @Test
  public void testAdmin() throws Exception {
    StreamProperties properties =
      FutureUtils.result(adminClient.getStream(nsName, streamName));
    assertEquals(
        StreamConfiguration.newBuilder(streamConf)
          .setBackendServiceUrl(defaultBackendUri.toString())
          .build()
        , properties.getStreamConf());
  }

}
