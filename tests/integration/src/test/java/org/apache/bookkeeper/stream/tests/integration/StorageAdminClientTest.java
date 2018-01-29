/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.tests.integration;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.exceptions.NamespaceExistsException;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.exceptions.StreamExistsException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Integration test for stream admin client test.
 */
public class StorageAdminClientTest extends StorageServerTestBase {

  @Rule
  public final TestName testName = new TestName();

  private OrderedScheduler scheduler;
  private StorageAdminClient adminClient;
  private URI defaultBackendUri;

  @Override
  protected void doSetup() throws Exception {
    scheduler = OrderedScheduler.newSchedulerBuilder()
      .name("admin-client-test")
      .numThreads(1)
      .build();
    StorageClientSettings settings = StorageClientSettings.newBuilder()
      .addEndpoints(cluster.getRpcEndpoints().toArray(new Endpoint[cluster.getRpcEndpoints().size()]))
      .usePlaintext(true)
      .build();
    adminClient = StorageClientBuilder.newBuilder()
      .withSettings(settings)
      .buildAdmin();
    defaultBackendUri = URI.create("distributedlog://" + cluster.getZkServers() + "/stream/storage");
  }

  @Override
  protected void doTeardown() throws Exception {
    if (null != adminClient) {
      adminClient.close();
    }
    if (null != scheduler) {
      scheduler.shutdown();
    }
  }

  @Test
  public void testNamespaceAPI() throws Exception {
    // Create a namespace
    String nsName = testName.getMethodName();
    NamespaceConfiguration colConf = NamespaceConfiguration.newBuilder()
      .setDefaultStreamConf(DEFAULT_STREAM_CONF)
      .build();
    NamespaceProperties colProps = FutureUtils.result(adminClient.createNamespace(nsName, colConf));
    assertEquals(nsName, colProps.getNamespaceName());
    assertEquals(colConf.getDefaultStreamConf(), colProps.getDefaultStreamConf());

    // create a duplicated namespace
    try {
      FutureUtils.result(adminClient.createNamespace(nsName, colConf));
      fail("Should fail on creation if namespace " + nsName + " already exists");
    } catch (NamespaceExistsException cee) {
      // expected
    } catch (ClientException ce) {
      // TODO: currently range server throws InternalServerError
      assertTrue(ce.getMessage().endsWith("code = " + StatusCode.INTERNAL_SERVER_ERROR));
    }

    String notFoundColName = testName.getMethodName() + "_notfound";
    // get a not-found namespace
    try {
      FutureUtils.result(adminClient.getNamespace(notFoundColName));
      fail("Should fail on get if namespace " + notFoundColName + " doesn't exist");
    } catch (NamespaceNotFoundException cnfe) {
      // expected
    }

    // delete a not-found namespace
    try {
      FutureUtils.result(adminClient.deleteNamespace(notFoundColName));
      fail("Should fail on delete if namespace " + notFoundColName + " doesn't exist");
    } catch (NamespaceNotFoundException cnfe) {
      // expected
    }

    // get an existing namespace
    NamespaceProperties getColProps = FutureUtils.result(adminClient.getNamespace(nsName));
    assertEquals(colProps, getColProps);

    // delete an existing namespace
    Boolean deleted = FutureUtils.result(adminClient.deleteNamespace(nsName));
    assertTrue(deleted);

    // the namespace should not exist after deleted.
    try {
      FutureUtils.result(adminClient.getNamespace(nsName));
      fail("Should fail on get if namespace " + nsName + " doesn't exist");
    } catch (NamespaceNotFoundException cnfe) {
      // expected
    }
  }

  @Test
  public void testStreamAPI() throws Exception {
    // Create a namespace
    String nsName = testName.getMethodName() + "_ns";
    NamespaceConfiguration colConf = NamespaceConfiguration.newBuilder()
      .setDefaultStreamConf(DEFAULT_STREAM_CONF)
      .build();
    NamespaceProperties colProps = FutureUtils.result(adminClient.createNamespace(nsName, colConf));
    assertEquals(nsName, colProps.getNamespaceName());
    assertEquals(colConf.getDefaultStreamConf(), colProps.getDefaultStreamConf());

    // Create a stream
    String streamName = testName.getMethodName() + "_stream";
    StreamConfiguration streamConf = StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
      .build();
    StreamProperties streamProps  = FutureUtils.result(adminClient.createStream(nsName, streamName, streamConf));
    assertEquals(streamName, streamProps.getStreamName());
    assertEquals(
        StreamConfiguration.newBuilder(streamConf)
          .setBackendServiceUrl(defaultBackendUri.toString())
          .build(),
        streamProps.getStreamConf());

    // create a duplicated stream
    try {
      FutureUtils.result(adminClient.createStream(nsName, streamName, streamConf));
      fail("Should fail on creation if stream " + streamName + " already exists");
    } catch (StreamExistsException cee) {
      // expected
    } catch (ClientException ce) {
      // TODO: currently it throws InternalServerError for stream exists case
      assertTrue(ce.getMessage().endsWith("code = " + StatusCode.INTERNAL_SERVER_ERROR));
    }

    String notFoundStreamName = testName.getMethodName() + "_notfound";
    // get a not-found stream
    try {
      FutureUtils.result(adminClient.getStream(nsName, notFoundStreamName));
      fail("Should fail on get if stream " + notFoundStreamName + " doesn't exist");
    } catch (StreamNotFoundException cnfe) {
      // expected
    }

    // delete a not-found stream
    try {
      FutureUtils.result(adminClient.deleteStream(nsName, notFoundStreamName));
      fail("Should fail on delete if stream " + notFoundStreamName + " doesn't exist");
    } catch (StreamNotFoundException cnfe) {
      // expected
    }

    // get an existing stream
    StreamProperties getStreamProps = FutureUtils.result(adminClient.getStream(nsName, streamName));
    assertEquals(streamProps, getStreamProps);

    // delete an existing stream
    Boolean deleted = FutureUtils.result(adminClient.deleteStream(nsName, streamName));
    assertTrue(deleted);

    // the stream should not exist after deleted.
    try {
      FutureUtils.result(adminClient.getStream(nsName, streamName));
      fail("Should fail on get if stream " + nsName + " doesn't exist");
    } catch (StreamNotFoundException cnfe) {
      // expected
    }
  }
}
