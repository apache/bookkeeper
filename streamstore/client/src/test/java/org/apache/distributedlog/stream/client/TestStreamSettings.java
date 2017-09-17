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

package org.apache.distributedlog.stream.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.junit.Test;

/**
 * Unit test for {@link StreamSettings}.
 */
public class TestStreamSettings {

  @Test
  public void testDefault() {
    List<Endpoint> endpoints = Lists.newArrayList(
      Endpoint.newBuilder()
        .setHostname("127.0.0.1")
        .setPort(80)
        .build());
    StreamSettings settings = StreamSettings.newBuilder()
      .addAllEndpoints(endpoints)
      .build();
    assertEquals(Runtime.getRuntime().availableProcessors(), settings.numWorkerThreads());
    assertTrue(settings.usePlaintext());
    assertFalse(settings.clientName().isPresent());
  }

  @Test
  public void testEmptyBuilder() {
    try {
      StreamSettings.newBuilder().build();
      fail("Should fail with missing endpoints");
    } catch (IllegalArgumentException iae) {
      assertEquals("No name resolver or endpoints or channel builder provided", iae.getMessage());
    }
  }

}
