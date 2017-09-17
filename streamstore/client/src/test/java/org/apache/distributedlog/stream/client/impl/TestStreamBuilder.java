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

package org.apache.distributedlog.stream.client.impl;

import static org.mockito.Mockito.mock;

import org.apache.distributedlog.stream.client.StreamBuilder;
import org.apache.distributedlog.stream.client.StreamSettings;
import org.junit.Test;

/**
 * Unit test of {@link org.apache.distributedlog.stream.client.StreamBuilder}.
 */
public class TestStreamBuilder {

  @Test(expected = NullPointerException.class)
  public void testBuildClientNullSettings() {
    StreamBuilder.newBuilder()
      .withSettings(null)
      .withCollection("collection")
      .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildClientNullCollectionName() {
    StreamBuilder.newBuilder()
      .withSettings(mock(StreamSettings.class))
      .withCollection(null)
      .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildClientInvalidCollectionName() {
    StreamBuilder.newBuilder()
      .withSettings(mock(StreamSettings.class))
      .withCollection("invalid-collection")
      .build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuildAdminClientNullSettings() {
    StreamBuilder.newBuilder()
      .withSettings(null)
      .buildAdmin();
  }

}
