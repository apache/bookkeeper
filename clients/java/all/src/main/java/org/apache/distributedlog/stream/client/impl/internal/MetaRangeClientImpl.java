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

package org.apache.distributedlog.stream.client.impl.internal;

import static org.apache.distributedlog.stream.client.impl.internal.ProtocolInternalUtils.createActiveRanges;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createGetActiveRangesRequest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.stream.client.impl.StorageContainerChannel;
import org.apache.distributedlog.stream.client.impl.StorageContainerChannelManager;
import org.apache.distributedlog.stream.client.impl.internal.mr.MetaRangeRequestProcessor;
import org.apache.distributedlog.stream.client.internal.api.HashStreamRanges;
import org.apache.distributedlog.stream.client.internal.api.MetaRangeClient;
import org.apache.distributedlog.stream.proto.StreamProperties;

/**
 * A default implementation for {@link MetaRangeClient}.
 */
@Slf4j
class MetaRangeClientImpl implements MetaRangeClient {

  private final StreamProperties streamProps;
  private final ScheduledExecutorService executor;
  private final StorageContainerChannel scClient;

  MetaRangeClientImpl(StreamProperties streamProps,
                      OrderedScheduler scheduler,
                      StorageContainerChannelManager channelManager) {
    this.streamProps = streamProps;
    this.executor = scheduler.chooseThread(streamProps.getStreamId());
    this.scClient = channelManager.getOrCreate(streamProps.getStorageContainerId());
  }

  @Override
  public StreamProperties getStreamProps() {
    return streamProps;
  }

  StorageContainerChannel getStorageContainerClient() {
    return scClient;
  }

  //
  // Meta KeyRange Server Requests
  //

  @Override
  public CompletableFuture<HashStreamRanges> getActiveDataRanges() {
    return MetaRangeRequestProcessor.of(
      createGetActiveRangesRequest(scClient.getStorageContainerId(), streamProps.getStreamId()),
      (response) -> createActiveRanges(response.getGetActiveRangesResp()),
      scClient,
      executor
    ).process();
  }

}
