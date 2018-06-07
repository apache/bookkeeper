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

package org.apache.bookkeeper.clients.impl.internal;

import static org.apache.bookkeeper.clients.impl.internal.ProtocolInternalUtils.createActiveRanges;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetActiveRangesRequest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannelManager;
import org.apache.bookkeeper.clients.impl.internal.api.HashStreamRanges;
import org.apache.bookkeeper.clients.impl.internal.api.MetaRangeClient;
import org.apache.bookkeeper.clients.impl.internal.mr.MetaRangeRequestProcessor;
import org.apache.bookkeeper.clients.utils.ClientConstants;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * A default implementation for {@link MetaRangeClient}.
 */
@Slf4j
class MetaRangeClientImpl implements MetaRangeClient {

    private final StreamProperties streamProps;
    private final ScheduledExecutorService executor;
    private final StorageContainerChannel scClient;
    private final Backoff.Policy backoffPolicy;

    MetaRangeClientImpl(StreamProperties streamProps,
                        OrderedScheduler scheduler,
                        StorageContainerChannelManager channelManager) {
        this(streamProps, scheduler, channelManager, ClientConstants.DEFAULT_INFINIT_BACKOFF_POLICY);

    }

    MetaRangeClientImpl(StreamProperties streamProps,
                        OrderedScheduler scheduler,
                        StorageContainerChannelManager channelManager,
                        Backoff.Policy backoffPolicy) {
        this.streamProps = streamProps;
        this.executor = scheduler.chooseThread(streamProps.getStreamId());
        this.scClient = channelManager.getOrCreate(streamProps.getStorageContainerId());
        this.backoffPolicy = backoffPolicy;
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
            createGetActiveRangesRequest(streamProps),
            (response) -> createActiveRanges(response),
            scClient,
            executor,
            backoffPolicy
        ).process();
    }

}
