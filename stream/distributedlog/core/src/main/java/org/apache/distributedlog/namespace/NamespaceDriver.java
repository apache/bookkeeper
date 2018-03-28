/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.namespace;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.acl.AccessControlManager;
import org.apache.distributedlog.api.subscription.SubscriptionsStore;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.distributedlog.injector.AsyncFailureInjector;
import org.apache.distributedlog.logsegment.LogSegmentEntryStore;
import org.apache.distributedlog.metadata.LogMetadataStore;
import org.apache.distributedlog.metadata.LogStreamMetadataStore;

/**
 * Manager to manage all the stores required by a namespace.
 */
public interface NamespaceDriver extends Closeable {
    /**
     * Role associated with the store.
     */
    enum Role {
        WRITER,
        READER
    }

    /**
     * Initialize the namespace manager.
     *
     * @param conf distributedlog configuration
     * @param dynConf dynamic distributedlog configuration
     * @param namespace root uri of the namespace
     * @param scheduler ordered scheduler
     * @param featureProvider feature provider
     * @param statsLogger stats logger
     * @param perLogStatsLogger per log stream stats logger
     * @param clientId client id
     * @return namespace manager
     * @throws IOException when failed to initialize the namespace manager
     */
    NamespaceDriver initialize(DistributedLogConfiguration conf,
                               DynamicDistributedLogConfiguration dynConf,
                               URI namespace,
                               OrderedScheduler scheduler,
                               FeatureProvider featureProvider,
                               AsyncFailureInjector failureInjector,
                               StatsLogger statsLogger,
                               StatsLogger perLogStatsLogger,
                               String clientId,
                               int regionId) throws IOException;

    /**
     * Get the scheme of the namespace driver.
     *
     * @return the scheme of the namespace driver.
     */
    String getScheme();

    /**
     * Get the root uri of the namespace driver.
     *
     * @return the root uri of the namespace driver.
     */
    URI getUri();

    /**
     * Retrieve the log {@code metadata store} used by the namespace.
     *
     * @return the log metadata store
     */
    LogMetadataStore getLogMetadataStore();

    /**
     * Retrieve the log stream {@code metadata store} used by the namespace.
     *
     * @param role the role to retrieve the log stream metadata store.
     * @return the log stream metadata store
     */
    LogStreamMetadataStore getLogStreamMetadataStore(Role role);

    /**
     * Retrieve the log segment {@code entry store} used by the namespace.
     *
     * @param role the role to retrieve the log segment entry store.
     * @return the log segment entry store.
     * @throws IOException when failed to open log segment entry store.
     */
    LogSegmentEntryStore getLogSegmentEntryStore(Role role);

    /**
     * Create an access control manager to manage/check acl for logs.
     *
     * @return access control manager for logs under the namespace.
     * @throws IOException
     */
    AccessControlManager getAccessControlManager()
            throws IOException;

    /**
     * Retrieve the metadata accessor for log stream {@code streamName}.
     * (TODO: it is a legacy interface. should remove it if we have metadata of stream.)
     *
     * @param streamName name of log stream.
     * @return metadata accessor for log stream {@code streamName}.
     */
    @SuppressWarnings("deprecation")
    org.apache.distributedlog.api.MetadataAccessor getMetadataAccessor(String streamName)
            throws InvalidStreamNameException, IOException;

    /**
     * Retrieve the subscriptions store for log stream {@code streamName}.
     *
     * @return the subscriptions store for log stream {@code streamName}
     */
    SubscriptionsStore getSubscriptionsStore(String streamName);

}
