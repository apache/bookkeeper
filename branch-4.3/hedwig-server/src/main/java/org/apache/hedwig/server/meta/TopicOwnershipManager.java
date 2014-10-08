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
package org.apache.hedwig.server.meta;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import com.google.protobuf.ByteString;

import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRanges;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionState;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.subscriptions.InMemorySubscriptionState;
import org.apache.hedwig.server.topics.HubInfo;
import org.apache.hedwig.util.Callback;
import org.apache.zookeeper.ZooKeeper;

/**
 * Manage topic ownership
 */
public interface TopicOwnershipManager extends Closeable {

    /**
     * Read owner information of a topic.
     *
     * @param topic
     *          Topic Name
     * @param callback
     *          Callback to return hub info. If there is no owner info, return null;
     *          If there is data but not valid owner info, return a Versioned object with null hub info;
     *          If there is valid owner info, return versioned hub info.
     * @param ctx
     *          Context of the callback
     */
    public void readOwnerInfo(ByteString topic, Callback<Versioned<HubInfo>> callback, Object ctx);

    /**
     * Write owner info for a specified topic.
     * A new owner info would be created if there is no one existed before.
     *
     * @param topic
     *          Topic Name
     * @param owner
     *          Owner hub info
     * @param version
     *          Current version of owner info
     *          If <code>version</code> is {@link Version.NEW}, create owner info.
     *          {@link PubSubException.TopicOwnerInfoExistsException} is returned when
     *          owner info existed before.
     *          Otherwise, the owner info is updated only when
     *          provided version equals to its current version.
     *          {@link PubSubException.BadVersionException} is returned when version doesn't match,
     *          {@link PubSubException.NoTopicOwnerInfoException} is returned when no owner info
     *          found to update.
     * @param callback
     *          Callback when owner info updated. New version would be returned if succeed to write.
     * @param ctx
     *          Context of the callback
     */
    public void writeOwnerInfo(ByteString topic, HubInfo owner, Version version,
                               Callback<Version> callback, Object ctx);

    /**
     * Delete owner info for a specified topic.
     *
     * @param topic
     *          Topic Name
     * @param version
     *          Current version of owner info
     *          If <code>version</code> is {@link Version.ANY}, delete owner info no matter its current version.
     *          Otherwise, the owner info is deleted only when
     *          provided version equals to its current version.
     * @param callback
     *          Callback when owner info deleted.
     *          {@link PubSubException.NoTopicOwnerInfoException} is returned when no owner info.
     *          {@link PubSubException.BadVersionException} is returned when version doesn't match.
     * @param ctx
     *          Context of the callback.
     */
    public void deleteOwnerInfo(ByteString topic, Version version,
                                Callback<Void> callback, Object ctx);
}
