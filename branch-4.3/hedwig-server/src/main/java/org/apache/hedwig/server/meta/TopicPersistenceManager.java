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

import com.google.protobuf.ByteString;

import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRanges;
import org.apache.hedwig.util.Callback;

/**
 * Manage topic persistence metadata.
 */
public interface TopicPersistenceManager extends Closeable {

    /**
     * Read persistence info of a specified topic.
     *
     * @param topic
     *          Topic Name
     * @param callback
     *          Callback when read persistence info.
     *          If no persistence info found, return null.
     * @param ctx
     *          Context of the callback
     */
    public void readTopicPersistenceInfo(ByteString topic,
                                         Callback<Versioned<LedgerRanges>> callback, Object ctx);

    /**
     * Update persistence info of a specified topic.
     *
     * @param topic
     *          Topic name
     * @param ranges
     *          Persistence info
     * @param version
     *          Current version of persistence info.
     *          If <code>version</code> is {@link Version.NEW}, create persistence info;
     *          {@link PubSubException.TopicPersistenceInfoExistsException} is returned when
     *          persistence info existed before.
     *          Otherwise, the persitence info is updated only when
     *          provided version equals to its current version.
     *          {@link PubSubException.BadVersionException} is returned when version doesn't match,
     *          {@link PubSubException.NoTopicPersistenceInfoException} is returned when no
     *          persistence info found to update.
     * @param callback
     *          Callback when persistence info updated. New version would be returned.
     * @param ctx
     *          Context of the callback
     */
    public void writeTopicPersistenceInfo(ByteString topic, LedgerRanges ranges, Version version,
                                          Callback<Version> callback, Object ctx);

    /**
     * Delete persistence info of a specified topic.
     * Currently used in test cases.
     *
     * @param topic
     *          Topic name
     * @param version
     *          Current version of persistence info
     *          If <code>version</code> is {@link Version.ANY}, delete persistence info no matter its current version.
     *          Otherwise, the persitence info is deleted only when
     *          provided version equals to its current version.
     * @param callback
     *          Callback return whether the deletion succeed.
     *          {@link PubSubException.NoTopicPersistenceInfoException} is returned when no persistence.
     *          {@link PubSubException.BadVersionException} is returned when version doesn't match.
     *          info found to delete.
     * @param ctx
     *          Context of the callback
     */
    public void deleteTopicPersistenceInfo(ByteString topic, Version version,
                                           Callback<Void> callback, Object ctx);

}
