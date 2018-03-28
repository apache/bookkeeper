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

package org.apache.bookkeeper.clients.impl.container;

import com.google.common.collect.Lists;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.bookkeeper.common.util.IRevisioned;
import org.apache.bookkeeper.stream.proto.common.Endpoint;

/**
 * Represents the information of a storage container.
 */
@Data(staticConstructor = "of")
@EqualsAndHashCode
@ToString
public class StorageContainerInfo implements IRevisioned {

    public static StorageContainerInfo of(long groupId, long revision, Endpoint endpoint) {
        return of(groupId, revision, endpoint, Lists.newArrayList(endpoint));
    }

    /**
     * KeyStorage Container Id.
     */
    private final long groupId;

    /**
     * The revision of storage container info.
     */
    private final long revision;

    /**
     * Endpoint for write service.
     */
    private final Endpoint writeEndpoint;

    /**
     * Endpoints for read service.
     */
    private final List<Endpoint> readEndpoints;

}
