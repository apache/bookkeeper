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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.junit.Test;

/**
 * Unit test for {@link StorageContainerInfo}.
 */
public class TestStorageContainerInfo {

    @Test
    public void testBasic() {
        long groupId = 1234L;
        long revision = 4468L;
        Endpoint endpoint = Endpoint.newBuilder()
            .setHostname("123.46.78.96")
            .setPort(3181)
            .build();
        StorageContainerInfo sc = StorageContainerInfo.of(
            groupId,
            revision,
            endpoint,
            Lists.newArrayList(endpoint));
        assertEquals("Group ID mismatch", groupId, sc.getGroupId());
        assertEquals("Revision mismatch", revision, sc.getRevision());
        assertEquals("Write Endpoint mismatch", endpoint, sc.getWriteEndpoint());
        assertEquals("Read Endpoint mismatch", Lists.newArrayList(endpoint), sc.getReadEndpoints());
    }

}
