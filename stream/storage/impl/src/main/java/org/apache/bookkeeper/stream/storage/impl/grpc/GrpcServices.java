/*
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

package org.apache.bookkeeper.stream.storage.impl.grpc;

import com.google.common.collect.Lists;
import io.grpc.ServerServiceDefinition;
import java.util.Collection;
import org.apache.bookkeeper.stream.storage.api.metadata.RangeStoreService;

/**
 * Define all the grpc services associated from a range store.
 */
public final class GrpcServices {

    private GrpcServices() {}

    /**
     * Create the grpc services of a range store.
     *
     * @param store range store.
     * @return the list of grpc services should be associated with the range store.
     */
    public static Collection<ServerServiceDefinition> create(RangeStoreService store) {
        return Lists.newArrayList(
            new GrpcRootRangeService(store).bindService(),
            new GrpcMetaRangeService(store).bindService(),
            new GrpcTableService(store).bindService());
    }

}
