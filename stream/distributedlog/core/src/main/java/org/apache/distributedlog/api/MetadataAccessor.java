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
package org.apache.distributedlog.api;

import java.io.Closeable;
import java.io.IOException;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.LimitedPrivate;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.distributedlog.io.AsyncCloseable;

/**
 * Provide a metadata accessor to access customized metadata associated with logs.
 *
 * @deprecated this class is here for legacy reason. It is not recommended to use this class for storing customized
 *             metadata.
 */
@LimitedPrivate
@Evolving
public interface MetadataAccessor extends Closeable, AsyncCloseable {
    /**
     * Get the name of the stream managed by this log manager.
     * @return streamName
     */
    String getStreamName();

    void createOrUpdateMetadata(byte[] metadata) throws IOException;

    void deleteMetadata() throws IOException;

    byte[] getMetadata() throws IOException;

    /**
     * Close the distributed log metadata, freeing any resources it may hold.
     */
    void close() throws IOException;

}
