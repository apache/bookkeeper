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

package org.apache.distributedlog.stream.storage.impl.metadata;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.GuardedBy;
import lombok.Data;
import org.apache.distributedlog.stream.proto.CollectionMetadata;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.storage.api.metadata.Collection;

/**
 * A default implementation of {@link Collection}.
 */
@Data(staticConstructor = "of")
public class CollectionImpl implements Collection {

  private final long collectionId;
  private final String collectionName;

  @GuardedBy("this")
  private CollectionMetadata metadata = CollectionMetadata.getDefaultInstance();
  @GuardedBy("this")
  private final Map<String, StreamProperties> streamNames = Maps.newHashMap();
  @GuardedBy("this")
  private final Map<Long, StreamProperties> streams = Maps.newHashMap();

  @Override
  public long getId() {
    return collectionId;
  }

  @Override
  public String getName() {
    return collectionName;
  }

  @Override
  public synchronized CollectionMetadata getMetadata() {
    return metadata;
  }

  @Override
  public synchronized void setMetadata(CollectionMetadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public synchronized Set<String> getStreams() {
    return ImmutableSet.copyOf(streamNames.keySet());
  }

  @Override
  public synchronized StreamProperties getStream(String streamName) {
    return streamNames.get(streamName);
  }

  @Override
  public synchronized StreamProperties getStream(long streamId) {
    return streams.get(streamId);
  }

  @Override
  public synchronized List<StreamProperties> getStreamsProperties() {
    return new ArrayList<>(streams.values());
  }


  @Override
  public synchronized boolean addStream(String streamName,
                                        StreamProperties streamProps) {
    if (null == streamNames.putIfAbsent(streamName, streamProps)) {
      streams.put(streamProps.getStreamId(), streamProps);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public synchronized StreamProperties removeStream(String streamName) {
    StreamProperties props = getStream(streamName);
    if (null != props) {
      return removeStream(props.getStreamId());
    } else {
      return null;
    }
  }

  @Override
  public synchronized StreamProperties removeStream(long streamId) {
    StreamProperties props = streams.remove(streamId);
    if (null == props) {
      return null;
    }
    streamNames.remove(props.getStreamName(), props);
    return props;
  }

}
