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

package org.apache.distributedlog.stream.protocol;

import org.apache.distributedlog.stream.proto.FixedRangeSplitPolicy;
import org.apache.distributedlog.stream.proto.RangeKeyType;
import org.apache.distributedlog.stream.proto.RetentionPolicy;
import org.apache.distributedlog.stream.proto.SegmentRollingPolicy;
import org.apache.distributedlog.stream.proto.SizeBasedSegmentRollingPolicy;
import org.apache.distributedlog.stream.proto.SplitPolicy;
import org.apache.distributedlog.stream.proto.StreamConfiguration;
import org.apache.distributedlog.stream.proto.TimeBasedRetentionPolicy;

/**
 * Protocol related constants used across the project.
 */
public final class ProtocolConstants {

  private ProtocolConstants() {}

  public static final String SCHEME = "stream";

  //
  // Storage Container Constants
  //

  // the root storage container id
  public static final long ROOT_STORAGE_CONTAINER_ID = 0L;
  // the default number of storage containers
  public static final int DEFAULT_NUM_STORAGE_CONTAINERS = 1024;
  // invalid storage container id
  public static final long INVALID_STORAGE_CONTAINER_ID = -1L;

  //
  // Stream Related Constants
  //

  // invalid collection id
  public static final long INVALID_COLLECTION_ID = -1L;
  // invalid stream id
  public static final long INVALID_STREAM_ID = -1L;
  // invalid metadata range id
  public static final long INVALID_RANGE_ID = -1L;

  // the minimum data range id - ids between [1L - 1024L) are reserved now.
  public static final long MIN_DATA_RANGE_ID = 1024L;

  // the minimum data stream id - stream id between [1L - 1024L) are reserved.
  public static final long MIN_DATA_STREAM_ID = 1024L;

  // default split policy
  public static final SplitPolicy DEFAULT_SPLIT_POLICY =
    SplitPolicy.newBuilder()
      .setFixedRangePolicy(
        FixedRangeSplitPolicy.newBuilder()
          .setNumRanges(2))
      .build();
  // default rolling policy
  public static final SegmentRollingPolicy DEFAULT_SEGMENT_ROLLING_POLICY =
    SegmentRollingPolicy.newBuilder()
      .setSizePolicy(
        SizeBasedSegmentRollingPolicy.newBuilder()
          .setMaxSegmentSize(128 * 1024 * 1024))
      .build();
  // default retention policy
  public static final RetentionPolicy DEFAULT_RETENTION_POLICY =
    RetentionPolicy.newBuilder()
      .setTimePolicy(
        TimeBasedRetentionPolicy.newBuilder()
          .setRetentionMinutes(-1))
      .build();
  // default stream configuration
  public static final int INIT_NUM_RANGES = 24;
  public static final int MIN_NUM_RANGES = 24;
  public static final StreamConfiguration DEFAULT_STREAM_CONF =
    StreamConfiguration.newBuilder()
      .setKeyType(RangeKeyType.HASH)
      .setInitialNumRanges(INIT_NUM_RANGES)
      .setMinNumRanges(MIN_NUM_RANGES)
      .setRetentionPolicy(DEFAULT_RETENTION_POLICY)
      .setRollingPolicy(DEFAULT_SEGMENT_ROLLING_POLICY)
      .setSplitPolicy(DEFAULT_SPLIT_POLICY)
      .build();

}
