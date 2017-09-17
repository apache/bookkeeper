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

package org.apache.distributedlog.stream.client.exceptions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * Test Case for stream related exceptions.
 */
public class StreamExceptionTest {

  @Test
  public void testStreamExistsException() {
    String streamName = "stream-exists";
    StreamExistsException see = new StreamExistsException(streamName);
    assertEquals("Stream 'stream-exists' already exists", see.getMessage());
    assertNull(see.getCause());
  }

  @Test
  public void testStreamNotFoundException() {
    String streamName = "stream-not-found";
    StreamNotFoundException snfe = new StreamNotFoundException(streamName);
    assertEquals("Stream 'stream-not-found' is not found", snfe.getMessage());
    assertNull(snfe.getCause());
  }

}
