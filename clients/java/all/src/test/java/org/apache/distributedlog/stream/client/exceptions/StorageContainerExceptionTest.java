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

import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.junit.Test;

/**
 * Unit test for {@link StorageContainerException}.
 */
public class StorageContainerExceptionTest {

  @Test
  public void testGetMessage() {
    StatusCode code = StatusCode.STALE_GROUP_INFO;
    StorageContainerException exception = new StorageContainerException(code, "test-get-message");
    assertEquals(code, exception.getCode());
    assertEquals("StorageContainerError : StatusCode = " + code + ", Error = test-get-message", exception.getMessage());
    assertNull(exception.getCause());
  }

}
