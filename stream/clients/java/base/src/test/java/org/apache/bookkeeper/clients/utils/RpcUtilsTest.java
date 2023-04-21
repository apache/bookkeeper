/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.clients.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

/**
 * Unit test {@link RpcUtils}.
 */
public class RpcUtilsTest {

    @Test
    public void testIsContainerNotFound() {
        StatusRuntimeException trueSRE = new StatusRuntimeException(Status.NOT_FOUND);
        assertTrue(RpcUtils.isContainerNotFound(trueSRE));
        StatusRuntimeException falseSRE = new StatusRuntimeException(Status.INTERNAL);
        assertFalse(RpcUtils.isContainerNotFound(falseSRE));

        StatusException trueSE = new StatusException(Status.NOT_FOUND);
        assertTrue(RpcUtils.isContainerNotFound(trueSE));
        StatusException falseSE = new StatusException(Status.INTERNAL);
        assertFalse(RpcUtils.isContainerNotFound(falseSE));

        Exception unknownException = new Exception("unknown");
        assertFalse(RpcUtils.isContainerNotFound(unknownException));
    }

}
