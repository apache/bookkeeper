/*
 *
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
 *
 */
package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;

/**
 * Factory to create {@link org.apache.bookkeeper.proto.PerChannelBookieClient}.
 */
interface PerChannelBookieClientFactory {

    /**
     * Create a {@link org.apache.bookkeeper.proto.PerChannelBookieClient} to
     * <i>address</i>.
     *
     * @return the client connected to address.
     * @throws SecurityException
     */
    PerChannelBookieClient create(BookieId address, PerChannelBookieClientPool pcbcPool,
                                  SecurityHandlerFactory shFactory,
                                  boolean forceUseV3) throws SecurityException;
}
