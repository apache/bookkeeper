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

package com.coreos.jetcd.internal.impl;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.api.WatchGrpc;
import lombok.extern.slf4j.Slf4j;

/**
 * Keep a reference to etcd internal connection manager.
 */
@Slf4j
public class EtcdConnectionManager {

    private final ClientImpl client;
    private ClientConnectionManager connMgr;

    public EtcdConnectionManager(Client client) {
        this((ClientImpl) client);
    }

    EtcdConnectionManager(ClientImpl client) {
        this.client = client;
        try {
            this.connMgr = EtcdClientUtils.getField(
                client, "connectionManager"
            );
        } catch (NoSuchFieldException e) {
            log.error("No `connectionManager` field found in etcd client", e);
            throw new RuntimeException(
                "No `connectionManager` field found in etcd client", e);
        } catch (IllegalAccessException e) {
            log.error("Illegal access to `connectionManager` field in etcd client", e);
            throw new RuntimeException(
                "Illegal access to `connectionManager` field in etcd client", e);
        }
    }

    /**
     * Create a watch api grpc stub.
     *
     * @return a watch api grpc stub.
     */
    public WatchGrpc.WatchStub newWatchStub() {
        return connMgr.newStub(WatchGrpc::newStub);
    }

}
