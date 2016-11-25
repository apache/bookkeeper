/**
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
package org.apache.bookkeeper.sasl;

import java.io.IOException;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.bookie.BookieConnectionPeer;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;

public class SASLBookieAuthProviderFactory implements org.apache.bookkeeper.auth.BookieAuthProvider.Factory {

    private ServerConfiguration configuration;

    @Override
    public void init(ServerConfiguration conf) throws IOException {
        this.configuration = conf;
    }

    @Override
    public BookieAuthProvider newProvider(BookieConnectionPeer connection,
        BookkeeperInternalCallbacks.GenericCallback<Void> completeCb) {
        return new SASLBookieAuthProvider(configuration, connection, completeCb);
    }

    @Override
    public String getPluginName() {
        return SaslConstants.PLUGIN_NAME;
    }

}
