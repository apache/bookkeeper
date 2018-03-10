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
package org.apache.bookkeeper.tools.cli.helpers;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;

/**
 * This is a mixin class for commands that needs a bookkeeper client.
 */
@Slf4j
public abstract class ClientCommand implements Command {

    @Override
    public void run(ServerConfiguration conf) throws Exception {
        // cast the server configuration to a client configuration object.
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        run(clientConf);
    }

    protected void run(ClientConfiguration conf) throws Exception {
        try (BookKeeper bk = BookKeeper.newBuilder(conf).build()) {
            run(bk);
        }
    }

    protected abstract void run(BookKeeper bk) throws Exception;


}
