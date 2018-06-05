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
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.common.BKFlags;

/**
 * A test base providing an environment for run a command.
 */
@Slf4j
public class CommandTestBase {

    protected final BKFlags bkFlags;
    protected final ServerConfiguration conf;

    public CommandTestBase() {
        this.conf = new ServerConfiguration();
        this.conf.setMetadataServiceUri("zk://127.0.0.1/path/to/ledgers");
        this.bkFlags = new BKFlags();
    }

}
