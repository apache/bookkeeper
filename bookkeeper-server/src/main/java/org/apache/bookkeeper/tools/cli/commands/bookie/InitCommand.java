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
package org.apache.bookkeeper.tools.cli.commands.bookie;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * A command to initialize new bookie.
 */
public class InitCommand extends BookieCommand<CliFlags> {

    private static final String NAME = "init";
    private static final String DESC = "Initialize new bookie.";

    public InitCommand() {
        super(CliSpec.newBuilder()
                .withName(NAME)
                .withDescription(DESC)
                .withFlags(new CliFlags())
                .build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, CliFlags cmdFlags) {

        boolean result = false;
        try {
            result = BookKeeperAdmin.initBookie(conf);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
        return result;
    }
}
