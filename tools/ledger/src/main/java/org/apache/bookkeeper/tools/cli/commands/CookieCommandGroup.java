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

package org.apache.bookkeeper.tools.cli.commands;

import org.apache.bookkeeper.tools.cli.BKCtl;
import org.apache.bookkeeper.tools.cli.commands.cookie.AdminCommand;
import org.apache.bookkeeper.tools.cli.commands.cookie.CreateCookieCommand;
import org.apache.bookkeeper.tools.cli.commands.cookie.DeleteCookieCommand;
import org.apache.bookkeeper.tools.cli.commands.cookie.GenerateCookieCommand;
import org.apache.bookkeeper.tools.cli.commands.cookie.GetCookieCommand;
import org.apache.bookkeeper.tools.cli.commands.cookie.UpdateCookieCommand;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliCommandGroup;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Commands that operates cookies.
 */
public class CookieCommandGroup extends CliCommandGroup<BKFlags> {

    private static final String NAME = "cookie";
    private static final String DESC = "Commands on operating cookies";

    private static final CliSpec<BKFlags> spec = CliSpec.<BKFlags>newBuilder()
        .withName(NAME)
        .withDescription(DESC)
        .withParent(BKCtl.NAME)
        .addCommand(new CreateCookieCommand())
        .addCommand(new DeleteCookieCommand())
        .addCommand(new GetCookieCommand())
        .addCommand(new UpdateCookieCommand())
        .addCommand(new GenerateCookieCommand())
        .addCommand(new AdminCommand())
        .build();

    public CookieCommandGroup() {
        super(spec);
    }
}
