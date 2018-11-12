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
package org.apache.bookkeeper.tools.cli;

import java.util.Iterator;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.Cli;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.tools.framework.CommandGroup;

/**
 * <b>bkctl</b> interacts and operates the <i>Apache BookKeeper</i> cluster.
 */
@Slf4j
public class BKCtl {

    public static final String NAME = "bkctl";

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        // load command groups
        ServiceLoader<CommandGroup> loader = ServiceLoader.load(
            CommandGroup.class, BKCtl.class.getClassLoader());

        CliSpec.Builder<BKFlags> specBuilder = CliSpec.<BKFlags>newBuilder()
            .withName(NAME)
            .withUsage(NAME + " [flags] [command group] [commands]")
            .withDescription(NAME + " interacts and operates Apache BookKeeper clusters")
            .withFlags(new BKFlags())
            .withConsole(System.out);

        Iterator<CommandGroup> cgIter = loader.iterator();
        while (cgIter.hasNext()) {
            CommandGroup<BKFlags> cg = cgIter.next();
            specBuilder.addCommand(cg);
        }

        CliSpec<BKFlags> spec = specBuilder.build();

        int retCode = Cli.runCli(spec, args);
        Runtime.getRuntime().exit(retCode);
    }

}
