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
package org.apache.bookkeeper.stream.cli;

import static org.apache.bookkeeper.tools.common.BKCommandCategories.CATEGORY_INFRA_SERVICE;

import org.apache.bookkeeper.stream.cli.commands.cluster.InitClusterCommand;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliCommandGroup;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Commands that operates stream storage namespaces.
 */
public class ClusterCommandGroup extends CliCommandGroup<BKFlags> {

    private static final String NAME = "cluster";
    private static final String DESC = "Commands on administrating bookkeeper clusters";

    private static final CliSpec<BKFlags> spec = CliSpec.<BKFlags>newBuilder()
        .withName(NAME)
        .withDescription(DESC)
        .withParent("bkctl")
        .withCategory(CATEGORY_INFRA_SERVICE)
        .addCommand(new InitClusterCommand())
        .build();

    public ClusterCommandGroup() {
        super(spec);
    }
}
