/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.tools.perf;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.Cli;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * <b>bkperf</b> evaluates the performance of <i>Apache BookKeeper</i> cluster.
 */
@Slf4j
public class BKPerf {

    public static final String NAME = "bkperf";

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        CliSpec.Builder<BKFlags> specBuilder = CliSpec.<BKFlags>newBuilder()
            .withName(NAME)
            .withUsage(NAME + " [flags] [command group] [commands]")
            .withDescription(NAME + " evaluates the performance of Apache BookKeeper clusters")
            .withFlags(new BKFlags())
            .withConsole(System.out)
            .addCommand(new DlogPerfCommandGroup())
            .addCommand(new TablePerfCommandGroup())
            .addCommand(new JournalPerfCommandGroup());

        CliSpec<BKFlags> spec = specBuilder.build();

        int retCode = Cli.runCli(spec, args);
        Runtime.getRuntime().exit(retCode);
    }

}
