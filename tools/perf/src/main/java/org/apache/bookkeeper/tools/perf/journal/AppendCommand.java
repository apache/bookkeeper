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

package org.apache.bookkeeper.tools.perf.journal;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.tools.common.BKCommand;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.tools.perf.journal.JournalWriter.Flags;
import org.apache.commons.configuration.CompositeConfiguration;

/**
 * Command to write log records to distributedlog streams.
 */
@Slf4j
public class AppendCommand extends BKCommand<Flags> {

    private static final String NAME = "append";
    private static final String DESC = "Append entries to a bookie journal";

    public AppendCommand() {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(new Flags())
            .build());
    }

    @Override
    protected boolean apply(ServiceURI serviceURI,
                            CompositeConfiguration conf,
                            BKFlags globalFlags, Flags cmdFlags) {
        JournalWriter writer = new JournalWriter(conf, cmdFlags);
        writer.run();
        return true;
    }

}
