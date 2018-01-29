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
package org.apache.bookkeeper.stream.server.service;

import java.io.IOException;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.Stats;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stream.server.conf.BookieConfiguration;

/**
 * A {@link org.apache.bookkeeper.common.component.LifecycleComponent} that runs stats provider.
 */
public class StatsProviderService extends AbstractLifecycleComponent<BookieConfiguration> {

    private final ServerConfiguration serverConf = new ServerConfiguration();
    private final StatsProvider statsProvider;

    public StatsProviderService(BookieConfiguration conf) {
        super("stats-provider", conf, NullStatsLogger.INSTANCE);
        this.serverConf.loadConf(conf.getUnderlyingConf());
        Stats.loadStatsProvider(conf);
        this.statsProvider = Stats.get();
    }

    public StatsProvider getStatsProvider() {
        return statsProvider;
    }

    @Override
    protected void doStart() {
        this.statsProvider.start(serverConf);
    }

    @Override
    protected void doStop() {
        this.statsProvider.stop();
    }

    @Override
    protected void doClose() throws IOException {
        // do nothing
    }
}
