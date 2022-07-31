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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;

import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;



/**
 * A test base providing an environment for run a command.
 */
@Slf4j
@SuppressWarnings("unchecked")
public class CommandTestBase extends MockCommandSupport {

    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();
    protected String[] journalDirsName;
    protected String[] ledgerDirNames;
    protected String[] indexDirNames;

    protected final BKFlags bkFlags;
    protected MockedStatic<MetadataDrivers> mockMetadataDrivers() {
        if (unsafeGetMockedStatic(MetadataDrivers.class) == null) {
            mockStatic(MetadataDrivers.class);
        }
        return getMockedStatic(MetadataDrivers.class);
    }

    protected void mockMetadataDriversWithRegistrationManager(RegistrationManager registrationManager) {
        mockMetadataDrivers().when(() -> MetadataDrivers
                .runFunctionWithRegistrationManager(any(ServerConfiguration.class), any(Function.class))
        ).then(invocation -> {
            Function<RegistrationManager, ?> func = invocation.getArgument(1);
            func.apply(registrationManager);
            return true;
        });
    }

    protected void mockMetadataDriversWithMetadataBookieDriver(MetadataBookieDriver metadataBookieDriver) {
        mockMetadataDrivers().when(() -> MetadataDrivers
                .runFunctionWithMetadataBookieDriver(any(ServerConfiguration.class), any(Function.class))
        ).then(invocation -> {
            Function<MetadataBookieDriver, ?> func = invocation.getArgument(1);
            func.apply(metadataBookieDriver);
            return true;
        });
    }

    protected void mockMetadataDriversWithLedgerManagerFactory(LedgerManagerFactory ledgerManagerFactory) {
        mockMetadataDrivers().when(() -> MetadataDrivers
                .runFunctionWithLedgerManagerFactory(any(ServerConfiguration.class), any(Function.class))
        ).then(invocation -> {
            Function<LedgerManagerFactory, ?> func = invocation.getArgument(1);
            func.apply(ledgerManagerFactory);
            return true;
        });
    }

    protected void mockServerConfigurationConstruction() {
        mockServerConfigurationConstruction(null);
    }

    protected void mockServerConfigurationConstruction(Consumer<ServerConfiguration> consumer) {
        mockConstruction(ServerConfiguration.class, (serverConfiguration, context) -> {
            final ServerConfiguration defaultConf = new ServerConfiguration();
            doReturn("zk://127.0.0.1/path/to/ledgers").when(serverConfiguration).getMetadataServiceUri();
            if (journalDirsName != null) {
                doReturn(journalDirsName).when(serverConfiguration).getJournalDirNames();
                doCallRealMethod().when(serverConfiguration).getJournalDirs();
            }
            if (ledgerDirNames != null) {
                doReturn(ledgerDirNames).when(serverConfiguration).getLedgerDirNames();
                doCallRealMethod().when(serverConfiguration).getLedgerDirs();
            }
            if (indexDirNames != null) {
                doReturn(indexDirNames).when(serverConfiguration).getIndexDirNames();
                doCallRealMethod().when(serverConfiguration).getIndexDirs();
            }
            doReturn(defaultConf.getDiskUsageThreshold()).when(serverConfiguration).getDiskUsageThreshold();
            doReturn(defaultConf.getDiskUsageWarnThreshold()).when(serverConfiguration).getDiskUsageWarnThreshold();
            if (consumer != null) {
                consumer.accept(serverConfiguration);
            }
        });
    }

    protected void mockClientConfigurationConstruction() {
        mockClientConfigurationConstruction(null);
    }

    protected void mockClientConfigurationConstruction(Consumer<ClientConfiguration> consumer) {
        mockConstruction(ClientConfiguration.class, (clientConfiguration, context) -> {
            doReturn("zk://127.0.0.1/path/to/ledgers").when(clientConfiguration).getMetadataServiceUri();
            doReturn(true).when(clientConfiguration).getBookieAddressResolverEnabled();
            if (consumer != null) {
                consumer.accept(clientConfiguration);
            }
        });
    }

    protected void mockBookKeeperAdminConstruction() {
        mockBookKeeperAdminConstruction(null);
    }

    protected void mockBookKeeperAdminConstruction(Consumer<BookKeeperAdmin> consumer) {
        mockConstruction(BookKeeperAdmin.class, (bookKeeperAdmin, context) -> {
            if (consumer != null) {
                consumer.accept(bookKeeperAdmin);
            }
        });
    }

    public CommandTestBase() {
        this.bkFlags = new BKFlags();
    }
}
