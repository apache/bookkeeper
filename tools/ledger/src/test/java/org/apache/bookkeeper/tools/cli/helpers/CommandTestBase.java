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
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;

/**
 * A test base providing an environment for run a command.
 */
@Slf4j
public class CommandTestBase {

    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();
    protected String[] journalDirsName;
    protected String[] ledgerDirNames;

    protected final BKFlags bkFlags;
    protected MockedConstruction<ServerConfiguration> serverConfigurationMockedConstruction;
    protected MockedConstruction<ClientConfiguration> clientConfigurationMockedConstruction;
    protected MockedConstruction<BookKeeperAdmin> bookkeeperAdminMockedConstruction;
    private MockedStatic<MetadataDrivers> metadataDriversMockedStatic;
    protected ServerConfiguration conf;
    private List<MockedConstruction<?>> miscMockedConstructions = new ArrayList<>();
    private List<MockedStatic<?>> miscMockedStatic = new ArrayList<>();

    protected void addMockedConstruction(MockedConstruction<?> mockedConstruction) {
        miscMockedConstructions.add(mockedConstruction);
    }
    protected void addMockedStatic(MockedStatic<?> mockedStatic) {
        miscMockedStatic.add(mockedStatic);
    }

    protected void initMockedMetadataDrivers() {
        if (metadataDriversMockedStatic == null) {
            metadataDriversMockedStatic = mockStatic(MetadataDrivers.class);
        }
    }

    protected void initMockedMetadataDriversWithRegistrationManager(RegistrationManager registrationManager) {
        initMockedMetadataDrivers();

        metadataDriversMockedStatic.when(() -> MetadataDrivers
                .runFunctionWithRegistrationManager(any(ServerConfiguration.class), any(Function.class))
        ).then(invocation -> {
            Function<RegistrationManager, ?> func = invocation.getArgument(1);
            func.apply(registrationManager);
            return true;
        });
    }

    protected void initMockedMetadataDriversWithMetadataBookieDriver(MetadataBookieDriver metadataBookieDriver) {
        initMockedMetadataDrivers();

        metadataDriversMockedStatic.when(() -> MetadataDrivers
                .runFunctionWithMetadataBookieDriver(any(ServerConfiguration.class), any(Function.class))
        ).then(invocation -> {
            Function<MetadataBookieDriver, ?> func = invocation.getArgument(1);
            func.apply(metadataBookieDriver);
            return true;
        });
    }

    protected void initMockedMetadataDriversWithLedgerManagerFactory(LedgerManagerFactory ledgerManagerFactory) {
        initMockedMetadataDrivers();

        metadataDriversMockedStatic.when(() -> MetadataDrivers
                .runFunctionWithLedgerManagerFactory(any(ServerConfiguration.class), any(Function.class))
        ).then(invocation -> {
            Function<LedgerManagerFactory, ?> func = invocation.getArgument(1);
            func.apply(ledgerManagerFactory);
            return true;
        });
    }

    protected void createMockedServerConfiguration() {
        createMockedServerConfiguration(null);
    }

    protected void createMockedServerConfiguration(Consumer<ServerConfiguration> consumer) {
        serverConfigurationMockedConstruction = mockConstruction(ServerConfiguration.class, (serverConfiguration, context) -> {
            doReturn("zk://127.0.0.1/path/to/ledgers").when(serverConfiguration).getMetadataServiceUri();
            String[] indexDirs = new String[3];
            for (int i = 0; i < indexDirs.length; i++) {
                File dir = this.testDir.newFile();
                dir.mkdirs();
                indexDirs[i] = dir.getAbsolutePath();
            }
            doReturn(indexDirs).when(serverConfiguration).getIndexDirNames();
            if (journalDirsName != null) {
                doReturn(journalDirsName).when(serverConfiguration).getJournalDirNames();
            }
            if (journalDirsName != null) {
                doReturn(journalDirsName).when(serverConfiguration).getJournalDirNames();
                doCallRealMethod().when(serverConfiguration).getJournalDirs();
            }
            if (ledgerDirNames != null) {
                doReturn(ledgerDirNames).when(serverConfiguration).getLedgerDirNames();
                doCallRealMethod().when(serverConfiguration).getLedgerDirs();
            }
            if (consumer != null) {
                consumer.accept(serverConfiguration);
            }
        });
    }

    protected void createMockedClientConfiguration() {
        createMockedClientConfiguration(null);
    }

    protected void createMockedClientConfiguration(Consumer<ClientConfiguration> consumer) {
        clientConfigurationMockedConstruction = mockConstruction(ClientConfiguration.class, (clientConfiguration, context) -> {
            doReturn("zk://127.0.0.1/path/to/ledgers").when(clientConfiguration).getMetadataServiceUri();
            if (consumer != null) {
                consumer.accept(clientConfiguration);
            }
        });
    }

    protected void createMockedBookKeeperAdmin() {
        createMockedBookKeeperAdmin(null);
    }

    protected void createMockedBookKeeperAdmin(Consumer<BookKeeperAdmin> consumer) {
        bookkeeperAdminMockedConstruction = mockConstruction(BookKeeperAdmin.class, (bookKeeperAdmin, context) -> {
            if (consumer != null) {
                consumer.accept(bookKeeperAdmin);
            }
        });
    }

    public CommandTestBase() {
        this.bkFlags = new BKFlags();
    }

    @After
    public void afterMethod() {
        if (serverConfigurationMockedConstruction != null) {
            serverConfigurationMockedConstruction.close();
        }
        if (clientConfigurationMockedConstruction != null) {
            clientConfigurationMockedConstruction.close();
        }
        if (bookkeeperAdminMockedConstruction != null) {
            bookkeeperAdminMockedConstruction.close();
        }
        if (metadataDriversMockedStatic != null) {
            metadataDriversMockedStatic.close();
        }
        miscMockedConstructions.forEach(MockedConstruction::close);
        miscMockedConstructions.clear();
        miscMockedStatic.forEach(MockedStatic::close);
        miscMockedStatic.clear();
    }

}
