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
import org.mockito.MockSettings;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.withSettings;

/**
 * A test base providing utility methods to mock environment to run commands test.
 */
@Slf4j
public abstract class MockCommandSupport {

    private Map<String, MockedConstruction<?>> miscMockedConstructions = new HashMap<>();
    private Map<String, MockedStatic<?>> miscMockedStatic = new HashMap<>();

    private static String mockedClassKey(Class clazz) {
        return clazz.getName();
    }

    protected <T> MockedStatic<T> mockStatic(Class<T> classToMock, MockSettings settings) {
        final String key = mockedClassKey(classToMock);
        miscMockedStatic.remove(key);
        final MockedStatic mockedStatic = Mockito.mockStatic(classToMock, settings);
        miscMockedStatic.put(key, mockedStatic);
        return mockedStatic;
    }

    protected <T> MockedStatic<T> mockStatic(Class<T> classToMock, Answer defaultAnswer) {
        return mockStatic(classToMock, withSettings().defaultAnswer(defaultAnswer));

    }

    protected <T> MockedStatic<T> mockStatic(Class<T> classToMock) {
        return mockStatic(classToMock, withSettings());
    }

    protected <T> MockedStatic<T> getMockedStatic(Class<T> mockedClass) {
        final MockedStatic<T> mockedStatic = (MockedStatic<T>)
                miscMockedStatic.get(mockedClassKey(mockedClass));
        if (mockedStatic == null) {
            throw new RuntimeException("Cannot get mocked static for class "
                    + mockedClass.getName() + ", not mocked yet.");
        }
        return mockedStatic;
    }

    protected <T> MockedConstruction<T> mockConstruction(Class<T> classToMock) {
        return mockConstruction(classToMock, null);
    }

    protected <T> MockedConstruction<T> mockConstruction(Class<T> classToMock,
                                                         MockedConstruction.MockInitializer<T> initializer) {
        return mockConstruction(classToMock, withSettings(), initializer);
    }


    protected <T> MockedConstruction<T> mockConstruction(Class<T> classToMock,
                                                         MockSettings settings,
                                                         MockedConstruction.MockInitializer<T> initializer) {
        final String key = mockedClassKey(classToMock);
        miscMockedConstructions.remove(key);

        final MockedConstruction<T> mockedConstruction = Mockito.mockConstruction(classToMock, initializer);
        miscMockedConstructions.put(key, mockedConstruction);
        return Mockito.mockConstruction(classToMock, initializer);
    }

    protected <T> MockedConstruction<T> getMockedConstruction(Class<T> mockedClass) {

        final MockedConstruction<T> mockedConstruction = (MockedConstruction<T>)
                miscMockedConstructions.get(mockedClassKey(mockedClass));
        if (mockedConstruction == null) {
            throw new RuntimeException("Cannot get mocked construction for class "
                    + mockedClass.getName() + ", not mocked yet.");
        }
        return mockedConstruction;
    }



    @After
    public void afterMethod() {
        miscMockedStatic.values().forEach(MockedStatic::close);
        miscMockedStatic.clear();

        miscMockedConstructions.values().forEach(MockedConstruction::close);
        miscMockedConstructions.clear();
    }

}
