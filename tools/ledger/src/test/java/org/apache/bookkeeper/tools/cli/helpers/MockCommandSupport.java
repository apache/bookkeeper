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

import static org.mockito.Mockito.withSettings;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.mockito.MockSettings;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

/**
 * A test base providing utility methods to mock environment to run commands test.
 */
@Slf4j
@SuppressWarnings("unchecked")
public abstract class MockCommandSupport {

    private Map<String, MockedConstruction<?>> miscMockedConstructions = new HashMap<>();
    private Map<String, MockedStatic<?>> miscMockedStatic = new HashMap<>();

    private static String mockedClassKey(Class clazz) {
        return clazz.getName();
    }

    protected <T> MockedStatic<T> mockStatic(Class<T> classToMock, MockSettings settings) {
        final String key = mockedClassKey(classToMock);
        final MockedStatic<?> prev = miscMockedStatic.remove(key);
        if (prev != null) {
            prev.close();
        }
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

    protected <T> MockedStatic<T> unsafeGetMockedStatic(Class<T> mockedClass) {
        return  (MockedStatic<T>) miscMockedStatic.get(mockedClassKey(mockedClass));
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
        return mockConstruction(classToMock, (mocked, context) -> {});
    }

    protected <T> MockedConstruction<T> mockConstruction(Class<T> classToMock,
                                                         MockedConstruction.MockInitializer<T> initializer) {
        return mockConstruction(classToMock, withSettings(), initializer);
    }


    protected <T> MockedConstruction<T> mockConstruction(Class<T> classToMock,
                                                         MockSettings settings,
                                                         MockedConstruction.MockInitializer<T> initializer) {
        final String key = mockedClassKey(classToMock);
        final MockedConstruction<?> prev = miscMockedConstructions.remove(key);
        if (prev != null) {
            prev.close();
        }

        final MockedConstruction<T> mockedConstruction = Mockito.mockConstruction(classToMock, settings, initializer);
        miscMockedConstructions.put(key, mockedConstruction);
        return mockedConstruction;
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
