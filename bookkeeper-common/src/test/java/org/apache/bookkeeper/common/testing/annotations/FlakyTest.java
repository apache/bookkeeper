/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.testing.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Annotation to mark a test as flaky.
 *
 * <p>Flaky tests are tests that produce inconsistent results,
 * occasionally failing or passing without changes to the code under test.
 * This could be due to external factors such as timing issues, resource contention,
 * dependency on non-deterministic data, or integration with external systems.
 *
 * <p>Tests marked with this annotation are excluded from execution
 * in CI pipelines and Maven commands by default, to ensure a reliable and
 * deterministic build process. However, they can still be executed manually
 * or in specific environments for debugging and resolution purposes.
 *
 * <p>Usage:
 * <pre>
 * {@code
 * @FlakyTest
 * public void testSomething() {
 *     // Test logic here
 * }
 * }
 * </pre>
 *
 * <p>It is recommended to investigate and address the root causes of flaky tests
 * rather than relying on this annotation long-term.
 */
@Documented
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Tag("flaky")
@Test
public @interface FlakyTest {

    /**
     * Context information such as links to discussion thread, tracking issues etc.
     *
     * @return context information about this flaky test.
     */
    String value();
}
