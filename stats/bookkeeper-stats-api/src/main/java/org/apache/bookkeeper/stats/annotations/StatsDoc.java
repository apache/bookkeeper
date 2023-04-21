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

package org.apache.bookkeeper.stats.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Documenting the stats.
 */
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StatsDoc {

    /**
     * The name of the category to group stats together.
     *
     * @return name of the stats category.
     */
    String category() default "";

    /**
     * The scope of this stats.
     *
     * @return scope of this stats
     */
    String scope() default "";

    /**
     * The name of this stats.
     *
     * @return name of this stats
     */
    String name();

    /**
     * The help message of this stats.
     *
     * @return help message of this stats
     */
    String help();

    /**
     * The parent metric name.
     *
     * <p>It can used for analyzing the relationships
     * between the metrics, especially for the latency metrics.
     *
     * @return the parent metric name
     */
    String parent() default "";

    /**
     * The metric name of an operation that happens
     * after the operation of this metric.
     *
     * <p>similar as {@link #parent()}, it can be used for analyzing
     * the dependencies between metrics.
     *
     * @return the metric name of an operation that happens after the operation of this metric.
     */
    String happensAfter() default "";

}
