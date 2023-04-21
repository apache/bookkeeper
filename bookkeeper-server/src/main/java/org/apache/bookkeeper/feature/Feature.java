package org.apache.bookkeeper.feature;

/*
 *
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
 *
 */

/**
 * This interface represents a feature.
 */
public interface Feature {

    int FEATURE_AVAILABILITY_MAX_VALUE = 100;

    /**
     * Returns a textual representation of the feature.
     *
     * @return name of the feature.
     */
    String name();

    /**
     * Returns the availability of this feature, an integer between 0 and 100.
     *
     * @return the availability of this feature.
     */
    int availability();

    /**
     * Whether this feature is available or not.
     *
     * @return true if this feature is available, otherwise false.
     */
    boolean isAvailable();
}

