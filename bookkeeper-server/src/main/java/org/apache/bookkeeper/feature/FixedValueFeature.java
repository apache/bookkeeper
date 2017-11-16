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
package org.apache.bookkeeper.feature;

/**
 * A feature implementation that has a fixed value of availability.
 */
public class FixedValueFeature implements Feature {
    protected final String name;
    protected int availability;

    public FixedValueFeature(String name, int availability) {
        this.name = name;
        this.availability = availability;
    }

    public FixedValueFeature(String name, boolean available) {
        this.name = name;
        this.availability = available ? FEATURE_AVAILABILITY_MAX_VALUE : 0;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public int availability() {
        return availability;
    }

    @Override
    public boolean isAvailable() {
        return availability() > 0;
    }
}
