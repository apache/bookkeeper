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

package org.apache.bookkeeper.common.conf.validators;

import lombok.Data;
import org.apache.bookkeeper.common.conf.Validator;

/**
 * Validator that validates a configuration value is in a numeric range.
 */
@Data
public class RangeValidator implements Validator {

    /**
     * A numeric range that checks the lower bound.
     *
     * @param min the minimum acceptable value
     * @return a numeric range that checks the lower bound
     */
    public static RangeValidator atLeast(Number min) {
        return new RangeValidator(min, null);
    }

    /**
     * A numeric range that checks the upper bound.
     *
     * @param max the maximum acceptable value
     * @return a numeric range that checks the upper bound
     */
    public static RangeValidator atMost(Number max) {
        return new RangeValidator(null, max);
    }

    /**
     * A numeric range that checks both lower and upper bounds.
     *
     * @param min the minimum acceptable value
     * @param max the maximum acceptable value
     * @return a numeric range that checks both lower and upper bounds
     */
    public static RangeValidator between(Number min, Number max) {
        return new RangeValidator(min, max);
    }

    private final Number min;
    private final Number max;

    @Override
    public boolean validate(String name, Object value) {
        if (value instanceof Number) {
            Number n = (Number) value;
            if (min != null && n.doubleValue() < min.doubleValue()) {
                return false;
            } else {
                return max == null || n.doubleValue() <= max.doubleValue();
            }
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        if (null == min) {
            return "[... , " + max + "]";
        } else if (null == max) {
            return "[" + min + ", ...]";
        } else {
            return "[" + min + ", " + max + "]";
        }
    }

    @Override
    public String documentation() {
        return toString();
    }
}
