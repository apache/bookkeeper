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
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.conf.Validator;
import org.apache.bookkeeper.common.util.ReflectionUtils;

/**
 * Validator that validates a configuration setting is returning a given type of class.
 */
@Slf4j
@Data
public class ClassValidator<T> implements Validator {

    /**
     * Create a validator to validate if a setting is returning a class that extends from
     * <tt>interfaceClass</tt>.
     *
     * @param interfaceClass interface class
     * @return the validator that expects a setting return a class that extends from <tt>interfaceClass</tt>
     */
    public static <T> ClassValidator<T> of(Class<T> interfaceClass) {
        return new ClassValidator<>(interfaceClass);
    }

    private final Class<T> interfaceClass;

    @Override
    public boolean validate(String name, Object value) {
        if (value instanceof String) {
            try {
                ReflectionUtils.forName((String) value, interfaceClass);
                return true;
            } catch (RuntimeException re) {
                log.warn("Setting value of '{}' is not '{}' : {}",
                    name, interfaceClass.getName(), value, re);
                return false;
            }
        } else if (value instanceof Class) {
            Class cls = (Class) value;
            if (!interfaceClass.isAssignableFrom(cls)) {
                log.warn("Setting value of '{}' is not '{}' : {}",
                    name, interfaceClass.getName(), cls.getName());
                return false;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "Class extends " + interfaceClass.getName();
    }

    @Override
    public String documentation() {
        return "class extends `" + interfaceClass.getName() + "`";
    }
}
