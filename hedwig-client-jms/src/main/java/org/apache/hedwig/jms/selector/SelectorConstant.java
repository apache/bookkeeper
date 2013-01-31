/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.jms.selector;

import java.util.Collections;
import java.util.Set;

/**
 * A constant in the AST ends up getting pushed into the stack as this.
 */
public class SelectorConstant {

    public enum SelectorDataType {BOOLEAN, INT, DOUBLE, STRING, STRING_SET}

    public final SelectorDataType type;

    private final Boolean boolValue;
    private final Integer intValue;
    private final Double doubleValue;
    private final String stringValue;

    private final Set<String> stringSet;

    public SelectorConstant(Boolean boolValue) {
        this.type = SelectorDataType.BOOLEAN;
        this.boolValue = boolValue;
        this.intValue = null;
        this.doubleValue = null;
        this.stringValue = null;
        this.stringSet = null;
    }

    public SelectorConstant(Integer intValue) {
        this.type = SelectorDataType.INT;
        this.boolValue = null;
        this.intValue = intValue;
        this.doubleValue = null;
        this.stringValue = null;
        this.stringSet = null;
    }

    public SelectorConstant(Double doubleValue) {
        this.type = SelectorDataType.DOUBLE;
        this.boolValue = null;
        this.intValue = null;
        this.doubleValue = doubleValue;
        this.stringValue = null;
        this.stringSet = null;
    }

    public SelectorConstant(String stringValue) {
        this.type = SelectorDataType.STRING;
        this.boolValue = null;
        this.intValue = null;
        this.doubleValue = null;
        this.stringValue = stringValue;
        this.stringSet = null;
    }

    public SelectorConstant(Set<String> stringSet) {
        this.type = SelectorDataType.STRING_SET;
        this.boolValue = null;
        this.intValue = null;
        this.doubleValue = null;
        this.stringValue = null;
        this.stringSet = stringSet;
    }

    public void addToStringSet(String value) throws ParseException {
        if (SelectorDataType.STRING_SET != type)
            throw new ParseException(getClass() + " Attempting to add to a non-string_list type : " + type);
        if (null == this.stringSet) throw new ParseException(getClass() +
            " Attempting to add to a null string_list : " + stringSet);
        this.stringSet.add(value);
    }

    public boolean isNull() {
        switch (type) {
            case BOOLEAN:
                return null == boolValue;
            case INT:
                return null == intValue;
            case DOUBLE:
                return null == doubleValue;
            case STRING:
                return null == stringValue;
            case STRING_SET:
                return null == stringSet;
            default:
                throw new IllegalStateException(getClass() + " Unknown type ? " + type);
        }
    }

    public boolean isTrue() throws SelectorEvaluationException {
        switch (type) {
            case BOOLEAN:
                return null != boolValue && Boolean.TRUE.equals(boolValue);
            default:
                throw new SelectorEvaluationException(getClass() + " Unexpected type " + type +
                    ", value " + getValueAsString());
        }
    }

    public String getValueAsString() throws SelectorEvaluationException {
        switch (type) {
            case BOOLEAN:
                return Boolean.toString(boolValue);
            case INT:
                return Integer.toString(intValue);
            case DOUBLE:
                return Double.toString(doubleValue);
            case STRING:
                return stringValue;
            case STRING_SET:
                return null != stringSet ? stringSet.toString() : "null";
            default:
                throw new SelectorEvaluationException(getClass() + " Unexpected type : " + type);
        }
    }

    public String toString() {
        try {
            switch (type) {
                case BOOLEAN:
                    return "boolean : " + getValueAsString();
                case INT:
                    return "int : " + getValueAsString();
                case DOUBLE:
                    return "double : " + getValueAsString();
                case STRING:
                    return "string : " + getValueAsString();
                case STRING_SET:
                    return "string_list : " + getValueAsString();
                default:
                    throw new IllegalStateException(getClass() + " Unexpected type");
            }
        } catch (SelectorEvaluationException seEx) {
            throw new IllegalStateException(getClass() + " Unexpected exception", seEx);
        }
    }

    public Boolean getBoolValue() throws SelectorEvaluationException {
        if (SelectorDataType.BOOLEAN != type)
            throw new SelectorEvaluationException(getClass() + " Illegal access to boolean for type " + type);
        return boolValue;
    }

    public Integer getIntValue() throws SelectorEvaluationException {
        if (SelectorDataType.INT != type)
            throw new SelectorEvaluationException(getClass() + " Illegal access to boolean for type " + type);
        return intValue;
    }

    public Double getDoubleValue() throws SelectorEvaluationException {
        if (SelectorDataType.DOUBLE != type)
            throw new SelectorEvaluationException(getClass() + " Illegal access to boolean for type " + type);
        return doubleValue;
    }

    public String getStringValue() throws SelectorEvaluationException {
        if (SelectorDataType.STRING != type)
            throw new SelectorEvaluationException(getClass() + " Illegal access to boolean for type " + type);
        return stringValue;
    }

    public Set<String> getStringSet() throws SelectorEvaluationException {
        if (SelectorDataType.STRING_SET != type)
            throw new SelectorEvaluationException(getClass() + " Illegal access to boolean for type " + type);
        // wrap to prevent changes ...
        return Collections.unmodifiableSet(stringSet);
    }
}
