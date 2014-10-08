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

import org.apache.hedwig.jms.message.MessageImpl;

/**
 * Comparison of values ..
 */
public abstract class ValueComparisonFunction extends BinaryExprFunction {

    protected SelectorConstant evaluateImpl(SelectorConstant left, SelectorConstant right,
                                            MessageImpl message) throws SelectorEvaluationException {

        switch (left.type) {
            case INT: {
                switch (right.type) {
                    case INT:
                        return new SelectorConstant(compareWithInt(left.getIntValue(), right.getIntValue()));
                    case DOUBLE:
                        return new SelectorConstant(compareWithInt(left.getIntValue(), right.getDoubleValue()));
                    default:
                        throw new SelectorEvaluationException(getClass() + " Unexpected type : " +
                            right.type + ". left : " + left + ", right : " + right);
                }
            }
            case DOUBLE: {
                switch (right.type) {
                    case INT:
                        return new SelectorConstant(compareWithDouble(left.getDoubleValue(), right.getIntValue()));
                    case DOUBLE:
                        return new SelectorConstant(compareWithDouble(left.getDoubleValue(), right.getDoubleValue()));
                    default:
                        throw new SelectorEvaluationException(getClass() + " Unexpected type : " +
                            right.type + ". left : " + left + ", right : " + right);
                }
            }
            case BOOLEAN: {
                switch (right.type) {
                    case BOOLEAN:
                        return new SelectorConstant(compareWithBoolean(left.getBoolValue(), right.getBoolValue()));
                    default:
                        throw new SelectorEvaluationException(getClass() + " Unexpected type : " +
                            right.type + ". left : " + left + ", right : " + right);
                }
            }

            case STRING: {
                switch (right.type) {
                    case STRING:
                        return new SelectorConstant(compareWithString(left.getStringValue(), right.getStringValue()));
                    default:
                        throw new SelectorEvaluationException(getClass() + " Unexpected type : " +
                            right.type + ". left : " + left + ", right : " + right);
                }
            }

            default:
                throw new SelectorEvaluationException(getClass() + " Unsupported type : " +
                    left.type + ". left : " + left + ", right : " + right);
        }
    }

    protected abstract Boolean compareWithInt(Integer left, Double right) throws SelectorEvaluationException;

    protected abstract Boolean compareWithInt(Integer left, Integer right) throws SelectorEvaluationException;

    protected abstract Boolean compareWithDouble(Double left, Double right) throws SelectorEvaluationException;

    protected abstract Boolean compareWithDouble(Double left, Integer right) throws SelectorEvaluationException;

    protected abstract Boolean compareWithString(String left, String right) throws SelectorEvaluationException;

    protected abstract Boolean compareWithBoolean(Boolean left, Boolean right) throws SelectorEvaluationException;


    public static final ValueComparisonFunction GREATER_THAN_FUNCTION = new ValueComparisonFunction() {
        @Override
        protected Boolean compareWithInt(Integer left, Double right) {
            if (null == left || null == right) return null;
            return (double) left > right;
        }

        @Override
        protected Boolean compareWithInt(Integer left, Integer right) {
            if (null == left || null == right) return null;
            return left > right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left > right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Integer right) {
            if (null == left || null == right) return null;
            return left > (double) right;
        }

        @Override
        protected Boolean compareWithString(String left, String right) throws SelectorEvaluationException {
            throw new SelectorEvaluationException(getClass()
                    + " Unsupported string comparison for greater_than operator");
        }

        @Override
        protected Boolean compareWithBoolean(Boolean left, Boolean right) throws SelectorEvaluationException {
            throw new SelectorEvaluationException(getClass()
                    + " Unsupported boolean comparison for greater_than operator");
        }
    };


    public static final ValueComparisonFunction GREATER_THAN_EQUAL_TO_FUNCTION = new ValueComparisonFunction() {
        @Override
        protected Boolean compareWithInt(Integer left, Double right) {
            if (null == left || null == right) return null;
            return (double) left >= right;
        }

        @Override
        protected Boolean compareWithInt(Integer left, Integer right) {
            if (null == left || null == right) return null;
            return left >= right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left >= right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Integer right) {
            if (null == left || null == right) return null;
            return left >= (double) right;
        }

        @Override
        protected Boolean compareWithString(String left, String right) throws SelectorEvaluationException {
            throw new SelectorEvaluationException(getClass() +
                " Unsupported string comparison for greater_than operator");
        }

        @Override
        protected Boolean compareWithBoolean(Boolean left, Boolean right) throws SelectorEvaluationException {
            throw new SelectorEvaluationException(getClass() +
                " Unsupported boolean comparison for greater_than operator");
        }
    };

    public static final ValueComparisonFunction LESS_THAN_FUNCTION = new ValueComparisonFunction() {
        @Override
        protected Boolean compareWithInt(Integer left, Double right) {
            if (null == left || null == right) return null;
            return (double) left < right;
        }

        @Override
        protected Boolean compareWithInt(Integer left, Integer right) {
            if (null == left || null == right) return null;
            return left < right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left < right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Integer right) {
            if (null == left || null == right) return null;
            return left < (double) right;
        }

        @Override
        protected Boolean compareWithString(String left, String right) throws SelectorEvaluationException {
            throw new SelectorEvaluationException(getClass() +
                " Unsupported string comparison for greater_than operator");
        }

        @Override
        protected Boolean compareWithBoolean(Boolean left, Boolean right) throws SelectorEvaluationException {
            throw new SelectorEvaluationException(getClass() +
                " Unsupported boolean comparison for greater_than operator");
        }
    };


    public static final ValueComparisonFunction LESS_THAN_EQUAL_TO_FUNCTION = new ValueComparisonFunction() {
        @Override
        protected Boolean compareWithInt(Integer left, Double right) {
            if (null == left || null == right) return null;
            return (double) left <= right;
        }

        @Override
        protected Boolean compareWithInt(Integer left, Integer right) {
            if (null == left || null == right) return null;
            return left <= right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left <= right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Integer right) {
            if (null == left || null == right) return null;
            return left <= (double) right;
        }

        @Override
        protected Boolean compareWithString(String left, String right) throws SelectorEvaluationException {
            throw new SelectorEvaluationException(getClass() +
                " Unsupported string comparison for greater_than operator");
        }

        @Override
        protected Boolean compareWithBoolean(Boolean left, Boolean right) throws SelectorEvaluationException {
            throw new SelectorEvaluationException(getClass() +
                " Unsupported boolean comparison for greater_than operator");
        }
    };

    public static final ValueComparisonFunction EQUAL_TO_FUNCTION = new ValueComparisonFunction() {
        @Override
        protected Boolean compareWithInt(Integer left, Double right) {
            if (null == left || null == right) return null;
            return (double) left == right;
        }

        @Override
        protected Boolean compareWithInt(Integer left, Integer right) {
            if (null == left || null == right) return null;
            return (int) left == right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return (double) left == right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Integer right) {
            if (null == left || null == right) return null;
            return left == (double) right;
        }

        @Override
        protected Boolean compareWithString(String left, String right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left.equals(right);
        }

        @Override
        protected Boolean compareWithBoolean(Boolean left, Boolean right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left.equals(right);
        }
    };


    public static final ValueComparisonFunction NOT_EQUAL_TO_FUNCTION = new ValueComparisonFunction() {
        @Override
        protected Boolean compareWithInt(Integer left, Double right) {
            if (null == left || null == right) return null;
            return (double) left != right;
        }

        @Override
        protected Boolean compareWithInt(Integer left, Integer right) {
            if (null == left || null == right) return null;
            return (int) left != right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return (double) left != right;
        }

        @Override
        protected Boolean compareWithDouble(Double left, Integer right) {
            if (null == left || null == right) return null;
            return left != (double) right;
        }

        @Override
        protected Boolean compareWithString(String left, String right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return !left.equals(right);
        }

        @Override
        protected Boolean compareWithBoolean(Boolean left, Boolean right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return !left.equals(right);
        }
    };



    @Override
    public String toString(){
        return getClass().getName();
    }
}
