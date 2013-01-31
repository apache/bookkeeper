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
 * Binary arithematic of values ..
 */
public abstract class BinaryArithmeticFunction extends BinaryExprFunction {

    protected SelectorConstant evaluateImpl(SelectorConstant left, SelectorConstant right,
                                            MessageImpl message) throws SelectorEvaluationException {

        switch (left.type) {
            case INT: {
                switch (right.type) {
                    case INT:
                        return new SelectorConstant(computeWithInt(left.getIntValue(), right.getIntValue()));
                    case DOUBLE:
                        return new SelectorConstant(computeWithInt(left.getIntValue(), right.getDoubleValue()));
                    default:
                        throw new SelectorEvaluationException(getClass() + " Unexpected type : " +
                            right.type + ". left : " + left + ", right : " + right);
                }
            }
            case DOUBLE: {
                switch (right.type) {
                    case INT:
                        return new SelectorConstant(computeWithDouble(left.getDoubleValue(), right.getIntValue()));
                    case DOUBLE:
                        return new SelectorConstant(computeWithDouble(left.getDoubleValue(), right.getDoubleValue()));
                    default:
                        throw new SelectorEvaluationException(getClass() + " Unexpected type : " +
                            right.type + ". left : " + left + ", right : " + right);
                }
            }
            case BOOLEAN:
            case STRING:
            default:
                throw new SelectorEvaluationException(getClass() + " Unsupported type : " + left.type +
                    ". left : " + left + ", right : " + right);
        }
    }


    protected abstract Double computeWithInt(Integer left, Double right) throws SelectorEvaluationException;

    protected abstract Integer computeWithInt(Integer left, Integer right) throws SelectorEvaluationException;

    protected abstract Double computeWithDouble(Double left, Double right) throws SelectorEvaluationException;

    protected abstract Double computeWithDouble(Double left, Integer right) throws SelectorEvaluationException;


    public static final BinaryArithmeticFunction ADD_FUNCTION = new BinaryArithmeticFunction() {
        @Override
        protected Double computeWithInt(Integer left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return (double) left + right;
        }

        @Override
        protected Integer computeWithInt(Integer left, Integer right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left + right;
        }

        @Override
        protected Double computeWithDouble(Double left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left + right;
        }

        @Override
        protected Double computeWithDouble(Double left, Integer right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left + (double) right;
        }
    };

    public static final BinaryArithmeticFunction SUB_FUNCTION = new BinaryArithmeticFunction() {
        @Override
        protected Double computeWithInt(Integer left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return (double) left - right;
        }

        @Override
        protected Integer computeWithInt(Integer left, Integer right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left - right;
        }

        @Override
        protected Double computeWithDouble(Double left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left - right;
        }

        @Override
        protected Double computeWithDouble(Double left, Integer right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left - (double) right;
        }
    };

    public static final BinaryArithmeticFunction MULTIPLY_FUNCTION = new BinaryArithmeticFunction() {
        @Override
        protected Double computeWithInt(Integer left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return (double) left * right;
        }

        @Override
        protected Integer computeWithInt(Integer left, Integer right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left * right;
        }

        @Override
        protected Double computeWithDouble(Double left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left * right;
        }

        @Override
        protected Double computeWithDouble(Double left, Integer right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            return left * (double) right;
        }
    };

    public static final BinaryArithmeticFunction DIVIDE_FUNCTION = new BinaryArithmeticFunction() {
        @Override
        protected Double computeWithInt(Integer left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            if ((double) 0 == right) throw new SelectorEvaluationException(getClass() + " denominator == 0");
            return (double) left / right;
        }

        @Override
        protected Integer computeWithInt(Integer left, Integer right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            if ((int) 0 == right) throw new SelectorEvaluationException(getClass() + " denominator == 0");
            return left / right;
        }

        @Override
        protected Double computeWithDouble(Double left, Double right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            if ((double) 0 == right) throw new SelectorEvaluationException(getClass() + " denominator == 0");
            return left / right;
        }

        @Override
        protected Double computeWithDouble(Double left, Integer right) throws SelectorEvaluationException {
            if (null == left || null == right) return null;
            if (0 == right) throw new SelectorEvaluationException(getClass() + " denominator == 0");
            return left / (double) right;
        }
    };


    @Override
    public String toString(){
        return getClass().getName();
    }
}
