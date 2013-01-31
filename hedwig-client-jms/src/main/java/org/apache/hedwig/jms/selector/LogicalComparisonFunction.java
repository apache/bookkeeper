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
 * Logical comparison between two booleans.
 */
public abstract class LogicalComparisonFunction extends BinaryExprFunction {
    public static final LogicalComparisonFunction AND_FUNCTION = new LogicalComparisonFunction() {
        @Override
        protected Boolean doComparison(Boolean left, Boolean right) {
            if (null == left || null == right) {
                return (Boolean.FALSE.equals(left) || Boolean.FALSE.equals(right)) ? false : null;
            }
            return left && right;
        }
    };

    public static final LogicalComparisonFunction OR_FUNCTION = new LogicalComparisonFunction() {
        @Override
        protected Boolean doComparison(Boolean left, Boolean right) {
            if (null == left || null == right) {
                return (Boolean.TRUE.equals(left) || Boolean.TRUE.equals(right)) ? true : null;
            }

            return left || right;
        }
    };


    protected abstract Boolean doComparison(Boolean left, Boolean right);

    protected SelectorConstant evaluateImpl(SelectorConstant left, SelectorConstant right,
                                            MessageImpl message) throws SelectorEvaluationException {
        if (SelectorConstant.SelectorDataType.BOOLEAN != left.type ||
            SelectorConstant.SelectorDataType.BOOLEAN != right.type) {
            throw new SelectorEvaluationException(getClass() + " Invalid value type ? " + left + ", " + right);
        }

        return new SelectorConstant(doComparison(left.getBoolValue(), right.getBoolValue()));
    }

    @Override
    public String toString(){
        return getClass().getName();
    }
}
