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
 * Evaluate binary expressions - where there is a left and right expression to be evaluated.
 */
public abstract class BinaryExprFunction implements ExprFunction {

    public void evaluate(SelectorEvalState state) throws SelectorEvaluationException {
        if (state.getStack().size() < 2)
            throw new SelectorEvaluationException(getClass() + " stack corruption ? " + state.getStack());

        SelectorConstant right = state.getStack().pop();
        SelectorConstant left = state.getStack().pop();

        SelectorConstant result = evaluateImpl(left, right, state.getMessage());

        if (MyNode.logger.isTraceEnabled()) MyNode.logger.trace(getClass() + ": left " + left +
            ", right " + right + " -> " + result);

        if (null != result) state.getStack().push(result);
        else throw new SelectorEvaluationException(getClass() +
            " Unexpected to return a null response in binary function evaluation");
    }

    protected abstract SelectorConstant evaluateImpl(SelectorConstant left, SelectorConstant right,
                                                     MessageImpl message) throws SelectorEvaluationException;

    @Override
    public String toString(){
        return getClass().getName();
    }
}
