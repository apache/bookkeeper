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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Root of all nodes in the AST generated.
 * Encapsulates state for evaluation of the ast by an interpreter.
 */
public class MyNode {

    final static Logger logger = LoggerFactory.getLogger(MyNode.class);

    // This is se for case of constant value literals.
    private SelectorConstant selectorConstant;

    // This is the actual expression to evaluate.
    private ExprFunction exprFunction;

    // Called while interpreting ..
    public SelectorConstant getConstantValue() throws SelectorEvaluationException {
        if (null == selectorConstant)
            throw new SelectorEvaluationException(getClass() +
                " Unexpected not to have evalData populated for " + this);
        return selectorConstant;
    }

    public void addToStringSet(String str) throws ParseException {
        if (null == selectorConstant) throw new ParseException(getClass() +
            " Unexpected not to have evalData populated for " + this);
        selectorConstant.addToStringSet(str);
    }

    // Called while parsing ..
    public void setConstantValue(SelectorConstant selectorData) throws ParseException {
        if (null != this.selectorConstant)
            throw new ParseException(getClass() + " Value already set ? prev : " +
                this.selectorConstant + ", new : " + selectorData);

        if (MyNode.logger.isTraceEnabled()) MyNode.logger.trace("Setting constant value " +
            selectorData + " for " + this);

        this.selectorConstant = selectorData;
    }


    // Called while interpreting ..
    public ExprFunction getExprFunction() throws SelectorEvaluationException {
        if (null == exprFunction)
            throw new SelectorEvaluationException(getClass() +
                " Unexpected not to have exprFunction populated for " + this);
        return exprFunction;
    }

    // Called while parsing ..
    public void setExprFunction(ExprFunction exprFunction) throws ParseException {
        if (null != this.exprFunction)
            throw new ParseException(getClass() + " exprFunction already set ? prev : " +
                this.exprFunction + ", new : " + exprFunction);

        if (MyNode.logger.isTraceEnabled()) MyNode.logger.trace("Setting function expr " +
            exprFunction + " for " + this);

        this.exprFunction = exprFunction;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MyNode");
        sb.append("{selectorConstant=").append(selectorConstant);
        sb.append(", exprFunction=").append(exprFunction);
        sb.append('}');
        return sb.toString();
    }




    // Internal to the parser - DO NOT use outside !
    SelectorConstant getConstantValueInternal() { return selectorConstant; }
    ExprFunction getExprFunctionInternal() { return exprFunction; }
}
