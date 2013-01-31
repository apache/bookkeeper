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

/**
 * Visits the AST to evaluate the message (in the SelectorEvalState) against this ast parse tree to see if the
 * message evaluates to true or false (whether to allow message to be submitted or not to consumers).
 */
public class InterpretSelectorParserVisitor implements SelectorParserVisitor {
    @Override
    public Object visit(SimpleNode node, SelectorEvalState data) throws SelectorEvaluationException {
        throw new SelectorEvaluationException(getClass() + " Should not result in this method being called !");
    }

    @Override
    public Object visit(ASTOrExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTAndExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTNotExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTGreaterThan node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTLessThan node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTLessThanEqualTo node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTGreaterThanEqualTo node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTEqualTo node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTNotEqualTo node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTIsNullExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTBetweenExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTInExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTLikeExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTLookupExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTAddExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTSubExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTDivideExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTMultiplyExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTNegateExpr node, SelectorEvalState data) throws SelectorEvaluationException {
        node.childrenAccept(this, data);
        node.getExprFunction().evaluate(data);
        return null;
    }

    @Override
    public Object visit(ASTConstant node, SelectorEvalState data) throws SelectorEvaluationException {
        // node.childrenAccept(this, data);
        if (0 != node.jjtGetNumChildren()) throw new SelectorEvaluationException(getClass() +
            " parse error ? " + node);
        data.getStack().push(node.getConstantValue());
        return null;
    }

    @Override
    public Object visit(ASTStringVarargParams node, SelectorEvalState data) throws SelectorEvaluationException {
        // node.childrenAccept(this, data);
        if (0 != node.jjtGetNumChildren()) throw new SelectorEvaluationException(getClass() +
            " parse error ? " + node);
        data.getStack().push(node.getConstantValue());
        return null;
    }
}
