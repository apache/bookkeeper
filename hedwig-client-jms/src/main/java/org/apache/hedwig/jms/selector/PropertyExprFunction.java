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

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Handles property (and header) dereference against message evaluation (based on identifier specified).
 */
public abstract class PropertyExprFunction implements ExprFunction {

    public static class LookupExpr extends PropertyExprFunction {
        private final String identifier;

        public LookupExpr(String identifier) {
            this.identifier = identifier;
        }

        @Override
        public void evaluate(SelectorEvalState state) throws SelectorEvaluationException {
            // No (stack) params required ...

            SelectorConstant result = doEvaluate(state.getMessage());
            if (MyNode.logger.isTraceEnabled()) MyNode.logger.trace(getClass() + ": identifier '" +
                identifier + "' -> " + result);
            state.getStack().push(result);
            return;
        }

        private SelectorConstant doEvaluate(final MessageImpl message) throws SelectorEvaluationException {


            if (!message.propertyExists(identifier)) {
                // defaulting to String, does it matter ?
                return new SelectorConstant((String) null);
            }

            final Object val = message.getSelectorProcessingPropertyValue(identifier);

            if (val instanceof Byte) {
                return new SelectorConstant((int) (Byte) val);
            }
            if (val instanceof Short) {
                return new SelectorConstant((int) (Short) val);
            }
            if (val instanceof Integer) {
                return new SelectorConstant((Integer) val);
            }
            if (val instanceof Long) {
                long lval = (Long) val;
                if (lval >= (long) Integer.MAX_VALUE || lval <= (long) Integer.MIN_VALUE)
                    throw new SelectorEvaluationException(getClass() + " long value " + lval +
                        " out of range for an int");

                return new SelectorConstant((int) lval);
            }
            if (val instanceof Boolean) {
                return new SelectorConstant((Boolean) val);
            }
            if (val instanceof Float) {
                return new SelectorConstant((double) (Float) val);
            }
            if (val instanceof Double) {
                return new SelectorConstant((Double) val);
            }
            if (val instanceof String) {
                return new SelectorConstant((String) val);
            }


            throw new SelectorEvaluationException(getClass() + " Unable to interpret value '" + val +
                "' for identifier " + identifier);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("LookupExpr");
            sb.append("{identifier='").append(identifier).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    public static class IsNullExpr extends PropertyExprFunction {

        private final boolean negate;

        public IsNullExpr(boolean negate) {
            this.negate = negate;
        }

        @Override
        public void evaluate(SelectorEvalState state) throws SelectorEvaluationException {

            final SelectorConstant result = doEvaluate(state);
            if (MyNode.logger.isTraceEnabled()) MyNode.logger.trace(getClass() + " -> " + result);
            state.getStack().push(result);
        }

        private SelectorConstant doEvaluate(SelectorEvalState state) throws SelectorEvaluationException {

            if (state.getStack().isEmpty())
                throw new SelectorEvaluationException(getClass() + " stack corruption ? " + state.getStack());

            final SelectorConstant value = state.getStack().pop();

            boolean result = value.isNull();
            if (negate) result = !result;
            return new SelectorConstant(result);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("IsNullExpr");
            sb.append("{negate=").append(negate);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class InExpr extends PropertyExprFunction {

        private final boolean negate;

        public InExpr(boolean negate) {
            this.negate = negate;
        }

        @Override
        public void evaluate(SelectorEvalState state) throws SelectorEvaluationException {
            if (state.getStack().size() < 2)
                throw new SelectorEvaluationException(getClass() + " stack corruption ? " + state.getStack());

            final SelectorConstant paramSet = state.getStack().pop();
            final SelectorConstant checkFor = state.getStack().pop();

            final SelectorConstant result = doEvaluate(paramSet, checkFor);
            if (MyNode.logger.isTraceEnabled()) MyNode.logger.trace(getClass() + ": checkFor '" +
                checkFor + "', paramSet '" + paramSet+ "' -> " + result);

            state.getStack().push(result);
            return ;

        }

        private SelectorConstant doEvaluate(SelectorConstant paramSet, SelectorConstant checkFor)
            throws SelectorEvaluationException {

            if (checkFor.isNull()){
                return new SelectorConstant((String) null);
            }

            if (SelectorConstant.SelectorDataType.STRING_SET != paramSet.type) {
                throw new SelectorEvaluationException(getClass() + " Expected string list, found : " +
                    paramSet.type + ", for " + paramSet);
            }
            if (SelectorConstant.SelectorDataType.STRING != checkFor.type){
                throw new SelectorEvaluationException(getClass() + " Expected string , found : " +
                    checkFor.type + ", for " + checkFor);
            }

            boolean result = paramSet.getStringSet().contains(checkFor.getStringValue());
            if (negate) result = !result;
            return new SelectorConstant(result);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("InExpr");
            sb.append("{negate=").append(negate);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class LikeExpr extends PropertyExprFunction {

        private final Pattern likePattern;
        private final String likePatternStr;
        private final boolean negate;

        public LikeExpr(String likeExpression, String escapeCharacter, boolean negate) throws ParseException {
            if (null != escapeCharacter && 1 != escapeCharacter.length()) {
                throw new ParseException(getClass() + " Escape character must be a single character : '" +
                    escapeCharacter + "'");
            }
            this.likePatternStr = generateRegexpPattern(likeExpression, escapeCharacter);
            try {
                this.likePattern = Pattern.compile(this.likePatternStr, Pattern.DOTALL);
            } catch (PatternSyntaxException psEx){
                throw new ParseException(LikeExpr.class + " Unable to compile '" + likeExpression +
                    "' into regexp Pattern using '" + this.likePatternStr+ "'");
            }
            this.negate = negate;
        }

        private static String generateRegexpWithoutWildcard(final String expression){
            int indxOffset = 0;
            int substringOffset = 0;
            StringBuilder sb = new StringBuilder();
            while (indxOffset < expression.length()){
                final int indxUnder = expression.indexOf('_', indxOffset);
                final int indxMod = expression.indexOf('%', indxOffset);
                if (-1 == indxUnder && -1 == indxMod) break;

                final int indx;

                if (-1 != indxUnder && -1 != indxMod) indx = Math.min(indxUnder, indxMod);
                else if (-1 != indxUnder) indx = indxUnder;
                else indx = indxMod;

                if (indx != substringOffset) {
                    sb.append(Pattern.quote(expression.substring(substringOffset, indx)));
                }
                sb.append(indx == indxUnder ? "." : ".*");
                substringOffset = indx + 1;
                indxOffset = indx + 1;
            }
            if (expression.length() != substringOffset) {
                sb.append(Pattern.quote(expression.substring(substringOffset)));
            }

            return sb.toString();
        }

        // If wildcard if prefixed with escapeChar, ignore it.
        private static String generateRegexpWithWildcard(final String expression, char escapeChar){
            int indxOffset = 0;
            int substringOffset = 0;
            StringBuilder sb = new StringBuilder();
            while (indxOffset < expression.length()){
                final int indxUnder = expression.indexOf('_', indxOffset);
                final int indxMod = expression.indexOf('%', indxOffset);
                if (-1 == indxUnder && -1 == indxMod) break;

                final int indx;

                if (-1 != indxUnder && -1 != indxMod) indx = Math.min(indxUnder, indxMod);
                else if (-1 != indxUnder) indx = indxUnder;
                else indx = indxMod;

                if (indx > 0 && escapeChar == expression.charAt(indx - 1)) {
                    // ignore it.
                    indxOffset = indx + 1;
                    continue;
                }

                if (indx != substringOffset) {
                    sb.append(Pattern.quote(expression.substring(substringOffset, indx)));
                }
                sb.append(indx == indxUnder ? "." : ".*");
                substringOffset = indx + 1;
                indxOffset = indx + 1;
            }
            if (expression.length() != substringOffset) {
                sb.append(Pattern.quote(expression.substring(substringOffset)));
            }

            return sb.toString();
        }

        private static String generateRegexpPattern(final String likeExpression,
                                                    final String escapeCharacterStr) throws ParseException {

            if (null == escapeCharacterStr){
                // Ok, hand-generating the pattern seems to be the only generic way to handle this, sigh :-(

                String rpat = generateRegexpWithoutWildcard(likeExpression);
                return rpat;
            }

            // expect this to be there ...
            final char escapeChar = escapeCharacterStr.charAt(0);

            // Test when escapeChar == ']', '[' and '^'. done !
            String rpat = generateRegexpWithWildcard(likeExpression, escapeChar);

            rpat = rpat.replace(escapeChar + "%", "%");
            rpat = rpat.replace(escapeChar + "_", "_");

            return rpat;
        }

        @Override
        public void evaluate(SelectorEvalState state) throws SelectorEvaluationException {

            if (state.getStack().isEmpty()) throw new SelectorEvaluationException(getClass() +
                " stack corruption ? " + state.getStack());

            final SelectorConstant checkFor = state.getStack().pop();

            final SelectorConstant result = doEvaluate(checkFor);
            if (MyNode.logger.isTraceEnabled()) MyNode.logger.trace(getClass() + ": checkFor '" +
                checkFor + "' -> " + result);

            state.getStack().push(result);
            return ;
        }

        private SelectorConstant doEvaluate(SelectorConstant checkFor) throws SelectorEvaluationException {
            if (checkFor.isNull()){
                return new SelectorConstant((String) null);
            }

            if (SelectorConstant.SelectorDataType.STRING != checkFor.type){
                throw new SelectorEvaluationException(getClass() + " Expected string , found : " +
                    checkFor.type + ", for " + checkFor);
            }


            final String value = checkFor.getStringValue();

            if (null == value) {
                return new SelectorConstant((Boolean) null);
            }

            boolean result = likePattern.matcher(value).matches();
            if (negate) result = !result;
            return new SelectorConstant(result);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("LikeExpr");
            sb.append("{likePatternStr=").append(likePatternStr);
            sb.append(", negate=").append(negate);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class BetweenExpr extends PropertyExprFunction {

        private final boolean negate;

        public BetweenExpr(boolean negate) {
            this.negate = negate;
        }

        @Override
        public void evaluate(SelectorEvalState state) throws SelectorEvaluationException {
            if (state.getStack().size() < 3){
                throw new SelectorEvaluationException(getClass() + " stack corruption ? " + state.getStack());
            }

            final SelectorConstant right = state.getStack().pop();
            final SelectorConstant left = state.getStack().pop();

            final SelectorConstant checkFor = state.getStack().pop();

            final SelectorConstant result = doEvaluate(checkFor, left, right);
            if (MyNode.logger.isTraceEnabled()) MyNode.logger.trace(getClass() + ": left '" + left +
                "', right '" + right + "', checkFor '" + checkFor + "' -> " + result);
            state.getStack().push(result);
        }

        private SelectorConstant doEvaluate(final SelectorConstant checkFor, final SelectorConstant left,
                                            final SelectorConstant right) throws SelectorEvaluationException {

            if (left.isNull() || right.isNull()) {
                // Unexpected for a bound to be null ...
                throw new SelectorEvaluationException(getClass() + " Unexpected for left or right bound to be null " +
                    left + ", " + right);
            }

            if (checkFor.isNull()){
                // If checkFor is null, then it cant be between anyway - return unknown.
                return new SelectorConstant((Boolean) null);
            }

            final Boolean result;

            // Between left and right ...
            switch (left.type) {
                case INT: {
                    switch (right.type) {
                        case INT: {
                            result = handleBetweenIntAndInt(checkFor, left.getIntValue(),
                                right.getIntValue());
                            break;
                        }
                        case DOUBLE: {
                            result = handleBetweenIntAndDouble(checkFor, left.getIntValue(),
                                right.getDoubleValue());
                            break;
                        }
                        default:
                            throw new SelectorEvaluationException(getClass() + " Unsupported type for right " +
                                right.type);
                    }
                    break;
                }
                case DOUBLE: {
                    switch (right.type) {
                        case INT: {
                            result = handleBetweenIntAndDouble(checkFor, right.getIntValue(),
                                left.getDoubleValue());
                            break;
                        }
                        case DOUBLE: {
                            result = handleBetweenDoubleAndDouble(checkFor, left.getDoubleValue(),
                                right.getDoubleValue());
                            break;
                        }
                        default:
                            throw new SelectorEvaluationException(getClass() +
                                " Unsupported type for right " + right.type);
                    }
                    break;
                }
                default:
                    throw new SelectorEvaluationException(getClass() + " Unsupported type for left " + right.type);
            }

            if (null == result) {
                // Cannot find the result as the type we expected.
                return new SelectorConstant((Boolean) null);
            }


            return new SelectorConstant(negate ? !result : result);
        }

        private Boolean handleBetweenIntAndInt(SelectorConstant checkFor, int intBound,
                                               int otherIntBound) throws SelectorEvaluationException {
            final int low = Math.min(intBound, otherIntBound);
            final int high = Math.max(intBound, otherIntBound);

            assert ! checkFor.isNull();


            switch (checkFor.type){
                case INT:
                    return checkFor.getIntValue() >= low && checkFor.getIntValue() <= high;
                case DOUBLE:
                    return checkFor.getDoubleValue() >= low && checkFor.getDoubleValue() <= high;
                default:
                    throw new SelectorEvaluationException(getClass() +
                        " Identifier value is of illegal type " + checkFor.type + " ... " + checkFor);
            }
        }

        private Boolean handleBetweenIntAndDouble(SelectorConstant checkFor, int intBound,
                                                  double doubleBound) throws SelectorEvaluationException {
            final double low = Math.min((double) intBound, doubleBound);
            final double high = Math.max((double) intBound, doubleBound);

            assert ! checkFor.isNull();


            switch (checkFor.type){
                case INT:
                    return checkFor.getIntValue() >= low && checkFor.getIntValue() <= high;
                case DOUBLE:
                    return checkFor.getDoubleValue() >= low && checkFor.getDoubleValue() <= high;
                default:
                    throw new SelectorEvaluationException(getClass() +
                        " Identifier value is of illegal type " + checkFor.type + " ... " + checkFor);
            }
        }

        private Boolean handleBetweenDoubleAndDouble(SelectorConstant checkFor, double doubleBound,
                                                     double otherDoubleBound) throws SelectorEvaluationException {
            final double low = Math.min(doubleBound, otherDoubleBound);
            final double high = Math.max(doubleBound, otherDoubleBound);

            assert ! checkFor.isNull();


            switch (checkFor.type){
                case INT:
                    return checkFor.getIntValue() >= low && checkFor.getIntValue() <= high;
                case DOUBLE:
                    return checkFor.getDoubleValue() >= low && checkFor.getDoubleValue() <= high;
                default:
                    throw new SelectorEvaluationException(getClass() +
                        " Identifier value is of illegal type " + checkFor.type + " ... " + checkFor);
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("BetweenExpr");
            sb.append("{negate=").append(negate);
            sb.append('}');
            return sb.toString();
        }
    }
}