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

import junit.framework.Assert;
import org.apache.hedwig.jms.message.MessageImpl;
import org.apache.hedwig.jms.message.TextMessageImpl;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSException;

/**
 * Test basic selector grammar.
 */
public class BasicSelectorGrammarTest {

    private static final String TEST_MESSAGE = "test_message";

    private static final String BOOLEAN_HEADER1 = "boolean_header1";
    private static final boolean BOOLEAN_VALUE1 = true;


    private static final String INT_HEADER1 = "int_header1";
    private static final int INT_VALUE1 = 1;

    private static final String INT_HEADER2 = "int_header2";
    private static final int INT_VALUE2 = 2;

    private static final String INT_HEADER3 = "int_header3";
    private static final int INT_VALUE3 = 3;


    private static final String DOUBLE_HEADER1 = "double_header1";
    private static final double DOUBLE_VALUE1 = 1;

    private static final String DOUBLE_HEADER2 = "double_header2";
    private static final double DOUBLE_VALUE2 = 2;


    private static final String STRING_HEADER1 = "string_header1";
    private static final String STRING_VALUE1 = "header_value1";

    private static final String STRING_HEADER2 = "string_header2";
    private static final String STRING_VALUE2 = "header_value2";

    private static final String STRING_HEADER3 = "string_header3";
    private static final String STRING_VALUE3 = "header_value3";

    private static final String STRING_HEADER4 = "string_header4";
    private static final String STRING_VALUE4 = "header_value4";

    // Contains both characters used to do regexp in LIKE
    private static final String STRING_LIKE_HEADER = "string_like_header";
    private static final String STRING_LIKE_VALUE = "value with a _ and % in it and a \n with \t also for testing.";

    private static final String STRING_QUOTES_HEADER = "string_quotes_header";
    private static final String STRING_QUOTES_VALUE = "quotes's value";
    private static final String STRING_QUOTED_QUOTES_VALUE = "quotes''s value";


    private MessageImpl message;

    @Before
    public void createMessage() {
        try {
            // Directly creating instead of using session ... this is just to test !
            TextMessageImpl message = new TextMessageImpl(null, TEST_MESSAGE);

            message.setBooleanProperty(BOOLEAN_HEADER1, BOOLEAN_VALUE1);

            message.setIntProperty(INT_HEADER1, INT_VALUE1);
            message.setIntProperty(INT_HEADER2, INT_VALUE2);
            message.setIntProperty(INT_HEADER3, INT_VALUE3);

            message.setDoubleProperty(DOUBLE_HEADER1, DOUBLE_VALUE1);
            message.setDoubleProperty(DOUBLE_HEADER2, DOUBLE_VALUE2);

            message.setStringProperty(STRING_HEADER1, STRING_VALUE1);
            message.setStringProperty(STRING_HEADER2, STRING_VALUE2);
            message.setStringProperty(STRING_HEADER3, STRING_VALUE3);
            message.setStringProperty(STRING_HEADER4, STRING_VALUE4);
            message.setStringProperty(STRING_LIKE_HEADER, STRING_LIKE_VALUE);

            message.setStringProperty(STRING_QUOTES_HEADER, STRING_QUOTES_VALUE);

            this.message = message;
        } catch (JMSException e) {
            throw new IllegalStateException("Unexpected ... ", e);
        }
    }


    @Test
    public void testBasicLookup() throws ParseException {
        // simple check's for int header.
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(BOOLEAN_HEADER1 + " = " + BOOLEAN_VALUE1),
                        message)
        );
        Assert.assertEquals(Boolean.FALSE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(BOOLEAN_HEADER1 + " <> " + BOOLEAN_VALUE1),
                        message)
        );

        // simple check's for int header.
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(INT_HEADER1 + " = " + INT_VALUE1),
                        message)
        );
        Assert.assertEquals(Boolean.FALSE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(INT_HEADER1 + " <> " + INT_VALUE1),
                        message)
        );

        // simple check's for double header.
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(DOUBLE_HEADER1 + " = " + DOUBLE_VALUE1),
                        message)
        );
        Assert.assertEquals(Boolean.FALSE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(DOUBLE_HEADER1 + " <> " + DOUBLE_VALUE1),
                        message)
        );

        // simple check's for String header.
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(STRING_HEADER1 + " = '" + STRING_VALUE1 + "'"),
                        message)
        );
        Assert.assertEquals(Boolean.FALSE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(STRING_HEADER1 + " <> '" + STRING_VALUE1 + "'"),
                        message)
        );

        // check for String header with quote ...
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(STRING_QUOTES_HEADER + " = '"
                                                            + STRING_QUOTED_QUOTES_VALUE + "'"),
                        message)
        );
        Assert.assertEquals(Boolean.FALSE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(STRING_QUOTES_HEADER
                                                            + " <> '" + STRING_QUOTED_QUOTES_VALUE + "'"),
                        message)
        );


        // incompatible header.
        Assert.assertNull(
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(STRING_QUOTES_HEADER + " = " + INT_VALUE1),
                        message)
        );
        Assert.assertNull(
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(BOOLEAN_HEADER1 + " = " + DOUBLE_VALUE1),
                        message)
        );
        Assert.assertNull(
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector("unknown_header = " + STRING_VALUE1),
                        message)
        );
    }


    @Test
    public void testArithmetic() throws ParseException {
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(INT_HEADER1 + " + " + INT_HEADER2 + " > " + INT_VALUE1),
                        message)
        );
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(INT_HEADER1 + " + ( 2 * " + INT_HEADER2
                                                            + " + " + INT_HEADER1 + " ) < " +
                                " 4 * ( " + INT_HEADER1 + " + " + INT_VALUE2 + " ) "),
                        message)
        );

        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(DOUBLE_HEADER1 + " + "
                                                            + DOUBLE_HEADER2 + " > " + DOUBLE_VALUE1),
                        message)
        );
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(DOUBLE_HEADER1 + " * 7.5 + 1 + 2 * ( "
                                                            + DOUBLE_HEADER2 + " + 2.0 * " + DOUBLE_HEADER1 + " ) = " +
                                " 0.5 + 4 * ( 2.0 * " + DOUBLE_HEADER1 + " + " + DOUBLE_VALUE2 + " ) "),
                        message)
        );

        // Incompatible header in computation - string used in arithmetic.
        Assert.assertNull(
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(STRING_HEADER1 + " * 4 + " + DOUBLE_HEADER1
                                                            + " * 7.5 + 1 + 2 * ( " + DOUBLE_HEADER2
                                                            + "+ 2.0 * " + DOUBLE_HEADER1 + " ) = " +
                                " 0.5 + 4 * ( 2.0 * " + DOUBLE_HEADER1 + " + " + DOUBLE_VALUE2 + " ) "),
                        message)
        );

        // Unknown header in computation.
        Assert.assertNull(
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(" unknown_header * 4 + "
                                                            + DOUBLE_HEADER1 + " * 7.5 + 1 + 2 * ( "
                                                            + DOUBLE_HEADER2 + " + 2.0 * " + DOUBLE_HEADER1 + " ) = " +
                                " 0.5 + 4 * ( 2.0 * " + DOUBLE_HEADER1 + " + " + DOUBLE_VALUE2 + " ) "),
                        message)
        );

    }

    @Test
    public void testFunctions() throws ParseException {

        // is (not) null.
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(DOUBLE_HEADER2 + " IS NOT NULL"),
                        message)
        );
        Assert.assertEquals(Boolean.FALSE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(STRING_HEADER1 + " IS NULL"),
                        message)
        );
        // unknown header.
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector("unknown_header is null"),
                        message)
        );


        // Between ...
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(DOUBLE_HEADER2 + " BETWEEN 1 AND 2"),
                        message)
        );
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(" ( - " + INT_HEADER1 + " * 2 + "
                                                            + DOUBLE_HEADER1 + " * 4 ) / 10.0 between 0 and 3 * "
                                                            + INT_HEADER2 + " * 2.4"),
                        message)
        );
        Assert.assertEquals(Boolean.FALSE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(INT_HEADER2
                                                            + " not between (0.4 * 2 + (0.01 + 0.3 * 0.2) ) AND 10.0"),
                        // SelectorParser.parseMessageSelector(INT_HEADER2 + " NOT BETWEEN 1 AND 3"),
                        message)
        );
        Assert.assertEquals(Boolean.FALSE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(INT_HEADER2
                                + " NOT BETWEEN (0.4 * 2 + (0.01 + 0.3 * 0.2) + "
                                + DOUBLE_VALUE1 + " / 10.0 ) AND 10.0"),
                        message)
        );

        // must throw runtime evaluation exception and return null (NOT parse time exception ) and so evaluate to false.
        Assert.assertNull(
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(STRING_HEADER1 + " * 2 + " + DOUBLE_HEADER1
                                                            + " / 1.4 NOT BETWEEN (0.4 * 2 + (0.01 + 0.3 * 0.2) + "
                                                            + DOUBLE_VALUE1 + " / 10.0 ) AND 10.0"),
                        message)
        );


        // (not)? IN
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(
                                STRING_HEADER3 + " IN ( '" + STRING_VALUE1
                                + "', '" + STRING_VALUE2 + "', '" + STRING_VALUE3 +
                                "', '" + STRING_VALUE4 + "', '" + STRING_QUOTED_QUOTES_VALUE + "') "),
                        message)
        );
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(
                                STRING_QUOTES_HEADER + " IN ( '" + STRING_VALUE1
                                + "', '" + STRING_VALUE2 + "', '" + STRING_VALUE3 +
                                "', '" + STRING_VALUE4 + "', '" + STRING_QUOTED_QUOTES_VALUE + "') "),
                        message)
        );
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(
                                STRING_QUOTES_HEADER + " NOT IN ( '" + STRING_VALUE1
                                + "', '" + STRING_VALUE2 + "', '" + STRING_VALUE3 +
                                "', '" + STRING_VALUE4 + "') "),
                        message)
        );
        // using non string identifiers used in 'IN' construct should return null.
        Assert.assertNull(
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(
                                INT_HEADER1 + " NOT IN ( '" + STRING_VALUE1 + "', '"
                                + STRING_VALUE2 + "', '" + STRING_VALUE3 +
                                        "', '" + STRING_VALUE4 + "') "),
                        message)
        );
        Assert.assertNull(
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(
                                BOOLEAN_HEADER1 + " IN ( '" + STRING_VALUE1 + "', '" + STRING_VALUE2
                                + "', '" + STRING_VALUE3 + "', '" + STRING_VALUE4 + "') "),
                        message)
        );


        // like
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(
                                STRING_HEADER1 + " LIKE 'header\\_%' ESCAPE '\\'"),
                        message)
        );

        // value is - ""value with a _ and % in it and a \n with \t also for testing."";
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(
                                STRING_LIKE_HEADER + " LIKE '% with a \\_ and \\%%' ESCAPE '\\'"),
                        message)
        );

        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(
                                STRING_LIKE_HEADER + " LIKE '%\n%'"),
                        message)
        );
        Assert.assertEquals(Boolean.TRUE,
                SelectorParser.evaluateSelector(
                        SelectorParser.parseMessageSelector(
                                STRING_LIKE_HEADER + " NOT LIKE '%\r%'"),
                        message)
        );
    }
}
