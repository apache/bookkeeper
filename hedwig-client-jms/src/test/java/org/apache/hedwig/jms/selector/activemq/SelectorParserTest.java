/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.jms.selector.activemq;

import org.apache.hedwig.jms.message.TextMessageImpl;
import org.apache.hedwig.jms.selector.Node;
import org.apache.hedwig.jms.selector.ParseException;
import org.apache.hedwig.jms.selector.SelectorParser;
import org.junit.Test;

import javax.jms.JMSException;

/**
 * Based on ActiveMQ's codebase : modified to suit our codebase.
 */
public class SelectorParserTest {

    @Test
    public void testParseWithParensAround() throws JMSException, ParseException {
        String[] values = {"x = 1 and y = 2", "(x = 1) and (y = 2)", "((x = 1) and (y = 2))"};

        TextMessageImpl message = new TextMessageImpl(null, "test");
        message.setIntProperty("x", 1);
        message.setIntProperty("y", 2);

        for (String value : values) {
            Node ast = SelectorParser.parseMessageSelector(value);
            assert Boolean.TRUE.equals(SelectorParser.evaluateSelector(ast, message));
        }
    }

}
