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

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Holds (any) state data required to evaluate a Selector.
 */
public class SelectorEvalState {
    private final MessageImpl message;
    private final Deque<SelectorConstant> stack;

    // Used ONLY for debugging ... it is sad that this is part of SelectorEvalState - but I dont
    // have time to do anything else right now !
    private int debugIndentCount = 0;

    public SelectorEvalState(MessageImpl message) {
        this.message = message;
        this.stack = new ArrayDeque<SelectorConstant>(32);
    }

    public MessageImpl getMessage() {
        return message;
    }

    public Deque<SelectorConstant> getStack() {
        return stack;
    }

    public int getDebugIndentCount() {
        return debugIndentCount;
    }

    public void setDebugIndentCount(int debugIndentCount) {
        this.debugIndentCount = debugIndentCount;
    }
}
