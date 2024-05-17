/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client.api;

import static org.apache.bookkeeper.client.api.BKException.Code.UnexpectedConditionException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;

/**
 * Tests for BKException methods.
 */
public class BKExceptionTest {

    @Test
    public void testGetMessage() throws Exception {
        Field[] fields = BKException.Code.class.getFields();
        int count = 0;
        for (Field f : fields) {
            if (f.getType() == Integer.TYPE && !f.getName().equals("UNINITIALIZED")) {
                int code = f.getInt(null);
                String msg = BKException.getMessage(code);
                if (code == UnexpectedConditionException) {
                    assertEquals("Unexpected condition", msg);
                } else {
                    assertNotEquals("failure on code " + f.getName(), "Unexpected condition", msg);
                }
                count++;
            }
        }
        // assert that we found at least 1 code other than UnexpectedConditionException
        assertTrue(count > 2);
    }
}
