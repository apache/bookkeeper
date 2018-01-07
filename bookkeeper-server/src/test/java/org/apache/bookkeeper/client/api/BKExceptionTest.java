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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Field;
import org.junit.Test;

/**
 * Tests for BKException methods.
 */
public class BKExceptionTest {

    @Test
    public void testGetMessage() throws Exception {
        Field[] fields = BKException.Code.class.getFields();
        for (Field f : fields) {
            if (f.getType() == Integer.TYPE && !f.getName().equals("UNINITIALIZED")) {
                int code = f.getInt(null);
                String msg = BKException.getMessage(code);
                if (code == UnexpectedConditionException) {
                    assertThat(msg, equalTo("Unexpected condition"));
                } else {
                    assertThat("failure on code " + f.getName(), msg,
                               not(equalTo("Unexpected condition")));
                }
            }
        }
    }
}
