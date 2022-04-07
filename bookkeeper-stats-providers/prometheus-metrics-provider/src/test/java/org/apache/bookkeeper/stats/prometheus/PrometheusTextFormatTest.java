/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.stats.prometheus;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link PrometheusTextFormat}.
 */
public class PrometheusTextFormatTest {

    @Test
    public void testPrometheusTypeDuplicate() throws IOException {
        PrometheusTextFormat prometheusTextFormat = new PrometheusTextFormat();
        StringWriter writer = new StringWriter();
        prometheusTextFormat.writeType(writer, "counter", "gauge");
        prometheusTextFormat.writeType(writer, "counter", "gauge");
        String string = writer.toString();
        Assert.assertEquals("# TYPE counter gauge\n", string);
    }

}
