/*
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
package org.apache.bookkeeper.net;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.net.InetAddress;
import javax.naming.NamingException;
import org.junit.Test;
import org.mockito.MockedStatic;

public class DNSTest {

    @Test
    public void testReverseDnsForLoopbackAddressDoesNotQueryPtr() throws Exception {
        String expectedHostname = DNS.getDefaultHost("default");
        try (MockedStatic<DNS> mockedDns = mockStatic(DNS.class, invocation -> {
            if ("getPtrAttributes".equals(invocation.getMethod().getName())) {
                throw new NamingException("simulated DNS timeout");
            }
            return invocation.callRealMethod();
        })) {
            String hostname = DNS.reverseDns(InetAddress.getByName("127.0.0.1"), null);

            assertEquals(expectedHostname, hostname);
            mockedDns.verify(() -> DNS.getPtrAttributes(anyString(), nullable(String.class)), never());
        }
    }

}
