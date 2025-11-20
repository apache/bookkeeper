/*
 * Copyright 2020 The Apache Software Foundation.
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
package org.apache.bookkeeper.discover;

import static org.apache.bookkeeper.discover.ZKRegistrationClient.deserializeBookieServiceInfo;
import static org.apache.bookkeeper.discover.ZKRegistrationManager.serializeBookieServiceInfo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.bookkeeper.discover.BookieServiceInfo.Endpoint;
import org.apache.bookkeeper.net.BookieId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test of the {@link BookieServiceInfo} serialization/deserialization methods.
 */
public class BookieServiceInfoTest {

    @Test
    public void testSerializeDeserializeBookieServiceInfo() throws Exception {
        String bookieId = "127.0.0.1:3181";
        {
            BookieServiceInfo expected = new BookieServiceInfo();
            Endpoint endpointRPC = new Endpoint("1", 1281, "localhost", "bookie-rpc",
                    Collections.emptyList(), Collections.emptyList());
            Endpoint endpointHTTP = new Endpoint("2", 1281, "localhost", "bookie-http",
                    Collections.emptyList(), Collections.emptyList());
            expected.setEndpoints(Arrays.asList(endpointRPC, endpointHTTP));

            Map<String, String> properties = new HashMap<>();
            properties.put("test", "value");
            expected.setProperties(properties);

            byte[] serialized = serializeBookieServiceInfo(expected);
            BookieServiceInfo deserialized = deserializeBookieServiceInfo(BookieId.parse(bookieId), serialized);

            assertBookieServiceInfoEquals(expected, deserialized);
        }
    }

    @Test
    public void testDeserializeBookieServiceInfo() throws Exception {
        BookieId bookieId = BookieId.parse("127.0.0.1:3181");
        {
            BookieServiceInfo expected = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookieId.toString());
            BookieServiceInfo deserialized = deserializeBookieServiceInfo(bookieId, null);

            assertBookieServiceInfoEquals(expected, deserialized);
        }
        {
            BookieServiceInfo expected = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookieId.toString());
            BookieServiceInfo deserialized = deserializeBookieServiceInfo(bookieId, new byte[]{});

            assertBookieServiceInfoEquals(expected, deserialized);
        }
    }

    private void assertBookieServiceInfoEquals(BookieServiceInfo expected, BookieServiceInfo provided) {
        for (Endpoint ep : expected.getEndpoints()) {
            Endpoint e = provided.getEndpoints().stream()
                    .filter(ee -> Objects.equals(ee.getId(), ep.getId()))
                    .findFirst()
                    .get();
            assertThat(e.getHost(), is(ep.getHost()));
            assertThat(e.getPort(), is(ep.getPort()));
            assertThat(e.getProtocol(), is(ep.getProtocol()));
            assertArrayEquals(e.getAuth().toArray(), ep.getAuth().toArray());
            assertArrayEquals(e.getExtensions().toArray(), ep.getExtensions().toArray());
        }
        assertEquals(expected.getProperties(), provided.getProperties());
    }

    @Test
    public void testComparableBookieServerInfo() throws Exception {
        final BookieServiceInfo.Endpoint endpoint = new BookieServiceInfo.Endpoint(
                "http", 8080, "host1", "HTTP",
                Lists.newArrayList("auth1"), Lists.newArrayList("ext1")
        );
        final Map<String, String> properties1 = Maps.newHashMap();
        properties1.put("key", "value");
        final BookieServiceInfo info = new BookieServiceInfo(
                properties1,
                Lists.newArrayList(endpoint)
        );
        final ObjectMapper objectMapper = new ObjectMapper();
        final String bData = objectMapper.writeValueAsString(info);
        final BookieServiceInfo another = objectMapper.readValue(bData, BookieServiceInfo.class);
        Assert.assertEquals(info, another);
    }

}
