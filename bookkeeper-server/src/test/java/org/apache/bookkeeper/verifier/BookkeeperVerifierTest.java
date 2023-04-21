/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.verifier;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Test for BookKeeperVerifier.
 */
public class BookkeeperVerifierTest extends BookKeeperClusterTestCase {
    public BookkeeperVerifierTest() {
        super(3);
    }

    /**
     * Simple test to verify that the verifier works against a local cluster.
     */
    @Test(timeout = 30000)
    public void testBasic() throws Exception {
        DirectBookkeeperDriver driver = new DirectBookkeeperDriver(bkc);
        BookkeeperVerifier verifier = new BookkeeperVerifier(
                driver,
                3,
                3,
                2,
                10,
                5,
                16,
                4,
                2,
                2,
                32,
                16 << 10,
                4 << 10,
                4,
                0.5
        );
        verifier.run();
    }
}
