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
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.client.api.BKException.Code.WriteException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;

/**
 * Test ensemble change has a max num.
 */
public class TestMaxEnsembleChangeNum extends BookKeeperClusterTestCase {

    final DigestType digestType;
    final byte[] testPasswd = "".getBytes();
    final byte[] data = "foobar".getBytes();


    public TestMaxEnsembleChangeNum() {
        super(3);
        this.digestType = DigestType.CRC32;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        baseClientConf.setDelayEnsembleChange(false);
        baseClientConf.setMaxEnsembleChangesNum(5);
        super.setUp();
    }

    @Test
    public void testChangeEnsembleMaxNum() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, 2, digestType, testPasswd);
        int numEntries = 5;
        int changeNum = 5;
        //first fragment
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }
        simulateEnsembleChange(changeNum, numEntries, lh);

        // one more ensemble change
        startNewBookie();
        killBookie(lh.getLedgerMetadata().currentEnsemble.get(0));
        // add failure
        try {
            lh.addEntry(data);
            fail("should not come to here");
        } catch (BKException exception){
            assertEquals(exception.getCode(), WriteException);
        }

    }


    private void simulateEnsembleChange(int changeNum, int numEntries, LedgerHandle lh) throws Exception{

        int expectedSize = lh.getLedgerMetadata().getEnsembles().size() + 1;
        //kill bookie and add again
        for (int num = 0; num < changeNum; num++){
            startNewBookie();

            killBookie(lh.getLedgerMetadata().currentEnsemble.get(0));
            for (int i = 0; i < numEntries; i++) {
                lh.addEntry(data);
            }
            // ensure there is a ensemble changed
            assertEquals("There should be one ensemble change",
                    expectedSize + num, lh.getLedgerMetadata().getEnsembles().size());
        }
    }
}