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

package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.IOException;

import org.apache.bookkeeper.conf.ServerConfiguration;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import junit.framework.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateNewLogTest {
    private static final Logger LOG = LoggerFactory
    .getLogger(CreateNewLogTest.class);
        
    private String[] ledgerDirs; 
    private int numDirs = 100;
    
    @Before
    public void setUp() throws Exception{
        ledgerDirs = new String[numDirs];
        for(int i = 0; i < numDirs; i++){
            File temp = File.createTempFile("bookie", "test");
            temp.delete();
            temp.mkdir();
            File currentTemp = new File(temp.getAbsoluteFile() + "/current");
            currentTemp.mkdir();
            ledgerDirs[i] = temp.getPath();
        }        
    }
    
    @After
    public void tearDown() throws Exception{
        for(int i = 0; i < numDirs; i++){
            File f = new File(ledgerDirs[i]);
            deleteRecursive(f);
        }
    }
    
    private void deleteRecursive(File f) {
        if (f.isDirectory()){
            for (File c : f.listFiles()){
                deleteRecursive(c);
            }
        }
        
        f.delete();
    }
    
    /**
     * Checks if new log file id is verified against all directories.
     * 
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-465}
     * 
     * @throws Exception
     */
    @Test(timeout=60000)
    public void testCreateNewLog() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
                     
        // Creating a new configuration with a number of 
        // ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf);
        EntryLogger el = new EntryLogger(conf, ledgerDirsManager);
        
        // Extracted from createNewLog()
        String logFileName = Long.toHexString(1) + ".log";
        File dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: " + dir);
        File newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();
        
        // Calls createNewLog, and with the number of directories we
        // are using, if it picks one at random it will fail.
        el.createNewLog();
        LOG.info("This is the current log id: " + el.getCurrentLogId());
        Assert.assertTrue("Wrong log id", el.getCurrentLogId() > 1);
    }

}