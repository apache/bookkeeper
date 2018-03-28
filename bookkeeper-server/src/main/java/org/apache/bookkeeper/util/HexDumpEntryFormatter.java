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

package org.apache.bookkeeper.util;

import java.io.IOException;

import org.apache.commons.io.HexDump;

/**
 * A hex dump entry formatter.
 */
public class HexDumpEntryFormatter extends EntryFormatter {
    @Override
    public void formatEntry(byte[] data) {
        try {
            HexDump.dump(data, 0, System.out, 0);
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Warn: Index is outside the data array's bounds : " + e.getMessage());
        } catch (IllegalArgumentException e) {
            System.out.println("Warn: The output stream is null : " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Warn: Something has gone wrong writing the data to stream : " + e.getMessage());
        }
    }

    @Override
    public void formatEntry(java.io.InputStream input) {
        try {
            byte[] data = new byte[input.available()];
            input.read(data, 0, data.length);
            formatEntry(data);
        } catch (IOException ie) {
            System.out.println("Warn: Unreadable entry : " + ie.getMessage());
        }
    }

}
