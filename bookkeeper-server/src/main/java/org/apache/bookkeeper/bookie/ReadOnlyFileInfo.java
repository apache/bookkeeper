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

package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.IOException;

/**
 * Provide a readonly file info.
 */
class ReadOnlyFileInfo extends FileInfo {

    public ReadOnlyFileInfo(File lf, byte[] masterKey) throws IOException {
        /*
         * For ReadOnlyFile it is okay to initialize FileInfo with
         * CURRENT_HEADER_VERSION, when fileinfo.readHeader is called it would
         * read actual header version.
         */
        super(lf, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        mode = "r";
    }

}
