/*
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
 */

package org.apache.bookkeeper.tools.common;

/**
 * Classes to keep a list of command categories.
 */
public final class BKCommandCategories {

    // commands that operate cluster and nodes
    public static final String CATEGORY_INFRA_SERVICE = "Infrastructure commands";

    // commands that operate ledger service
    public static final String CATEGORY_LEDGER_SERVICE = "Ledger service commands";

    // commands that operate table service
    public static final String CATEGORY_TABLE_SERVICE = "Table service commands";

}
