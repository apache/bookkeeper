/**
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
package org.apache.bookkeeper.sasl;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/**
 * SASL Constants.
 */
public class SaslConstants {

    static final String PLUGIN_NAME = "sasl";

    public static final String JAAS_BOOKIE_SECTION_NAME = "saslJaasBookieSectionName";
    public static final String JAAS_DEFAULT_BOOKIE_SECTION_NAME = "Bookie";

    public static final String JAAS_AUDITOR_SECTION_NAME = "saslJaasAuditorSectionName";
    public static final String JAAS_DEFAULT_AUDITOR_SECTION_NAME = "Auditor";

    public static final String JAAS_CLIENT_SECTION_NAME = "saslJaasClientSectionName";
    public static final String JAAS_DEFAULT_CLIENT_SECTION_NAME = "BookKeeper";

    /**
     * This is a regexp which limits the range of possible ids which can connect to the Bookie using SASL.
     * By default only clients whose id contains the word 'bookkeeper' are allowed to connect.
     */
    public static final String JAAS_CLIENT_ALLOWED_IDS = "saslJaasClientAllowedIds";
    public static final String JAAS_CLIENT_ALLOWED_IDS_DEFAULT = ".*bookkeeper.*";

    static final String KINIT_COMMAND_DEFAULT = "/usr/bin/kinit";

    static final String KINIT_COMMAND = "kerberos.kinit";

    static final String SASL_BOOKKEEPER_PROTOCOL = "bookkeeper";
    static final String SASL_BOOKKEEPER_REALM = "bookkeeper";
    static final String SASL_SERVICE_NAME = "bookkeeper.sasl.servicename";
    static final String SASL_SERVICE_NAME_DEFAULT = "bookkeeper";

    static final String SASL_MD5_DUMMY_HOSTNAME = "bookkeeper";

    static boolean isUsingTicketCache(String configurationEntry) {

        AppConfigurationEntry[] entries = Configuration.getConfiguration()
            .getAppConfigurationEntry(configurationEntry);
        if (entries == null) {
            return false;
        }
        for (AppConfigurationEntry entry : entries) {
            // there will only be a single entry, so this for() loop will only be iterated through once.
            if (entry.getOptions().get("useTicketCache") != null) {
                String val = (String) entry.getOptions().get("useTicketCache");
                if (val.equals("true")) {
                    return true;
                }
            }
        }
        return false;
    }

    static String getPrincipal(String configurationEntry) {

        AppConfigurationEntry[] entries = Configuration.getConfiguration()
            .getAppConfigurationEntry(configurationEntry);
        if (entries == null) {
            return null;
        }
        for (AppConfigurationEntry entry : entries) {
            if (entry.getOptions().get("principal") != null) {
                return (String) entry.getOptions().get("principal");
            }
        }
        return null;
    }
}
