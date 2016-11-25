/*
 * Copyright 2016 The Apache Software Foundation.
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
package org.apache.bookkeeper.sasl;

/**
 * Utility for common Sasl Plugin values
 */
class SaslConstants {

    static final String PLUGIN_NAME = "sasl";

    static final String JAAS_BOOKIE_SECTION_NAME = "saslJaasBookieSectionName";
    static final String JAAS_DEFAULT_BOOKIE_SECTION_NAME = "Bookie";

    static final String JAAS_AUDITOR_SECTION_NAME = "saslJaasAuditorSectionName";
    static final String JAAS_DEFAULT_AUDITOR_SECTION_NAME = "Auditor";

    static final String JAAS_CLIENT_SECTION_NAME = "saslJaasClientSectionName";
    static final String JAAS_DEFAULT_CLIENT_SECTION_NAME = "BookKeeper";

    static final String SASL_BOOKKEEPER_PROTOCOL = "bookkeeper";
    static final String SASL_BOOKKEEPER_REALM = "bookkeeper";

    static final String SASL_MD5_DUMMY_HOSTNAME = "bookkeeper";
}
