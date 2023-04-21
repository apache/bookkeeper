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
package org.apache.bookkeeper.util;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

/**
 * Certificate parsing utilities.
 */
public abstract class CertUtils {

    // OU values
    public static final String OU_ROLE_NAME_CODE = "0";
    public static final String OU_CLUSTER_NAME_CODE = "1";

    public static final String OU_VALUES_SEPARATOR = ";";
    public static final String OU_CODE_SEPARATOR = ":";
    public static final String OU_NAME_SEPARATOR = ",";

    static final Pattern OU_VALUES_SPLITTER = Pattern.compile(OU_VALUES_SEPARATOR);
    static final Pattern OU_GENERAL_NAME_REGEX = Pattern.compile("^([0-9]+)" + OU_CODE_SEPARATOR + "(.*)$");
    static final Pattern OU_NAME_SPLITTER = Pattern.compile(OU_NAME_SEPARATOR);

    private CertUtils() {
    }

    public static String getOUString(X509Certificate cert) throws IOException {
        return getOUStringFromSubject(cert.getSubjectX500Principal().getName());
    }

    public static String getOUStringFromSubject(String subject) throws IOException {
        try {
            LdapName ldapDN = new LdapName(subject);
            for (Rdn rdn : ldapDN.getRdns()) {
                if ("OU".equalsIgnoreCase(rdn.getType())) {
                    return rdn.getValue().toString();
                }
            }
            return null;
        } catch (InvalidNameException ine) {
            throw new IOException(ine);
        }
    }

    public static Map<String, String> getOUMapFromOUString(String ou) throws IOException {
        Map<String, String> ouMap = new HashMap<>();
        if (ou != null) {
            String[] ouParts = OU_VALUES_SPLITTER.split(ou);
            for (String ouPart : ouParts) {
                Matcher matcher = OU_GENERAL_NAME_REGEX.matcher(ouPart);
                if (matcher.find() && matcher.groupCount() == 2) {
                    ouMap.put(matcher.group(1).trim(), matcher.group(2).trim());
                }
            }
        }
        return Collections.unmodifiableMap(ouMap);
    }

    public static Map<String, String> getOUMap(X509Certificate cert) throws IOException {
        return getOUMapFromOUString(getOUString(cert));
    }

    public static String[] getRolesFromOU(X509Certificate cert) throws IOException {
        return getRolesFromOUMap(getOUMap(cert));
    }

    public static String[] getRolesFromOUMap(Map<String, String> ouMap) throws IOException {
        String roleNames = ouMap.get(OU_ROLE_NAME_CODE);
        if (roleNames != null) {
            String[] roleParts = OU_NAME_SPLITTER.split(roleNames);
            if (roleParts.length > 0) {
                List<String> roles = new ArrayList<>(roleParts.length);
                for (String role : roleParts) {
                    roles.add(role.trim());
                }
                return roles.toArray(new String[roles.size()]);
            }
        }
        return null;
    }
}
