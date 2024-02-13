/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.metadata.etcd;

import io.netty.handler.ssl.SslProvider;
import org.apache.commons.configuration.CompositeConfiguration;

class EtcdConfig extends CompositeConfiguration {
    private static final String USE_TLS = "useTls";
    private static final String TLS_PROVIDER = "tlsProvider";
    private static final String TLS_TRUST_CERTS_FILE_PATH = "tlsTrustCertsFilePath";
    private static final String TLS_KEY_FILE_PATH = "tlsKeyFilePath";
    private static final String TLS_CERTIFICATE_FILE_PATH = "tlsCertificateFilePath";
    private static final String AUTHORITY = "authority";

    public boolean isUseTls() {
        return getBoolean(USE_TLS, false);
    }

    public SslProvider getTlsProvider() {
        return SslProvider.valueOf(getString(TLS_PROVIDER));
    }

    public String getTlsTrustCertsFilePath() {
        return getString(TLS_TRUST_CERTS_FILE_PATH);
    }

    public String getTlsKeyFilePath() {
        return getString(TLS_KEY_FILE_PATH);
    }

    public String getTlsCertificateFilePath() {
        return getString(TLS_CERTIFICATE_FILE_PATH);
    }

    public String getAuthority() {
        return getString(AUTHORITY);
    }
}

