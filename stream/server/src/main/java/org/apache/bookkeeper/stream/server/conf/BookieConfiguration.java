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
package org.apache.bookkeeper.stream.server.conf;

import org.apache.bookkeeper.common.conf.ComponentConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;

/**
 * A configuration delegates bookkeeper's {@link org.apache.bookkeeper.conf.ServerConfiguration}.
 */
public class BookieConfiguration extends ComponentConfiguration {

    public static BookieConfiguration of(CompositeConfiguration conf) {
        return new BookieConfiguration(conf);
    }

    protected BookieConfiguration(CompositeConfiguration conf) {
        super(conf, "");
    }
}
