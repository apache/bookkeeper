/*
 * Copyright 2017 The Apache Software Foundation.
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
package org.apache.bookkeeper.client.api;

import java.io.IOException;
import org.apache.bookkeeper.client.BKException;

/**
 * BookKeeper Client Builder to build client instances.
 */
public interface BookKeeperBuilder {

    /**
     * Configure the bookkeeper client with a provided component.
     *
     * @param component an external component to inject into the newly created BookKeeper clien
     *
     * @return client builder.
     *
     */
    public BookKeeperBuilder with(Object component);

    public BookKeeper build() throws BKException, InterruptedException;

}
