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
package org.apache.distributedlog.function;

import org.apache.distributedlog.io.AsyncCloseable;
import scala.Function0;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

/**
 * Function to close {@link org.apache.distributedlog.io.AsyncCloseable}
 */
public class CloseAsyncCloseableFunction extends AbstractFunction0<BoxedUnit> {

    /**
     * Return a function to close an {@link AsyncCloseable}.
     *
     * @param closeable closeable to close
     * @return function to close an {@link AsyncCloseable}
     */
    public static Function0<BoxedUnit> of(AsyncCloseable closeable) {
        return new CloseAsyncCloseableFunction(closeable);
    }

    private final AsyncCloseable closeable;

    private CloseAsyncCloseableFunction(AsyncCloseable closeable) {
        this.closeable = closeable;
    }

    @Override
    public BoxedUnit apply() {
        closeable.asyncClose();
        return BoxedUnit.UNIT;
    }
}
