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
package org.apache.bookkeeper.http;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import lombok.CustomLog;
import org.apache.commons.configuration2.Configuration;

/**
 * Class to load and instantiate http server from config.
 */
@CustomLog
public class HttpServerLoader {

    public static final String HTTP_SERVER_CLASS = "httpServerClass";
    static HttpServer server = null;

    public static void loadHttpServer(Configuration conf) {
        String className = conf.getString(HTTP_SERVER_CLASS);
        if (className != null) {
            try {
                Class cls = Class.forName(className);
                @SuppressWarnings("unchecked")
                Constructor<? extends HttpServer> cons =
                    (Constructor<? extends HttpServer>) cls.getDeclaredConstructor();
                server = cons.newInstance();
            } catch (ClassNotFoundException cnfe) {
                log.error().exception(cnfe).attr("className", className).log("Couldn't find configured class");
            } catch (NoSuchMethodException nsme) {
                log.error().exception(nsme).attr("className", className)
                        .log("Couldn't find default constructor for class");
            } catch (InstantiationException ie) {
                log.error().exception(ie).attr("className", className).log("Couldn't construct class");
            } catch (IllegalAccessException iae) {
                log.error().exception(iae).attr("className", className)
                        .log("Couldn't construct class. Is the constructor private?");
            } catch (InvocationTargetException ite) {
                log.error().exception(ite).log("Constructor threw an exception. It should not have.");
            }
        }
    }

    public static HttpServer get() {
        return server;
    }
}
