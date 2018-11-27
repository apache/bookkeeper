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
package org.apache.bookkeeper.common.util;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

/**
 * General Class Reflection Utils.
 */
public class ReflectionUtils {

    private static final Map<Class<?>, Constructor<?>> constructorCache =
            new ConcurrentHashMap<Class<?>, Constructor<?>>();

    /**
     * Returns the {@code Class} object object associated with the class or interface
     * with the given string name, which is a subclass of {@code xface}.
     *
     * @param className class name
     * @param xface class interface
     * @return the class object associated with the class or interface with the given string name.
     */
    public static <T> Class<? extends T> forName(String className,
                                                 Class<T> xface) {

        // Construct the class
        Class<?> theCls;
        try {
            theCls = Class.forName(className);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        }
        if (!xface.isAssignableFrom(theCls)) {
            throw new RuntimeException(className + " not " + xface.getName());
        }
        return theCls.asSubclass(xface);
    }

    /**
     * Get the value of the <code>name</code> property as a <code>Class</code>.
     * If no such property is specified, then <code>defaultCls</code> is returned.
     *
     * @param conf
     *          Configuration Object.
     * @param name
     *          Class Property Name.
     * @param defaultCls
     *          Default Class to be returned.
     * @param classLoader
     *          Class Loader to load class.
     * @return property value as a <code>Class</code>, or <code>defaultCls</code>.
     * @throws ConfigurationException
     */
    public static Class<?> getClass(Configuration conf, String name,
                                    Class<?> defaultCls, ClassLoader classLoader)
            throws ConfigurationException {
        String valueStr = conf.getString(name);
        if (null == valueStr) {
            return defaultCls;
        }
        try {
            return Class.forName(valueStr, true, classLoader);
        } catch (ClassNotFoundException cnfe) {
            throw new ConfigurationException(cnfe);
        }
    }

    /**
     * Get the value of the <code>name</code> property as a <code>Class</code> implementing
     * the interface specified by <code>xface</code>.
     *
     * <p>If no such property is specified, then <code>defaultValue</code> is returned.
     *
     * <p>An exception is thrown if the returned class does not implement the named interface.
     *
     * @param conf
     *          Configuration Object.
     * @param name
     *          Class Property Name.
     * @param defaultValue
     *          Default Class to be returned.
     * @param xface
     *          The interface implemented by the named class.
     * @param classLoader
     *          Class Loader to load class.
     * @return property value as a <code>Class</code>, or <code>defaultValue</code>.
     * @throws ConfigurationException
     */
    public static <T> Class<? extends T> getClass(Configuration conf,
                                                  String name, Class<? extends T> defaultValue,
                                                  Class<T> xface, ClassLoader classLoader)
        throws ConfigurationException {
        try {
            Class<?> theCls = getClass(conf, name, defaultValue, classLoader);
            if (null != theCls && !xface.isAssignableFrom(theCls)) {
                throw new ConfigurationException(theCls + " not " + xface.getName());
            } else if (null != theCls) {
                return theCls.asSubclass(xface);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Create an object for the given class.
     *
     * @param theCls
     *          class of which an object is created.
     * @return a new object
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> theCls) {
        T result;
        try {
            Constructor<T> meth = (Constructor<T>) constructorCache.get(theCls);
            if (null == meth) {
                meth = theCls.getDeclaredConstructor();
                meth.setAccessible(true);
                constructorCache.put(theCls, meth);
            }
            result = meth.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * Create an object using the given class name.
     *
     * @param clsName
     *          class name of which an object is created.
     * @param xface
     *          The interface implemented by the named class.
     * @return a new object
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(String clsName, Class<T> xface) {
        Class<?> theCls;
        try {
            theCls = Class.forName(clsName);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        }
        if (!xface.isAssignableFrom(theCls)) {
            throw new RuntimeException(clsName + " not " + xface.getName());
        }
        return newInstance(theCls.asSubclass(xface));
    }
}
