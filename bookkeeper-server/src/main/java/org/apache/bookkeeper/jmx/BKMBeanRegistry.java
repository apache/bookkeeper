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

package org.apache.bookkeeper.jmx;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.jmx.ZKMBeanInfo;

/**
 * This class provides a unified interface for registering/unregistering of
 * bookkeeper MBeans with the platform MBean server. It builds a hierarchy of MBeans
 * where each MBean represented by a filesystem-like path. Eventually, this hierarchy
 * will be stored in the zookeeper data tree instance as a virtual data tree.
 */
public class BKMBeanRegistry extends MBeanRegistry {
    static final Logger LOG = LoggerFactory.getLogger(BKMBeanRegistry.class);
    
    static final String DOMAIN = "org.apache.BookKeeperService";

    static BKMBeanRegistry instance=new BKMBeanRegistry(); 

    public static BKMBeanRegistry getInstance(){
        return instance;
    }

    protected String getDomainName() {
        return DOMAIN;
    }
    
    /**
     * This takes a path, such as /a/b/c, and converts it to 
     * name0=a,name1=b,name2=c
     * 
     * Copy from zookeeper MBeanRegistry since tokenize is private
     */
    protected int tokenize(StringBuilder sb, String path, int index) {
        String[] tokens = path.split("/");
        for (String s: tokens) {
            if (s.length()==0)
                continue;
            sb.append("name").append(index++).append("=").append(s).append(",");
        }
        return index;
    }

    /**
     * Builds an MBean path and creates an ObjectName instance using the path. 
     * @param path MBean path
     * @param bean the MBean instance
     * @return ObjectName to be registered with the platform MBean server
     */
    protected ObjectName makeObjectName(String path, ZKMBeanInfo bean)
        throws MalformedObjectNameException {
        if(path==null)
            return null;
        StringBuilder beanName = new StringBuilder(getDomainName() + ":");
        int counter=0;
        counter=tokenize(beanName,path,counter);
        tokenize(beanName,bean.getName(),counter);
        beanName.deleteCharAt(beanName.length()-1);
        try {
            return new ObjectName(beanName.toString());
        } catch (MalformedObjectNameException e) {
            LOG.warn("Invalid name \"" + beanName.toString() + "\" for class "
                    + bean.getClass().toString());
            throw e;
        }
    }
}
