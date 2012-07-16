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
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include "gtest/gtest.h"

#include "../lib/util.h"
#include <hedwig/exceptions.h>
#include <stdexcept>

TEST(UtilTest, testHostAddress) {
  // good address (no ports)
  Hedwig::HostAddress a1 = Hedwig::HostAddress::fromString("www.yahoo.com");
  ASSERT_TRUE(a1.port() == 4080);

  // good address with ip (no ports)
  Hedwig::HostAddress a2 = Hedwig::HostAddress::fromString("127.0.0.1");
  ASSERT_TRUE(a2.port() == 4080);
  ASSERT_TRUE(a2.ip() == ((127 << 24) | 1));

  // good address
  Hedwig::HostAddress a3 = Hedwig::HostAddress::fromString("www.yahoo.com:80");
  ASSERT_TRUE(a3.port() == 80);

  // good address with ip
  Hedwig::HostAddress a4 = Hedwig::HostAddress::fromString("127.0.0.1:80");
  ASSERT_TRUE(a4.port() == 80);
  ASSERT_TRUE(a4.ip() == ((127 << 24) | 1));

  // good address (with ssl)
  Hedwig::HostAddress a5 = Hedwig::HostAddress::fromString("www.yahoo.com:80:443");
  ASSERT_TRUE(a5.port() == 80);

  // good address with ip
  Hedwig::HostAddress a6 = Hedwig::HostAddress::fromString("127.0.0.1:80:443");
  ASSERT_TRUE(a6.port() == 80);
  ASSERT_TRUE(a6.ip() == ((127 << 24) | 1));

  // nothing
  ASSERT_THROW(Hedwig::HostAddress::fromString(""), Hedwig::HostResolutionException);
    
  // nothing but colons
  ASSERT_THROW(Hedwig::HostAddress::fromString("::::::::::::::::"), Hedwig::ConfigurationException);
    
  // only port number
  ASSERT_THROW(Hedwig::HostAddress::fromString(":80"), Hedwig::HostResolutionException);
 
  // text after colon (isn't supported)
  ASSERT_THROW(Hedwig::HostAddress::fromString("www.yahoo.com:http"), Hedwig::ConfigurationException);
    
  // invalid hostname
  ASSERT_THROW(Hedwig::HostAddress::fromString("com.oohay.www:80"), Hedwig::HostResolutionException);
    
  // null
  ASSERT_THROW(Hedwig::HostAddress::fromString(NULL), std::logic_error);
}

