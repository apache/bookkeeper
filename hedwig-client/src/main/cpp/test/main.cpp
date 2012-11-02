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

#include "../lib/channel.h"
#include "../lib/util.h"
#include <hedwig/protocol.h>
#include <hedwig/callback.h>
#include <iostream>

#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/helpers/exception.h>

#include "util.h"

#include "gtest/gtest.h"

bool TestServerConfiguration::isSSL = false;
std::string TestServerConfiguration::certFile = "";
bool TestServerConfiguration::multiplexing = false;

int main( int argc, char **argv)
{
  try {
    if (getenv("LOG4CXX_CONF") == NULL) {
      std::cerr << "Set LOG4CXX_CONF in your environment to get logging." << std::endl;
      log4cxx::BasicConfigurator::configure();
    } else {
      log4cxx::PropertyConfigurator::configure(getenv("LOG4CXX_CONF"));
    }
  } catch (std::exception &e) {
    std::cerr << "exception caught while configuring log4cpp via : " << e.what() << std::endl;
  } catch (...) {
    std::cerr << "unknown exception while configuring log4cpp vi'." << std::endl;
  }

  // Enable SSL for testing
  int opt;
  while((opt = getopt(argc,argv,"s:c:m:")) > 0) {
    switch(opt) {
    case 's':
      if (std::string(optarg) == "true") {
        std::cout << "run in ssl mode...." << std::endl;
        TestServerConfiguration::isSSL = true;
      } else {
        TestServerConfiguration::isSSL = false;
      }
      break;
    case 'm':
      if (std::string(optarg) == "true") {
        std::cout << "run in multiplexing mode ..." << std::endl;
        TestServerConfiguration::multiplexing = true;
      } else {
        TestServerConfiguration::multiplexing = false;
      }
      break;
    case 'c':
      std::cout << "use cert file :" << optarg << std::endl;
      TestServerConfiguration::certFile = std::string(optarg);
      break;
    }//switch
  }//while
  
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  google::protobuf::ShutdownProtobufLibrary();

  return ret;
}
