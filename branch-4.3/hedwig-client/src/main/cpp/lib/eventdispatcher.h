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
#ifndef EVENTDISPATCHER_H
#define EVENTDISPATCHER_H

#include <vector>

#include <hedwig/client.h>

#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>

namespace Hedwig {
  typedef boost::shared_ptr<boost::asio::io_service::work> work_ptr;
  typedef boost::shared_ptr<boost::thread> thread_ptr;

  class IOService;
  typedef boost::shared_ptr<IOService> IOServicePtr;

  class IOService {
  public:
    IOService();
    virtual ~IOService();

    // start the io service
    void start();
    // stop the io service
    void stop();
    // run the io service
    void run();

    inline boost::asio::io_service& getService() {
      return service;
    }

  private:
    boost::asio::io_service service;  
    work_ptr work;
  };

  class EventDispatcher {
  public:  
    EventDispatcher(const Configuration& conf);
    ~EventDispatcher();
    
    void start();

    void stop();
    
    IOServicePtr& getService();

  private:
    void run_forever(IOServicePtr service, size_t idx);

    const Configuration& conf;

    // number of threads
    size_t num_threads;
    // running flag
    bool running;
    // pool of io_services.
    std::vector<IOServicePtr> services;
    // threads
    std::vector<thread_ptr> threads;
    // next io_service used for a connection
    boost::mutex next_lock;
    std::size_t next_io_service;
  };
}

#endif
