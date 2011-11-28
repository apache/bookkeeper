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

#include "eventdispatcher.h"

#include <log4cxx/logger.h>

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("hedwig."__FILE__));

using namespace Hedwig;

EventDispatcher::EventDispatcher(int numThreads)
  : num_threads(numThreads), running(false), next_io_service(0) {
  if (0 == num_threads) {
    throw std::runtime_error("number of threads in dispatcher is zero");
  }
  for (size_t i = 0; i < num_threads; i++) {
    io_service_ptr service(new boost::asio::io_service);
    services.push_back(service);
  }
}

void EventDispatcher::run_forever(io_service_ptr service, size_t idx) {
  LOG4CXX_DEBUG(logger, "Starting event dispatcher " << idx);

  while (true) {
    try {
      service->run();
      break;
    } catch (std::exception &e) {
    LOG4CXX_ERROR(logger, "Exception in dispatch handler " << idx << " : " << e.what());
    }
  }
  LOG4CXX_DEBUG(logger, "Event dispatcher " << idx << " done");
}

void EventDispatcher::start() {
  if (running) {
    return;
  }
  for (size_t i = 0; i < num_threads; i++) {
    io_service_ptr service = services[i];
    work_ptr work(new boost::asio::io_service::work(*service));
    works.push_back(work);
    // new thread
    thread_ptr t(new boost::thread(boost::bind(&EventDispatcher::run_forever, this, service, i)));
    threads.push_back(t);
  }
  running = true;
}

void EventDispatcher::stop() {
  if (!running) {
    return;
  }

  works.clear();

  for (size_t i = 0; i < num_threads; i++) {
    services[i]->stop();
  }

  for (size_t i = 0; i < num_threads; i++) {
    threads[i]->join();
  }
  threads.clear();

  running = false;
}

EventDispatcher::~EventDispatcher() {
  services.clear();
}

boost::asio::io_service& EventDispatcher::getService() {
  size_t next = 0;
  {
    boost::lock_guard<boost::mutex> lock(next_lock);
    next = next_io_service;
    next_io_service = (next_io_service + 1) % num_threads;
  }
  boost::asio::io_service& service = *services[next];
  return service;
}
