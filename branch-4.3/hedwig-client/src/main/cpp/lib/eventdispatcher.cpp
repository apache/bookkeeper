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

const int DEFAULT_NUM_DISPATCH_THREADS = 1;

IOService::IOService() {
}

IOService::~IOService() {}

void IOService::start() {
  if (work.get()) {
    return;
  }
  work = work_ptr(new boost::asio::io_service::work(service));
}

void IOService::stop() {
  if (!work.get()) {
    return;
  }

  work = work_ptr();
  service.stop();
}

void IOService::run() {
  while (true) {
    try {
      service.run();
      break;
    } catch (std::exception &e) {
      LOG4CXX_ERROR(logger, "Exception in IO Service " << this << " : " << e.what());
    }
  }
}

EventDispatcher::EventDispatcher(const Configuration& conf)
  : conf(conf), running(false), next_io_service(0) {
  num_threads = conf.getInt(Configuration::NUM_DISPATCH_THREADS,
                            DEFAULT_NUM_DISPATCH_THREADS);
  if (0 == num_threads) {
    LOG4CXX_ERROR(logger, "Number of threads in dispatcher is zero");
    throw std::runtime_error("number of threads in dispatcher is zero");
  }
  for (size_t i = 0; i < num_threads; i++) {
    services.push_back(IOServicePtr(new IOService()));
  }
  LOG4CXX_DEBUG(logger, "Created EventDispatcher " << this);
}

void EventDispatcher::run_forever(IOServicePtr service, size_t idx) {
  LOG4CXX_INFO(logger, "Starting event dispatcher " << idx);

  service->run();

  LOG4CXX_INFO(logger, "Event dispatcher " << idx << " done");
}

void EventDispatcher::start() {
  if (running) {
    return;
  }

  for (size_t i = 0; i < num_threads; i++) {
    IOServicePtr service = services[i];
    service->start();
    // new thread
    thread_ptr t(new boost::thread(boost::bind(&EventDispatcher::run_forever,
                                               this, service, i)));
    threads.push_back(t);
  }
  running = true;
}

void EventDispatcher::stop() {
  if (!running) {
    return;
  }

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

IOServicePtr& EventDispatcher::getService() {
  size_t next = 0;
  {
    boost::lock_guard<boost::mutex> lock(next_lock);
    next = next_io_service;
    next_io_service = (next_io_service + 1) % num_threads;
  }
  return services[next];
}
