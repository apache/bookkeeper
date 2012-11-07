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

#include <hedwig/client.h>
#include <memory>

#include "clientimpl.h"
#include <log4cxx/logger.h>

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("hedwig."__FILE__));

using namespace Hedwig;

const std::string Configuration::DEFAULT_SERVER = "hedwig.cpp.default_server";
const std::string Configuration::MESSAGE_CONSUME_RETRY_WAIT_TIME = "hedwig.cpp.message_consume_retry_wait_time";
const std::string Configuration::SUBSCRIBER_CONSUME_RETRY_WAIT_TIME = "hedwig.cpp.subscriber_consume_retry_wait_time";
const std::string Configuration::MAX_MESSAGE_QUEUE_SIZE = "hedwig.cpp.max_msgqueue_size";
const std::string Configuration::RECONNECT_SUBSCRIBE_RETRY_WAIT_TIME = "hedwig.cpp.reconnect_subscribe_retry_wait_time";
const std::string Configuration::SYNC_REQUEST_TIMEOUT = "hedwig.cpp.sync_request_timeout";
const std::string Configuration::SUBSCRIBER_AUTOCONSUME = "hedwig.cpp.subscriber_autoconsume";
const std::string Configuration::NUM_DISPATCH_THREADS = "hedwig.cpp.num_dispatch_threads";
const std::string Configuration::SUBSCRIPTION_MESSAGE_BOUND = "hedwig.cpp.subscription_message_bound";
const std::string Configuration::SSL_ENABLED = "hedwig.cpp.ssl_enabled";
const std::string Configuration::SSL_PEM_FILE = "hedwig.cpp.ssl_pem";
const std::string Configuration::SUBSCRIPTION_CHANNEL_SHARING_ENABLED = "hedwig.cpp.subscription_channel_sharing_enabled";

Client::Client(const Configuration& conf) {
  LOG4CXX_DEBUG(logger, "Client::Client (" << this << ")");

  clientimpl = ClientImpl::Create( conf );
}

Subscriber& Client::getSubscriber() {
  return clientimpl->getSubscriber();
}

Publisher& Client::getPublisher() {
  return clientimpl->getPublisher();
}

Client::~Client() {
  LOG4CXX_DEBUG(logger, "Client::~Client (" << this << ")");

  clientimpl->Destroy();
}


