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
#ifndef PUBLISHER_IMPL_H
#define PUBLISHER_IMPL_H

#include <hedwig/publish.h>
#include <hedwig/callback.h>
#include "clientimpl.h"
#include "data.h"

namespace Hedwig {
  class PublishResponseAdaptor : public ResponseCallback {
  public:
    PublishResponseAdaptor(const PublishResponseCallbackPtr& pubCallback);

    void operationComplete(const ResponseBody & result);
    void operationFailed(const std::exception& exception);
  private:
    PublishResponseCallbackPtr pubCallback;
  };

  class PublishResponseHandler : public ResponseHandler {
  public:
    PublishResponseHandler(const DuplexChannelManagerPtr& channelManager);
    virtual ~PublishResponseHandler() {};

    virtual void handleResponse(const PubSubResponsePtr& m, const PubSubDataPtr& txn,
                                const DuplexChannelPtr& channel);
  };

  class PublisherImpl : public Publisher {
  public:
    PublisherImpl(const DuplexChannelManagerPtr& channelManager);

    PublishResponsePtr publish(const std::string& topic, const std::string& message);
    PublishResponsePtr publish(const std::string& topic, const Message& message);

    void asyncPublish(const std::string& topic, const std::string& message, const OperationCallbackPtr& callback);
    void asyncPublish(const std::string& topic, const Message& message, const OperationCallbackPtr& callback);
    void asyncPublishWithResponse(const std::string& topic, const Message& messsage,
                                  const PublishResponseCallbackPtr& callback);

    void doPublish(const std::string& topic, const Message& message,
                   const ResponseCallbackPtr& callback);
  private:
    DuplexChannelManagerPtr channelManager;
  };

};

#endif
