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

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <iostream>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <vector>
#include <utility>
#include <deque>
#include "channel.h"
#include "util.h"
#include "clientimpl.h"

#include <log4cxx/logger.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("hedwig."__FILE__));

using namespace Hedwig;

const std::string DEFAULT_SSL_PEM_FILE = "";

AbstractDuplexChannel::AbstractDuplexChannel(IOServicePtr& service,
                                             const HostAddress& addr, 
                                             const ChannelHandlerPtr& handler)
  : address(addr), handler(handler), service(service->getService()),
    instream(&in_buf), copy_buf(NULL), copy_buf_length(0),
    state(UNINITIALISED), receiving(false), reading(false), sending(false),
    closed(false)
{}

AbstractDuplexChannel::~AbstractDuplexChannel() {
  free(copy_buf);
  copy_buf = NULL;
  copy_buf_length = 0;

  LOG4CXX_INFO(logger, "Destroying DuplexChannel(" << this << ")");
}

ChannelHandlerPtr AbstractDuplexChannel::getChannelHandler() {
  return handler;
}

/*static*/ void AbstractDuplexChannel::connectCallbackHandler(
                  AbstractDuplexChannelPtr channel,
                  OperationCallbackPtr callback,
                  const boost::system::error_code& error) {
  channel->doAfterConnect(callback, error);
}

void AbstractDuplexChannel::connect() {
  connect(OperationCallbackPtr());
}

void AbstractDuplexChannel::connect(const OperationCallbackPtr& callback) {  
  setState(CONNECTING);
  doConnect(callback);
}

void AbstractDuplexChannel::doAfterConnect(const OperationCallbackPtr& callback,
                                           const boost::system::error_code& error) {
  if (error) {
    LOG4CXX_ERROR(logger, "Channel " << this << " connect error : " << error.message().c_str());
    channelConnectFailed(ChannelConnectException(), callback);
    return;
  }

  // set no delay option
  boost::system::error_code ec;
  setSocketOption(ec);
  if (ec) {
    LOG4CXX_WARN(logger, "Channel " << this << " set up socket error : " << ec.message().c_str());
    channelConnectFailed(ChannelSetupException(), callback);
    return;
  } 

  boost::asio::ip::tcp::endpoint localEp;
  boost::asio::ip::tcp::endpoint remoteEp;
  localEp = getLocalAddress(ec);
  remoteEp = getRemoteAddress(ec);

  if (!ec) {
    LOG4CXX_INFO(logger, "Channel " << this << " connected :"
                         << localEp.address().to_string() << ":" << localEp.port() << "=>"
                         << remoteEp.address().to_string() << ":" << remoteEp.port());
    // update ip address since if might connect to VIP
    address.updateIP(remoteEp.address().to_v4().to_ulong());
  }
  // the channel is connected
  channelConnected(callback);
}

void AbstractDuplexChannel::channelConnectFailed(const std::exception& e,
                                                 const OperationCallbackPtr& callback) {
  channelDisconnected(e);
  setState(DEAD);
  if (callback.get()) {
    callback->operationFailed(e);
  }
}

void AbstractDuplexChannel::channelConnected(const OperationCallbackPtr& callback) {
  // for normal channel, we have done here
  setState(CONNECTED);
  if (callback.get()) {
    callback->operationComplete();
  }

  // enable sending & receiving
  startSending();
  startReceiving();
}

/*static*/ void AbstractDuplexChannel::messageReadCallbackHandler(
                AbstractDuplexChannelPtr channel, 
                std::size_t message_size,
                const boost::system::error_code& error, 
                std::size_t bytes_transferred) {
  LOG4CXX_DEBUG(logger, "DuplexChannel::messageReadCallbackHandler " << error << ", " 
                        << bytes_transferred << " channel(" << channel.get() << ")");

  if (error) {
    if (!channel->isClosed()) {
      LOG4CXX_INFO(logger, "Invalid read error (" << error << ") bytes_transferred (" 
                           << bytes_transferred << ") channel(" << channel.get() << ")");
    }
    channel->channelDisconnected(ChannelReadException());
    return;
  }

  if (channel->copy_buf_length < message_size) {
    channel->copy_buf_length = message_size;
    channel->copy_buf = (char*)realloc(channel->copy_buf, channel->copy_buf_length);
    if (channel->copy_buf == NULL) {
      LOG4CXX_ERROR(logger, "Error allocating buffer. channel(" << channel.get() << ")");
      // if failed to realloc memory, we should disconnect the channel.
      // then it would enter disconnect logic, which would close channel and release
      // its resources includes the copy_buf memory.
      channel->channelDisconnected(ChannelOutOfMemoryException());
      return;
    }
  }
  
  channel->instream.read(channel->copy_buf, message_size);
  PubSubResponsePtr response(new PubSubResponse());
  bool err = response->ParseFromArray(channel->copy_buf, message_size);

  if (!err) {
    LOG4CXX_ERROR(logger, "Error parsing message. channel(" << channel.get() << ")");
    channel->channelDisconnected(ChannelReadException());
    return;
  } else {
    LOG4CXX_DEBUG(logger,  "channel(" << channel.get() << ") : " << channel->in_buf.size() 
                           << " bytes left in buffer");
  }

  ChannelHandlerPtr h;
  {
    boost::shared_lock<boost::shared_mutex> lock(channel->destruction_lock);
    if (channel->handler.get()) {
      h = channel->handler;
    }
  }

  // channel did stopReceiving, we should not call #messageReceived
  // store this response in outstanding_response variable and did stop receiving
  // when we startReceiving again, we can process this last response.
  {
    boost::lock_guard<boost::mutex> lock(channel->receiving_lock);
    if (!channel->isReceiving()) {
      // queue the response
      channel->outstanding_response = response;
      channel->reading = false;
      return;
    }
  }

  // channel is still in receiving status
  if (h.get()) {
    h->messageReceived(channel, response);
  }

  AbstractDuplexChannel::readSize(channel);
}

/*static*/ void AbstractDuplexChannel::sizeReadCallbackHandler(
                   AbstractDuplexChannelPtr channel, 
                   const boost::system::error_code& error, 
                   std::size_t bytes_transferred) {
  LOG4CXX_DEBUG(logger, "DuplexChannel::sizeReadCallbackHandler " << error << ", " 
                        << bytes_transferred << " channel(" << channel.get() << ")");

  if (error) {
    if (!channel->isClosed()) {
      LOG4CXX_INFO(logger, "Invalid read error (" << error << ") bytes_transferred (" 
                           << bytes_transferred << ") channel(" << channel.get() << ")");
    }
    channel->channelDisconnected(ChannelReadException());
    return;
  }
  
  if (channel->in_buf.size() < sizeof(uint32_t)) {
    LOG4CXX_ERROR(logger, "Not enough data in stream. Must have been an error reading. " 
                          << " Closing channel(" << channel.get() << ")");
    channel->channelDisconnected(ChannelReadException());
    return;
  }

  uint32_t size;
  std::istream is(&channel->in_buf);
  is.read((char*)&size, sizeof(uint32_t));
  size = ntohl(size);

  int toread = size - channel->in_buf.size();
  LOG4CXX_DEBUG(logger, " size of incoming message " << size << ", currently in buffer " 
                        << channel->in_buf.size() << " channel(" << channel.get() << ")");
  if (toread <= 0) {
    AbstractDuplexChannel::messageReadCallbackHandler(channel, size, error, 0);
  } else {
    channel->readMsgBody(channel->in_buf, toread, size);
  }
}

/*static*/ void AbstractDuplexChannel::readSize(AbstractDuplexChannelPtr channel) {
  int toread = sizeof(uint32_t) - channel->in_buf.size();
  LOG4CXX_DEBUG(logger, " size of incoming message " << sizeof(uint32_t) 
                        << ", currently in buffer " << channel->in_buf.size() 
                        << " channel(" << channel.get() << ")");

  if (toread < 0) {
    AbstractDuplexChannel::sizeReadCallbackHandler(channel, boost::system::error_code(), 0);
  } else {
    channel->readMsgSize(channel->in_buf);
  }
}

void AbstractDuplexChannel::startReceiving() {
  LOG4CXX_DEBUG(logger, "DuplexChannel::startReceiving channel(" << this
                        << ") currently receiving = " << receiving);

  PubSubResponsePtr response;
  bool inReadingState;
  {
    boost::lock_guard<boost::mutex> lock(receiving_lock);
    // receiving before just return
    if (receiving) {
      return;
    } 
    receiving = true;

    // if we have last response collected in previous startReceiving
    // we need to process it, but we should process it under receiving_lock
    // otherwise we enter dead lock
    // subscriber#startDelivery(subscriber#queue_lock) =>
    // channel#startReceiving(channel#receiving_lock) =>
    // sbuscriber#messageReceived(subscriber#queue_lock)
    if (outstanding_response.get()) {
      response = outstanding_response;
      outstanding_response = PubSubResponsePtr();
    }

    // if channel is in reading status wait data from remote server
    // we don't need to insert another readSize op
    inReadingState = reading;
    if (!reading) {
      reading = true;
    }
  }

  // consume message buffered in receiving queue
  // there is at most one message buffered when we
  // stopReceiving between #readSize and #readMsgBody
  if (response.get()) {
    ChannelHandlerPtr h;
    {
      boost::shared_lock<boost::shared_mutex> lock(this->destruction_lock);
      if (this->handler.get()) {
        h = this->handler;
      }
    }
    if (h.get()) {
      h->messageReceived(shared_from_this(), response);
    }
  }

  // if channel is not in reading state, #readSize
  if (!inReadingState) {
    AbstractDuplexChannel::readSize(shared_from_this());
  }
}

bool AbstractDuplexChannel::isReceiving() {
  return receiving;
}

bool AbstractDuplexChannel::isClosed() {
  return closed;
}

void AbstractDuplexChannel::stopReceiving() {
  LOG4CXX_DEBUG(logger, "DuplexChannel::stopReceiving channel(" << this << ")");
  
  boost::lock_guard<boost::mutex> lock(receiving_lock);
  receiving = false;
}

void AbstractDuplexChannel::startSending() {
  {
    boost::shared_lock<boost::shared_mutex> lock(state_lock);
    if (state != CONNECTED) {
      return;
    }
  }

  boost::lock_guard<boost::mutex> lock(sending_lock);
  if (sending) {
    return;
  }
  LOG4CXX_DEBUG(logger, "AbstractDuplexChannel::startSending channel(" << this << ")");
  
  WriteRequest w;
  { 
    boost::lock_guard<boost::mutex> lock(write_lock);
    if (write_queue.empty()) {
      return;
    }
    w = write_queue.front();
    write_queue.pop_front();
  }

  sending = true;

  std::ostream os(&out_buf);
  uint32_t size = htonl(w.first->ByteSize());
  os.write((char*)&size, sizeof(uint32_t));
  
  bool err = w.first->SerializeToOstream(&os);
  if (!err) {
    w.second->operationFailed(ChannelWriteException());
    channelDisconnected(ChannelWriteException());
    return;
  }

  writeBuffer(out_buf, w.second);
}

const HostAddress& AbstractDuplexChannel::getHostAddress() const {
  return address;
}

void AbstractDuplexChannel::channelDisconnected(const std::exception& e) {
  setState(DEAD);
  
  {
    boost::lock_guard<boost::mutex> lock(write_lock);
    while (!write_queue.empty()) {
      WriteRequest w = write_queue.front();
      write_queue.pop_front();
      w.second->operationFailed(e);
    }
  }

  ChannelHandlerPtr h;
  {
    boost::shared_lock<boost::shared_mutex> lock(destruction_lock);
    if (handler.get()) {
      h = handler;
    }
  }
  if (h.get()) {
    h->channelDisconnected(shared_from_this(), e);
  }
}

void AbstractDuplexChannel::close() {
  {
    boost::shared_lock<boost::shared_mutex> statelock(state_lock);
    state = DEAD;
  }

  {
    boost::lock_guard<boost::shared_mutex> lock(destruction_lock);
    if (closed) {
      // some one has closed the socket.
      return;
    }
    closed = true;
    handler = ChannelHandlerPtr(); // clear the handler in case it ever referenced the channel*/
  }

  LOG4CXX_INFO(logger, "Killing duplex channel (" << this << ")");

  // If we are going away, fail all transactions that haven't been completed
  failAllTransactions();
  closeSocket();  
}

/*static*/ void AbstractDuplexChannel::writeCallbackHandler(
                  AbstractDuplexChannelPtr channel,
                  OperationCallbackPtr callback,
                  const boost::system::error_code& error, 
                  std::size_t bytes_transferred) {
  if (error) {
    if (!channel->isClosed()) {
      LOG4CXX_DEBUG(logger, "AbstractDuplexChannel::writeCallbackHandler " << error << ", " 
                            << bytes_transferred << " channel(" << channel.get() << ")");
    }
    callback->operationFailed(ChannelWriteException());
    channel->channelDisconnected(ChannelWriteException());
    return;
  }

  callback->operationComplete();

  channel->out_buf.consume(bytes_transferred);

  {
    boost::lock_guard<boost::mutex> lock(channel->sending_lock);
    channel->sending = false;
  }

  channel->startSending();
}

void AbstractDuplexChannel::writeRequest(const PubSubRequestPtr& m,
                                         const OperationCallbackPtr& callback) {
  {
    boost::shared_lock<boost::shared_mutex> lock(state_lock);
    if (state != CONNECTED && state != CONNECTING) {
      LOG4CXX_ERROR(logger,"Tried to write transaction [" << m->txnid() << "] to a channel [" 
                           << this << "] which is " << (state == DEAD ? "DEAD" : "UNINITIALISED"));
      callback->operationFailed(UninitialisedChannelException());
      return;
    }
  }

  { 
    boost::lock_guard<boost::mutex> lock(write_lock);
    WriteRequest w(m, callback);
    write_queue.push_back(w);
  }

  startSending();
}

//
// Transaction operations
//

/**
   Store the transaction data for a request.
*/
void AbstractDuplexChannel::storeTransaction(const PubSubDataPtr& data) {
  LOG4CXX_DEBUG(logger, "Storing txnid(" << data->getTxnId() << ") for channel(" << this << ")");

  boost::lock_guard<boost::mutex> lock(txnid2data_lock);
  txnid2data[data->getTxnId()] = data;
}

/**
   Give the transaction back to the caller. 
*/
PubSubDataPtr AbstractDuplexChannel::retrieveTransaction(long txnid) {
  boost::lock_guard<boost::mutex> lock(txnid2data_lock);

  PubSubDataPtr data = txnid2data[txnid];
  txnid2data.erase(txnid);
  if (data == NULL) {
    LOG4CXX_ERROR(logger, "Transaction txnid(" << txnid 
                          << ") doesn't exist in channel (" << this << ")");
  }

  return data;
}

void AbstractDuplexChannel::failAllTransactions() {
  boost::lock_guard<boost::mutex> lock(txnid2data_lock);
  for (TransactionMap::iterator iter = txnid2data.begin(); iter != txnid2data.end(); ++iter) {
    PubSubDataPtr& data = (*iter).second;
    data->getCallback()->operationFailed(ChannelDiedException());
  }
  txnid2data.clear();
}

// Set state for the channel
void AbstractDuplexChannel::setState(State s) {
  boost::lock_guard<boost::shared_mutex> lock(state_lock);
  state = s;
}

//
// Basic Asio Channel Implementation
//

AsioDuplexChannel::AsioDuplexChannel(IOServicePtr& service,
                                     const HostAddress& addr, 
                                     const ChannelHandlerPtr& handler)
  : AbstractDuplexChannel(service, addr, handler) {
  this->socket = boost_socket_ptr(new boost_socket(getService()));
  LOG4CXX_DEBUG(logger, "Creating DuplexChannel(" << this << ")");
}

AsioDuplexChannel::~AsioDuplexChannel() {
}

void AsioDuplexChannel::doConnect(const OperationCallbackPtr& callback) {
  boost::system::error_code error = boost::asio::error::host_not_found;
  uint32_t ip2conn = address.ip();
  uint16_t port2conn = address.port();
  boost::asio::ip::tcp::endpoint endp(boost::asio::ip::address_v4(ip2conn), port2conn);

  socket->async_connect(endp, boost::bind(&AbstractDuplexChannel::connectCallbackHandler, 
                                          shared_from_this(), callback,
                                          boost::asio::placeholders::error));
  LOG4CXX_INFO(logger, "Channel (" << this << ") fire connect operation to ip (" 
                                   << ip2conn << ") port (" << port2conn << ")");
}

void AsioDuplexChannel::setSocketOption(boost::system::error_code& ec) {
  boost::asio::ip::tcp::no_delay option(true);
  socket->set_option(option, ec);
}

boost::asio::ip::tcp::endpoint AsioDuplexChannel::getLocalAddress(
    boost::system::error_code& ec) {
  return socket->local_endpoint(ec);
}

boost::asio::ip::tcp::endpoint AsioDuplexChannel::getRemoteAddress(
    boost::system::error_code& ec) {
  return socket->remote_endpoint(ec);
}

void AsioDuplexChannel::writeBuffer(boost::asio::streambuf& buffer,
                                    const OperationCallbackPtr& callback) {
  boost::asio::async_write(*socket, buffer,
    boost::bind(&AbstractDuplexChannel::writeCallbackHandler, 
    shared_from_this(), callback,
    boost::asio::placeholders::error, 
    boost::asio::placeholders::bytes_transferred)
  );
}

void AsioDuplexChannel::readMsgSize(boost::asio::streambuf& buffer) {
  boost::asio::async_read(*socket, buffer, boost::asio::transfer_at_least(sizeof(uint32_t)),
                          boost::bind(&AbstractDuplexChannel::sizeReadCallbackHandler,
                                      shared_from_this(),
                                      boost::asio::placeholders::error,
                                      boost::asio::placeholders::bytes_transferred));
}

void AsioDuplexChannel::readMsgBody(boost::asio::streambuf& buffer,
                                    int toread, uint32_t msgSize) {
  boost::asio::async_read(*socket, buffer, boost::asio::transfer_at_least(toread),
                          boost::bind(&AbstractDuplexChannel::messageReadCallbackHandler,
                                      shared_from_this(), msgSize,
                                      boost::asio::placeholders::error,
                                      boost::asio::placeholders::bytes_transferred));
}

void AsioDuplexChannel::closeSocket() {
  boost::system::error_code ec;

  socket->cancel(ec);
  if (ec) {
    LOG4CXX_WARN(logger, "Channel " << this << " canceling io error : " << ec.message().c_str());
  }

  socket->shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
  if (ec) {
    LOG4CXX_WARN(logger, "Channel " << this << " shutdown error : " << ec.message().c_str());
  }

  socket->close(ec);
  if (ec) {
    LOG4CXX_WARN(logger, "Channel " << this << " close error : " << ec.message().c_str());
  }
  LOG4CXX_DEBUG(logger, "Closed socket for channel " << this << ".");
}

// SSL Context Factory

SSLContextFactory::SSLContextFactory(const Configuration& conf)
  : conf(conf),
    sslPemFile(conf.get(Configuration::SSL_PEM_FILE,
                        DEFAULT_SSL_PEM_FILE)) {
}

SSLContextFactory::~SSLContextFactory() {}

boost_ssl_context_ptr SSLContextFactory::createSSLContext(boost::asio::io_service& service) {
  boost_ssl_context_ptr sslCtx(new boost_ssl_context(service,
                               boost::asio::ssl::context::sslv23_client));
  sslCtx->set_verify_mode(boost::asio::ssl::context::verify_none);
  if (!sslPemFile.empty()) {
    boost::system::error_code err;
    sslCtx->load_verify_file(sslPemFile, err);

    if (err) {
      LOG4CXX_ERROR(logger, "Failed to load verify ssl pem file : "
                            << sslPemFile);
      throw InvalidSSLPermFileException();
    }
  }
  return sslCtx;
}

//
// SSL Channl Implementation
//

#ifndef __APPLE__
AsioSSLDuplexChannel::AsioSSLDuplexChannel(IOServicePtr& service,
                                           const boost_ssl_context_ptr& sslCtx,
                                           const HostAddress& addr,
                                           const ChannelHandlerPtr& handler)
  : AbstractDuplexChannel(service, addr, handler), ssl_ctx(sslCtx),
    sslclosed(false) {
#else
AsioSSLDuplexChannel::AsioSSLDuplexChannel(IOServicePtr& service,
                                           const boost_ssl_context_ptr& sslCtx,
                                           const HostAddress& addr,
                                           const ChannelHandlerPtr& handler)
  : AbstractDuplexChannel(service, addr, handler), ssl_ctx(sslCtx) {
#endif
  ssl_socket = boost_ssl_socket_ptr(new boost_ssl_socket(getService(), *ssl_ctx));
  LOG4CXX_DEBUG(logger, "Created SSL DuplexChannel(" << this << ")");
}

AsioSSLDuplexChannel::~AsioSSLDuplexChannel() {
}

void AsioSSLDuplexChannel::doConnect(const OperationCallbackPtr& callback) {
  boost::system::error_code error = boost::asio::error::host_not_found;
  uint32_t ip2conn = address.ip();
  uint16_t port2conn = address.sslPort();
  boost::asio::ip::tcp::endpoint endp(boost::asio::ip::address_v4(ip2conn), port2conn);

  ssl_socket->lowest_layer().async_connect(endp,
      boost::bind(&AbstractDuplexChannel::connectCallbackHandler, 
                  shared_from_this(), callback,
                  boost::asio::placeholders::error));
  LOG4CXX_INFO(logger, "SSL Channel (" << this << ") fire connect operation to ip (" 
                                       << ip2conn << ") port (" << port2conn << ")");
}

void AsioSSLDuplexChannel::setSocketOption(boost::system::error_code& ec) {
  boost::asio::ip::tcp::no_delay option(true);
  ssl_socket->lowest_layer().set_option(option, ec);
}

boost::asio::ip::tcp::endpoint AsioSSLDuplexChannel::getLocalAddress(
    boost::system::error_code& ec) {
  return ssl_socket->lowest_layer().local_endpoint(ec);
}

boost::asio::ip::tcp::endpoint AsioSSLDuplexChannel::getRemoteAddress(
    boost::system::error_code& ec) {
  return ssl_socket->lowest_layer().remote_endpoint(ec);
}

void AsioSSLDuplexChannel::channelConnected(const OperationCallbackPtr& callback) {
  // for SSL channel, we had to do SSL hand shake
  startHandShake(callback);
  LOG4CXX_INFO(logger, "SSL Channel " << this << " fire hand shake operation");
}

void AsioSSLDuplexChannel::sslChannelConnected(const OperationCallbackPtr& callback) {
  LOG4CXX_INFO(logger, "SSL Channel " << this << " hand shake finish!!");
  AbstractDuplexChannel::channelConnected(callback);
}

void AsioSSLDuplexChannel::startHandShake(const OperationCallbackPtr& callback) {
  ssl_socket->async_handshake(boost::asio::ssl::stream_base::client,
                              boost::bind(&AsioSSLDuplexChannel::handleHandshake,
                                          boost::dynamic_pointer_cast<AsioSSLDuplexChannel>(shared_from_this()),
                                          callback, boost::asio::placeholders::error));
}

void AsioSSLDuplexChannel::handleHandshake(AsioSSLDuplexChannelPtr channel,
                                           OperationCallbackPtr callback,
                                           const boost::system::error_code& error) {
  if (error) {
    LOG4CXX_ERROR(logger, "SSL Channel " << channel.get() << " hand shake error : "
                          << error.message().c_str());
    channel->channelConnectFailed(ChannelConnectException(), callback);
    return;
  }
  channel->sslChannelConnected(callback);
}

void AsioSSLDuplexChannel::writeBuffer(boost::asio::streambuf& buffer,
                                       const OperationCallbackPtr& callback) {
  boost::asio::async_write(*ssl_socket, buffer,
    boost::bind(&AbstractDuplexChannel::writeCallbackHandler, 
    shared_from_this(), callback,
    boost::asio::placeholders::error, 
    boost::asio::placeholders::bytes_transferred)
  );
}

void AsioSSLDuplexChannel::readMsgSize(boost::asio::streambuf& buffer) {
  boost::asio::async_read(*ssl_socket, buffer, boost::asio::transfer_at_least(sizeof(uint32_t)),
                          boost::bind(&AbstractDuplexChannel::sizeReadCallbackHandler, 
                                      shared_from_this(),
                                      boost::asio::placeholders::error, 
                                      boost::asio::placeholders::bytes_transferred));
}

void AsioSSLDuplexChannel::readMsgBody(boost::asio::streambuf& buffer,
                                       int toread, uint32_t msgSize) {
  boost::asio::async_read(*ssl_socket, buffer, boost::asio::transfer_at_least(toread),
                          boost::bind(&AbstractDuplexChannel::messageReadCallbackHandler, 
                                      shared_from_this(), msgSize,
                                      boost::asio::placeholders::error, 
                                      boost::asio::placeholders::bytes_transferred));
}

#ifndef __APPLE__
// boost asio doesn't provide time out mechanism to shutdown ssl
void AsioSSLDuplexChannel::sslShutdown() {
  ssl_socket->async_shutdown(boost::bind(&AsioSSLDuplexChannel::handleSSLShutdown,
                                         boost::shared_dynamic_cast<AsioSSLDuplexChannel>(shared_from_this()),
                                         boost::asio::placeholders::error));
}

void AsioSSLDuplexChannel::handleSSLShutdown(const boost::system::error_code& error) {
  if (error) {
    LOG4CXX_ERROR(logger, "SSL Channel " << this << " shutdown error : "
                          << error.message().c_str());
  }
  {
    boost::lock_guard<boost::mutex> lock(sslclosed_lock);
    sslclosed = true;
  }
  sslclosed_cond.notify_all();
}
#endif

void AsioSSLDuplexChannel::closeSocket() {
#ifndef __APPLE__
  // Shutdown ssl
  sslShutdown();
  // time wait 
  {
    boost::mutex::scoped_lock lock(sslclosed_lock);
    if (!sslclosed) {
      sslclosed_cond.timed_wait(lock, boost::posix_time::milliseconds(1000)); 
    }
  }
#endif
  closeLowestLayer();
}

void AsioSSLDuplexChannel::closeLowestLayer() {
  boost::system::error_code ec;

  ssl_socket->lowest_layer().cancel(ec);
  if (ec) {
    LOG4CXX_WARN(logger, "Channel " << this << " canceling io error : " << ec.message().c_str());
  }

  ssl_socket->lowest_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
  if (ec) {
    LOG4CXX_WARN(logger, "Channel " << this << " shutdown error : " << ec.message().c_str());
  }

  ssl_socket->lowest_layer().close(ec);
  if (ec) {
    LOG4CXX_WARN(logger, "Channel " << this << " close error : " << ec.message().c_str());
  }
}
