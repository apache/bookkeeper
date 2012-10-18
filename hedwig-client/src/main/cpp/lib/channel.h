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
#ifndef HEDWIG_CHANNEL_H
#define HEDWIG_CHANNEL_H

#include <hedwig/protocol.h>
#include <hedwig/callback.h>
#include <hedwig/client.h>
#include "util.h"
#include "data.h"
#include "eventdispatcher.h"

#ifdef USE_BOOST_TR1
#include <boost/tr1/memory.hpp>
#include <boost/tr1/unordered_map.hpp>
#else
#include <tr1/memory>
#include <tr1/unordered_map>
#endif

#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

#include <boost/asio.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>

namespace Hedwig {
  class ChannelException : public std::exception { };
  class UninitialisedChannelException : public ChannelException {};

  class ChannelConnectException : public ChannelException {};
  class CannotCreateSocketException : public ChannelConnectException {};
  class ChannelSetupException : public ChannelConnectException {};
  class ChannelNotConnectedException : public ChannelConnectException {};

  class ChannelDiedException : public ChannelException {};

  class ChannelWriteException : public ChannelException {};
  class ChannelReadException : public ChannelException {};
  class ChannelThreadException : public ChannelException {};
  class ChannelOutOfMemoryException : public ChannelException {};

  class InvalidSSLPermFileException : public std::exception {};

  class DuplexChannel;
  typedef boost::shared_ptr<DuplexChannel> DuplexChannelPtr;
  typedef boost::asio::ip::tcp::socket boost_socket;
  typedef boost::shared_ptr<boost_socket> boost_socket_ptr;
  typedef boost::asio::ssl::stream<boost_socket> boost_ssl_socket;
  typedef boost::shared_ptr<boost_ssl_socket> boost_ssl_socket_ptr;

  class ChannelHandler {
  public:
    virtual void messageReceived(const DuplexChannelPtr& channel, const PubSubResponsePtr& m) = 0;
    virtual void channelConnected(const DuplexChannelPtr& channel) = 0;

    virtual void channelDisconnected(const DuplexChannelPtr& channel, const std::exception& e) = 0;
    virtual void exceptionOccurred(const DuplexChannelPtr& channel, const std::exception& e) = 0;

    virtual ~ChannelHandler() {}
  };

  typedef boost::shared_ptr<ChannelHandler> ChannelHandlerPtr;

  // A channel interface to send requests
  class DuplexChannel {
  public:
    virtual ~DuplexChannel() {}

    // Return the channel handler bound with a channel
    virtual ChannelHandlerPtr getChannelHandler() = 0;

    // Issues a connect request to the target host
    // User could writeRequest after issued connect request, those requests should
    // be buffered and written until the channel is connected.
    virtual void connect() = 0;

    // Issues a connect request to the target host
    // User could writeRequest after issued connect request, those requests should
    // be buffered and written until the channel is connected.
    // The provided callback would be triggered after connected.
    virtual void connect(const OperationCallbackPtr& callback) = 0;

    // Write the request to underlying channel
    // If the channel is not established, all write requests would be buffered
    // until channel is connected.
    virtual void writeRequest(const PubSubRequestPtr& m,
                              const OperationCallbackPtr& callback) = 0; 

    // Returns the remote address where this channel is connected to.
    virtual const HostAddress& getHostAddress() const = 0;

    // Resumes the read operation of this channel asynchronously
    virtual void startReceiving() = 0;

    // Suspends the read operation of this channel asynchronously
    virtual void stopReceiving() = 0;

    // Returns if and only if the channel will read a message
    virtual bool isReceiving() = 0;

    //
    // Transaction operations
    //

    // Store a pub/sub request
    virtual void storeTransaction(const PubSubDataPtr& data) = 0;

    // Remove a pub/sub request
    virtual PubSubDataPtr retrieveTransaction(long txnid) = 0;

    // Fail all transactions
    virtual void failAllTransactions() = 0;

    // Handle the case that the channel is disconnected due issues found
    // when reading or writing
    virtual void channelDisconnected(const std::exception& e) = 0;

    // Close the channel to release the resources
    // Once a channel is closed, it can not be open again. Calling this
    // method on a closed channel has no efffect.
    virtual void close() = 0;
  };

  typedef boost::asio::ssl::context boost_ssl_context;
  typedef boost::shared_ptr<boost_ssl_context> boost_ssl_context_ptr;

  class SSLContextFactory {
  public:
    SSLContextFactory(const Configuration& conf);
    ~SSLContextFactory();

    boost_ssl_context_ptr createSSLContext(boost::asio::io_service& service);
  private:
    const Configuration& conf;
    std::string sslPemFile;
  };

  typedef boost::shared_ptr<SSLContextFactory> SSLContextFactoryPtr;

  class AbstractDuplexChannel;
  typedef boost::shared_ptr<AbstractDuplexChannel> AbstractDuplexChannelPtr;

  class AbstractDuplexChannel : public DuplexChannel,
                                public boost::enable_shared_from_this<AbstractDuplexChannel> {
  public:
    AbstractDuplexChannel(IOServicePtr& service,
                          const HostAddress& addr, 
                          const ChannelHandlerPtr& handler);
    virtual ~AbstractDuplexChannel();

    virtual ChannelHandlerPtr getChannelHandler();

    //
    // Connect Operation
    //

    // Asio Connect Callback Handler
    static void connectCallbackHandler(AbstractDuplexChannelPtr channel, 
                                       OperationCallbackPtr callback,
                                       const boost::system::error_code& error);
    virtual void connect();
    virtual void connect(const OperationCallbackPtr& callback);

    //
    // Write Operation
    //

    // Asio Write Callback Handler
    static void writeCallbackHandler(AbstractDuplexChannelPtr channel,
                                     OperationCallbackPtr callback, 
                                     const boost::system::error_code& error, 
                                     std::size_t bytes_transferred);
    // Write request
    virtual void writeRequest(const PubSubRequestPtr& m,
                              const OperationCallbackPtr& callback);

    // get the target host
    virtual const HostAddress& getHostAddress() const;

    static void sizeReadCallbackHandler(AbstractDuplexChannelPtr channel, 
                                        const boost::system::error_code& error, 
                                        std::size_t bytes_transferred);
    static void messageReadCallbackHandler(AbstractDuplexChannelPtr channel,
                                           std::size_t messagesize, 
                                           const boost::system::error_code& error, 
                                           std::size_t bytes_transferred);
    static void readSize(AbstractDuplexChannelPtr channel);

    // start receiving responses from underlying channel
    virtual void startReceiving();
    // is the underlying channel in receiving state
    virtual bool isReceiving();
    // stop receiving responses from underlying channel
    virtual void stopReceiving();

    // Store a pub/sub request
    virtual void storeTransaction(const PubSubDataPtr& data);

    // Remove a pub/sub request
    virtual PubSubDataPtr retrieveTransaction(long txnid);

    // Fail all transactions
    virtual void failAllTransactions();

    // channel is disconnected for a specified exception
    virtual void channelDisconnected(const std::exception& e);

    // close the channel
    virtual void close();

    inline boost::asio::io_service & getService() const {
      return service;
    }

  protected:
    // execute the connect operation
    virtual void doConnect(const OperationCallbackPtr& callback) = 0;

    virtual void doAfterConnect(const OperationCallbackPtr& callback,
                                const boost::system::error_code& error);

    // Execute the action after channel connect
    // It would be executed in asio connect callback handler
    virtual void setSocketOption(boost::system::error_code& ec) = 0;
    virtual boost::asio::ip::tcp::endpoint
            getRemoteAddress(boost::system::error_code& ec) = 0;
    virtual boost::asio::ip::tcp::endpoint
            getLocalAddress(boost::system::error_code& ec) = 0;

    // Channel failed to connect
    virtual void channelConnectFailed(const std::exception& e,
                                      const OperationCallbackPtr& callback);
    // Channel connected
    virtual void channelConnected(const OperationCallbackPtr& callback);

    // Start sending buffered requests to target host
    void startSending();

    // Write a buffer to underlying socket
    virtual void writeBuffer(boost::asio::streambuf& buffer,
                             const OperationCallbackPtr& callback) = 0;

    // Read a message from underlying socket
    virtual void readMsgSize(boost::asio::streambuf& buffer) = 0;
    virtual void readMsgBody(boost::asio::streambuf& buffer,
                             int toread, uint32_t msgSize) = 0;

    // is the channel under closing
    bool isClosed();

    // close the underlying socket to release resource 
    virtual void closeSocket() = 0;

    enum State { UNINITIALISED, CONNECTING, CONNECTED, DEAD };
    void setState(State s);

    // Address
    HostAddress address;
  private:
    ChannelHandlerPtr handler;

    boost::asio::io_service &service;

    // buffers for input stream
    boost::asio::streambuf in_buf;
    std::istream instream;

    // only exists because protobufs can't play nice with streams
    // (if there's more than message len in it, it tries to read all)
    char* copy_buf;
    std::size_t copy_buf_length;

    // buffers for output stream
    boost::asio::streambuf out_buf;
    // write requests queue 
    typedef std::pair<PubSubRequestPtr, OperationCallbackPtr> WriteRequest;
    boost::mutex write_lock;
    std::deque<WriteRequest> write_queue;

    // channel state
    State state;
    boost::shared_mutex state_lock;

    // reading state
    bool receiving;
    bool reading;
    PubSubResponsePtr outstanding_response;
    boost::mutex receiving_lock;

    // sending state
    bool sending;
    boost::mutex sending_lock;

    // flag indicates the channel is closed
    // some callback might return when closing
    bool closed;

    // transactions
    typedef std::tr1::unordered_map<long, PubSubDataPtr> TransactionMap;

    TransactionMap txnid2data;
    boost::mutex txnid2data_lock;
    boost::shared_mutex destruction_lock;
  };

  class AsioDuplexChannel : public AbstractDuplexChannel {
  public:
    AsioDuplexChannel(IOServicePtr& service,
                      const HostAddress& addr, 
                      const ChannelHandlerPtr& handler);
    virtual ~AsioDuplexChannel();
  protected:
    // execute the connect operation
    virtual void doConnect(const OperationCallbackPtr& callback);

    // Execute the action after channel connect
    // It would be executed in asio connect callback handler
    virtual void setSocketOption(boost::system::error_code& ec);
    virtual boost::asio::ip::tcp::endpoint
            getRemoteAddress(boost::system::error_code& ec);
    virtual boost::asio::ip::tcp::endpoint
            getLocalAddress(boost::system::error_code& ec);

    // Write a buffer to underlying socket
    virtual void writeBuffer(boost::asio::streambuf& buffer,
                             const OperationCallbackPtr& callback);

    // Read a message from underlying socket
    virtual void readMsgSize(boost::asio::streambuf& buffer);
    virtual void readMsgBody(boost::asio::streambuf& buffer,
                             int toread, uint32_t msgSize);

    // close the underlying socket to release resource 
    virtual void closeSocket();
  private:
    // underlying socket
    boost_socket_ptr socket;
  };

  typedef boost::shared_ptr<AsioDuplexChannel> AsioDuplexChannelPtr;

  class AsioSSLDuplexChannel;
  typedef boost::shared_ptr<AsioSSLDuplexChannel> AsioSSLDuplexChannelPtr;

  class AsioSSLDuplexChannel : public AbstractDuplexChannel {
  public:
    AsioSSLDuplexChannel(IOServicePtr& service,
                         const boost_ssl_context_ptr& sslCtx,
                         const HostAddress& addr, 
                         const ChannelHandlerPtr& handler);
    virtual ~AsioSSLDuplexChannel();
  protected:
    // execute the connect operation
    virtual void doConnect(const OperationCallbackPtr& callback);
    // Execute the action after channel connect
    // It would be executed in asio connect callback handler
    virtual void setSocketOption(boost::system::error_code& ec);
    virtual boost::asio::ip::tcp::endpoint
            getRemoteAddress(boost::system::error_code& ec);
    virtual boost::asio::ip::tcp::endpoint
            getLocalAddress(boost::system::error_code& ec);

    virtual void channelConnected(const OperationCallbackPtr& callback);

    // Start SSL Hand Shake after the channel is connected
    void startHandShake(const OperationCallbackPtr& callback);
    // Asio Callback After Hand Shake
    static void handleHandshake(AsioSSLDuplexChannelPtr channel,
                                OperationCallbackPtr callback,
                                const boost::system::error_code& error);

    void sslChannelConnected(const OperationCallbackPtr& callback);

    // Write a buffer to underlying socket
    virtual void writeBuffer(boost::asio::streambuf& buffer,
                             const OperationCallbackPtr& callback);

    // Read a message from underlying socket
    virtual void readMsgSize(boost::asio::streambuf& buffer);
    virtual void readMsgBody(boost::asio::streambuf& buffer,
                             int toread, uint32_t msgSize);

    // close the underlying socket to release resource 
    virtual void closeSocket();

  private:
#ifndef __APPLE__
    // Shutdown ssl
    void sslShutdown();
    // Handle ssl shutdown
    void handleSSLShutdown(const boost::system::error_code& error);
#endif
    // Close lowest layer
    void closeLowestLayer();

    // underlying ssl socket
    boost_ssl_socket_ptr ssl_socket;
    // ssl context
    boost_ssl_context_ptr ssl_ctx;

#ifndef __APPLE__
    // Flag indicated ssl is closed.
    bool sslclosed;
    boost::mutex sslclosed_lock;
    boost::condition_variable sslclosed_cond;
#endif
  };
  

  struct DuplexChannelPtrHash : public std::unary_function<DuplexChannelPtr, size_t> {
    size_t operator()(const Hedwig::DuplexChannelPtr& channel) const {
      return reinterpret_cast<size_t>(channel.get());
    }
  };
};
#endif
