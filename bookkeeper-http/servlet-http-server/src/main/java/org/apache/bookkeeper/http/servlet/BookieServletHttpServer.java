package org.apache.bookkeeper.http.servlet;

import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServiceProvider;

/**
 * Only use for hold Http service provider,not a fully implement bookie http service server
 **/
public class BookieServletHttpServer implements HttpServer {
  private static HttpServiceProvider bookieHttpServiceProvider;
  private static int listenPort=-1;

  public static HttpServiceProvider getBookieHttpServiceProvider(){
    return bookieHttpServiceProvider;
  }
  /**
   * Listen  port
   **/
  public static int getListenPort(){
    return listenPort;
  }
  @Override
  public void initialize(HttpServiceProvider httpServiceProvider) {
    this.bookieHttpServiceProvider=httpServiceProvider;
  }

  @Override
  public boolean startServer(int port) {
    listenPort=port;
    return true;
  }

  @Override
  public void stopServer() {

  }

  @Override
  public boolean isRunning() {
    return true;
  }
}
