package org.apache.bookkeeper.http.servlet;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.List;
import javax.servlet.Servlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * Jetty based http server
 **/

public class JettyHttpServer {

  private Server jettyServer;
  private ContextHandlerCollection contexts;

  public JettyHttpServer(String host,int port){
     this.jettyServer= new Server(new InetSocketAddress(host, port));
     this.contexts= new ContextHandlerCollection();
     this.jettyServer.setHandler(contexts);
  }
  /**
   * Add servlet
   **/
  public void addServlet(String webApp, String contextPath,String pathSpec, List<Servlet> servlets) throws IOException{
    if(servlets==null){
      return;
    }
    File bookieApi = new File(webApp);
    if (!bookieApi.isDirectory()) {
      Files.createDirectories(bookieApi.toPath());
    }
    WebAppContext webAppBookie = new WebAppContext(bookieApi.getAbsolutePath(), contextPath);
    for(Servlet s:servlets) {
      webAppBookie.addServlet(new ServletHolder(s), pathSpec);
    }
    contexts.addHandler(webAppBookie);
  }

  /**
   * Start jetty server
   **/
  public void startServer() throws Exception{
       jettyServer.start();
  }

  /**
   * Stop jetty server
   **/
  public void stopServer() throws Exception{
       jettyServer.stop();
  }
}
