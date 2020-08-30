package org.apache.bookkeeper.http.servlet;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.Servlet;
import org.apache.bookkeeper.http.NullHttpServiceProvider;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBookieHttpServiceServlet {

  private JettyHttpServer jettyHttpServer;
  private String host="localhost";
  int port=8080;
  private BookieServletHttpServer bookieServletHttpServer;
  @Before
  public void setUp() throws Exception {
    this.bookieServletHttpServer=new BookieServletHttpServer();
    this.bookieServletHttpServer.initialize(new NullHttpServiceProvider());
    this.jettyHttpServer=new JettyHttpServer(host,port);
    List<Servlet> servlets=new ArrayList<>();
    servlets.add(new BookieHttpServiceServlet());
    jettyHttpServer.addServlet("web/bookie","/","/", servlets);
    jettyHttpServer.startServer();
  }

  @Test
  public void testBookieHeartBeat() throws URISyntaxException,IOException {
    assertThat(IOUtils.toString(new URI(String.format("http://%s:%d/heartbeat",host,port)), "UTF-8"), containsString("OK"));
  }

  @After
  public void stop() throws Exception{
    jettyHttpServer.stopServer();
  }
}
