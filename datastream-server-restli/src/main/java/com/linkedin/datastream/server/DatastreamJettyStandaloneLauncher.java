package com.linkedin.datastream.server;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import com.linkedin.restli.docgen.DefaultDocumentationRequestHandler;
import com.linkedin.restli.server.DelegatingTransportDispatcher;
import com.linkedin.restli.server.RestLiConfig;
import com.linkedin.restli.server.RestLiServer;
import com.linkedin.restli.server.resources.ResourceFactory;


/**
 * Datastream specific netty standalone launcher which uses the DatastreamResourceFactory for instantiating
 * the datastream restli resources.
 */
public class DatastreamJettyStandaloneLauncher {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamJettyStandaloneLauncher.class.getName());

  private final String[] _packages;
  private final HttpJettyServer _httpJettyServer;
  private int _port;

  public DatastreamJettyStandaloneLauncher(int httpPort, ResourceFactory resourceFactory, String... packages) {
    this(httpPort, Runtime.getRuntime().availableProcessors() + 1, resourceFactory, packages);
  }

  public DatastreamJettyStandaloneLauncher(int port, int parseqThreadPoolSize, ResourceFactory resourceFactory,
      String... packages) {
    _port = port;
    _packages = packages;

    final RestLiConfig config = new RestLiConfig();
    config.setDocumentationRequestHandler(new DefaultDocumentationRequestHandler());
    config.setServerNodeUri(URI.create("/"));
    config.addResourcePackageNames(_packages);

    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(parseqThreadPoolSize);
    final Engine engine = new EngineBuilder()
        .setTaskExecutor(scheduler)
        .setTimerScheduler(scheduler)
        .build();

    final RestLiServer restServer = new RestLiServer(config, resourceFactory, engine);
    final TransportDispatcher dispatcher = new DelegatingTransportDispatcher(restServer, restServer);

    _httpJettyServer = (HttpJettyServer) new HttpServerFactory(FilterChains.empty()).createServer(port, dispatcher);

  }

  /**
   * Start the server
   *
   * @throws java.io.IOException server startup fails
   */
  public void start() throws Exception {
    _httpJettyServer.start();

    // Get the actual port if no port (== 0) is specified
    if (_port == 0) {
      // Must gain access to Jetty server instance to get the port we just bound to
      Server jettyServer = ReflectionUtils.getField(_httpJettyServer, "_server");
      if (jettyServer == null) {
        throw new DatastreamRuntimeException("Failed to obtain Jetty server from HttpJettyServer via reflection.");
      }

      // Get the real port after binding
      _port = jettyServer.getURI().getPort();
    }

    LOG.info("Embedded Jetty started with port: " + _port);
  }

  /**
   * Stop the server
   *
   * @throws java.io.IOException server shutdown fails
   */
  public void stop() throws Exception {
    _httpJettyServer.stop();
  }

  public int getPort() {
    return _port;
  }
}
