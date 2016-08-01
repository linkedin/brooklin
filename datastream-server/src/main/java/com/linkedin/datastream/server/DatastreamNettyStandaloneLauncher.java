package com.linkedin.datastream.server;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.server.dms.DatastreamResourceFactory;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.http.server.HttpNettyServerFactory;
import com.linkedin.r2.transport.http.server.HttpServer;
import com.linkedin.restli.docgen.DefaultDocumentationRequestHandler;
import com.linkedin.restli.server.DelegatingTransportDispatcher;
import com.linkedin.restli.server.ErrorResponseFormat;
import com.linkedin.restli.server.RestLiConfig;
import com.linkedin.restli.server.RestLiServer;


/**
 * Datastream specific netty standalone launcher which uses the DatastreamResourceFactory for instantiating
 * the datastream restli resources.
 */
public class DatastreamNettyStandaloneLauncher {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamNettyStandaloneLauncher.class.getName());

  private final int _port;
  private final int _threadPoolSize;
  private final int _parseqThreadPoolSize;
  private final Collection<String> _packages;
  private final HttpServer _server;

  public DatastreamNettyStandaloneLauncher(int httpPort, DatastreamResourceFactory resourceFactory) {
    this(httpPort, HttpNettyServerFactory.DEFAULT_THREAD_POOL_SIZE, Runtime.getRuntime().availableProcessors() + 1,
        resourceFactory);
  }

  public DatastreamNettyStandaloneLauncher(int port, int threadPoolSize, int parseqThreadPoolSize,
      DatastreamResourceFactory resourceFactory) {
    _port = port;
    _threadPoolSize = threadPoolSize;
    _parseqThreadPoolSize = parseqThreadPoolSize;
    _packages = resourceFactory.getRestPackages();

    final RestLiConfig config = new RestLiConfig();
    config.setDocumentationRequestHandler(new DefaultDocumentationRequestHandler());
    config.setServerNodeUri(URI.create("/"));
    config.addResourcePackageNames(_packages);
    config.setErrorResponseFormat(ErrorResponseFormat.FULL);
    LOG.info("Restli resource packages: " + _packages);

    LOG.info("Netty parseqThreadPoolSize: " + parseqThreadPoolSize);
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(parseqThreadPoolSize);
    final Engine engine = new EngineBuilder()
        .setTaskExecutor(scheduler)
        .setTimerScheduler(scheduler)
        .build();

    final RestLiServer restServer = new RestLiServer(config, resourceFactory, engine);
    final TransportDispatcher dispatcher = new DelegatingTransportDispatcher(restServer);
    LOG.info("Netty threadPoolSize: " + threadPoolSize);
    _server = new HttpNettyServerFactory(FilterChains.empty()).createServer(_port, threadPoolSize, dispatcher);
  }

  /**
   * Start the server
   *
   * @throws java.io.IOException server startup fails
   */
  public void start() throws IOException {
    _server.start();
  }

  /**
   * Stop the server
   *
   * @throws IOException server shutdown fails
   */
  public void stop() throws IOException {
    _server.stop();
  }
}
