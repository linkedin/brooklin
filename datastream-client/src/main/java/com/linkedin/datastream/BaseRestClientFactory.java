package com.linkedin.datastream;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;

import io.netty.channel.nio.NioEventLoopGroup;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.RestliUtils;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.util.NamedThreadFactory;
import com.linkedin.restli.client.RestClient;


/**
 * Base class for implementing a factory for Rest.li client wrappers, eg. DatastreamRestClient.
 * We cache R2 RestClient by (URI, HTTP_PARAMS) because they are stateless. All custom factories
 * share the HttpClientFactory. R2 RestClient is the actual HTTP client used by wrappers to send
 * the actual requests and receive responses.
 *
 * Rest.li wrappers can be created in two ways:
 * 1) use default R2 RestClient either previously registered or created through HttpClientFactory.
 * 2) use a cached or new RestClient created with the specified http properties through HttpClientFactory.
 *
 * The registered R2 RestClient is mapped with (URI, EMPTY_MAP) in the cache so it will not be used
 * when caller specify non-empty HTTP_PARAMS.
 *
 * Rest.li client wrapper class is expected to provide a constructor with a R2 RestClient as the
 * only argument.
 *
 * Caller can specify a custom Rest.li client wrapper instance per URI which will be used whenever
 * wrapper is requested for such URI. This has the highest precedence and is useful for plugging
 * in mocked wrappers for unit/integration tests.
 *
 * Lastly, there is no need to shutdown the factory or rest client during runtime because they are
 * essentially singleton per (URI, HTTP_PARAMS). As such, they might be reused for future wrapper
 * creations so we need to keep them alive. When application shuts down, all resources will be
 * released by the JVM.
 *
 * @param <T> type of the specific Rest.li client wrapper (eg. DatastreamRestClient).
 */
public final class BaseRestClientFactory<T> {
  private final Logger _logger;
  private final Class<T> _restClientClass;
  private final Map<String, T> _overrides = new ConcurrentHashMap<>();
  private final Map<String, RestClient> _restClients = new ConcurrentHashMap<>();

  // Created lazily
  private HttpClientFactory _httpClientFactory;

  /**
   * @param clazz Class object of the Rest.li client wrapper
   */
  public BaseRestClientFactory(Class<T> clazz, Logger logger) {
    _restClientClass = clazz;
    _logger = logger;
  }

  /**
   * Get a rest client wrapper with custom HTTP configs
   * @param uri URI to the HTTP endpoint
   * @param httpConfig custom config for HTTP client, please find the configs in
   *                   {@link com.linkedin.r2.transport.http.client.HttpClientFactory}
   * @return instance of the BaseRestClient
   */
  public synchronized T getClient(String uri, Map<String, String> httpConfig) {
    T client = getOverride(uri);
    if (client == null) {
      client = createClient(getRestClient(uri, httpConfig));
    }
    return client;
  }

  /**
   * Get a rest client wrapper with custom HTTP configs
   * @param uri URI to the HTTP endpoint
   * @param httpConfig custom config for HTTP client, please find the configs in
   *                   {@link com.linkedin.r2.transport.http.client.HttpClientFactory}
   * @param clientConfig custom config for the DatastreamRestClient
   * @return instance of the BaseRestClient which takes in clientConfig as a parameter in the constructor
   */
  public synchronized T getClient(String uri, Map<String, String> httpConfig, Properties clientConfig) {
    T client = getOverride(uri);
    if (client == null) {
      client = createClient(getRestClient(uri, httpConfig), clientConfig);
    }
    return client;
  }

  /**
   * Register an existing R2 RestClient already bound to {@param uri}.
   * It is cached and reused for future Rest.li client wrapper creations without custom HTTP parameters.
   * @param uri URI to the HTTP endpoint
   * @param restClient custom R2 RestClient
   */
  public synchronized void registerRestClient(String uri, RestClient restClient) {
    uri = RestliUtils.sanitizeUri(uri);
    _restClients.put(getKey(uri, Collections.emptyMap()), restClient);
  }

  /**
   * Used mainly for testing, to override the rest client wrapper returned for a given URI.
   * As custom factories are used in the static context, each test case should use its own
   * URI, to avoid conflicts in case the test are running in parallel.
   */
  public synchronized void addOverride(String uri, T restliClient) {
    Validate.notNull(restliClient, "null RestClient");
    uri = RestliUtils.sanitizeUri(uri);
    if (_overrides.containsKey(uri)) {
      _logger.warn("Replacing existing RestliClient override for URI: " + uri);
    }
    _overrides.put(uri, restliClient);
  }

  private T getOverride(String uri) {
    uri = RestliUtils.sanitizeUri(uri);
    return _overrides.getOrDefault(uri, null);
  }

  private static String getKey(String uri, Map<String, String> httpConfig) {
    return String.format("%s-%d", uri, httpConfig.hashCode());
  }

  /**
   * Special ThreadFactory to be used with HttpClientFactory:
   *  - add brooklin prefix to the threads
   *  - create daemon threads to prevent jvm unclean shutdown
   *
   * By default, HttpClientFactory creates deamon threads with NamedThreadFactory.
   * Such threads will block JVM from shutting down when they are not fully stopped.
   * We do not have nor plan to support use cases where we need to access DMS during
   * shutdown, hence it is okay for us to make the threads daemon.
   */
  private class DaemonNamedThreadFactory extends NamedThreadFactory {
    public DaemonNamedThreadFactory(String name) {
      super(name + " " + StringUtils.substringAfterLast(_logger.getName(), "."));
    }

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = super.newThread(runnable);
      thread.setDaemon(true);
      return thread;
    }
  }

  private void initHttpClientFactory() {
    ThreadFactory eventLoopThreadFactory = new DaemonNamedThreadFactory("R2 Nio Event Loop");
    ThreadFactory schedExecThreadFactory = new DaemonNamedThreadFactory("R2 Netty Scheduler");
    _httpClientFactory = new HttpClientFactory.Builder()
        .setNioEventLoopGroup(new NioEventLoopGroup(0, eventLoopThreadFactory))
        .setScheduleExecutorService(Executors.newSingleThreadScheduledExecutor(schedExecThreadFactory))
        .build();
  }

  private RestClient getRestClient(String uri, Map<String, String> httpConfig) {
    String canonicalUri = RestliUtils.sanitizeUri(uri);
    RestClient restClient;
    String key = getKey(uri, httpConfig);
    restClient = _restClients.computeIfAbsent(key, (k) -> {
      if (_httpClientFactory == null) {
        initHttpClientFactory();
      }

      _logger.info("Creating RestClient for {} with {}, count={}", canonicalUri, httpConfig, _restClients.size() + 1);
      return new RestClient(new TransportClientAdapter(_httpClientFactory.getClient(httpConfig)), canonicalUri);
    });
    return restClient;
  }

  private T createClient(RestClient restClient) {
    T client = ReflectionUtils.createInstance(_restClientClass, restClient);
    if (client == null) {
      throw new DatastreamRuntimeException("Failed to instantiate " + _restClientClass);
    }
    return client;
  }

  private T createClient(RestClient restClient, Properties config) {
    T client = ReflectionUtils.createInstance(_restClientClass, restClient, config);
    if (client == null) {
      throw new DatastreamRuntimeException("Failed to instantiate " + _restClientClass);
    }
    return client;
  }
}
