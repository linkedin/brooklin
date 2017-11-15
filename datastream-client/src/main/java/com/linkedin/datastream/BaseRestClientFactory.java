package com.linkedin.datastream;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.RestliUtils;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
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
  private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

  private final Logger _logger;
  private final Class<T> _restClientClass;
  private final Map<String, T> _overrides = new ConcurrentHashMap<>();
  private final Map<String, RestClient> _restClients = new ConcurrentHashMap<>();
  private HttpClientFactory _httpClientFactory = new HttpClientFactory();

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
   * @return
   */
  public synchronized T getClient(String uri, Map<String, String> httpConfig) {
    T client = getOverride(uri);
    if (client == null) {
      client = createClient(getRestClient(uri, httpConfig));
    }
    return client;
  }

  /**
   * Register an existing R2 RestClient already bound to {@param uri}.
   * It is cached and reused for future Rest.li client wrapper creations without custom HTTP parameters.
   * @param uri URI to the HTTP endpoint
   * @param restClient custom R2 RestClient
   * @return
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

  /**
   * This must be called to allow JVM to shutdown cleanly. This is necessary because
   * R2 HttpClientFactory creates non-daemon threads for RestClient. Default shutdown
   * timeout is 5000ms (see: {@link HttpClientFactory#DEFAULT_SHUTDOWN_TIMEOUT}. For
   * HttpClientFactory shutdown, we use @param timeout.
   * @param callback callback to receive shutdown status
   * @param timeout timeout wait for two times of this duration before giving up
   */
  public synchronized void shutdown(Callback<None> callback, Duration timeout) {
    if (_httpClientFactory == null) {
      return;
    }

    if (!_restClients.isEmpty()) {
      // We are not interested in individual client shutdown status
      // given R2 client already has sufficient logging for such.
      FutureCallback<None> noopCallback = new FutureCallback<>();

      _logger.info("Initiating asynchronous shutdown of all RestClients ...");
      for (RestClient restClient : _restClients.values()) {
        restClient.shutdown(noopCallback);
      }
      _restClients.clear();
      _logger.info("All RestClients are shutdown successfully ...");
    }

    final CountDownLatch shutdownLatch = new CountDownLatch(1);

    // HttpClientFactory actually reference counts all RestClients managed by itself.
    // Once the count reaches zero, the factory self shuts down as such below code is
    // most likely be an noop unless some RestClients failed to shutdown above.
    _httpClientFactory.shutdown(new Callback<None>() {
      @Override
      public void onSuccess(None none) {
        shutdownLatch.countDown();
        _logger.info("Shutdown complete");
        if (callback != null) {
          callback.onSuccess(none);
        }
      }

      @Override
      public void onError(Throwable e) {
        shutdownLatch.countDown();
        _logger.error("Error during shutdown", e);
        if (callback != null) {
          callback.onError(e);
        }
      }
    }, timeout.toMillis(), TimeUnit.MILLISECONDS);

    try {
      shutdownLatch.await(timeout.toMillis() * 2, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      _logger.error("HttpClientFactory failed to shutdown properly.", e);
    } finally {
      // Always nullify the factory as we do not know the state of the
      // factory in case if fails to properly shutdown.
      _httpClientFactory = null;
    }
  }

  private T getOverride(String uri) {
    uri = RestliUtils.sanitizeUri(uri);
    return _overrides.getOrDefault(uri, null);
  }

  private static String getKey(String uri, Map<String, String> httpConfig) {
    return String.format("%s-%d", uri, httpConfig.hashCode());
  }

  private RestClient getRestClient(String uri, Map<String, String> httpConfig) {
    String canonicalUri = RestliUtils.sanitizeUri(uri);
    RestClient restClient;
    String key = getKey(uri, httpConfig);
    restClient = _restClients.computeIfAbsent(key, (k) ->
      new RestClient(new TransportClientAdapter(_httpClientFactory.getClient(httpConfig)), canonicalUri));
    return restClient;
  }

  private T createClient(RestClient restClient) {
    T client = ReflectionUtils.createInstance(_restClientClass, restClient);
    if (client == null) {
      throw new DatastreamRuntimeException("Failed to instantiate " + _restClientClass);
    }
    return client;
  }
}
