package com.linkedin.datastream.server;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.security.AclAware;
import com.linkedin.datastream.server.api.security.AclNamespaceProvider;
import com.linkedin.datastream.server.api.security.Authorizer;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;

import static com.linkedin.datastream.common.DatastreamStatus.READY;
import static com.linkedin.datastream.server.api.security.Authorizer.ACL_NAMESPACE;
import static com.linkedin.datastream.server.api.security.Authorizer.DESTINATION_ACL_KEY;
import static com.linkedin.datastream.server.api.security.Authorizer.SOURCE_ACL_KEY;


/**
 * ACLManager handles all ACL operations used in BrooklinServer. It sits between Datastream
 * REST endpoints (eg. create/delete) and Authorizer to:
 *
 * 1) authorize datastream CRUD requests (if supported by Connector)
 * 2) apply source ACLs onto destinations (if supported by Transport admins)
 * 3) store necessary ACL-related metadata in the datastreams
 * 4) synchronize source and destionation ACLs periodically
 *
 * Note that ACL sync is performed only if ACLManager is part of leader Coordinator.
 */
public class ACLManager implements Runnable, MetricsAware {
  private static final Logger LOG = LoggerFactory.getLogger(ACLManager.class);
  private static final DefaultAclNamespaceProvider DEFAULT_NAMESPACE_AWARE = new DefaultAclNamespaceProvider();
  private static final long ACL_SYNC_INIT_DELAY_MS = 30000;

  private final Map<String, AclAware> _srcAclAwareMap;
  private final Map<String, AclAware> _dstAclAwareMap;
  private final Map<String, AclNamespaceProvider> _namespaceProvider;

  // For ACL sync
  private final AtomicBoolean _syncStopped;
  private final AtomicBoolean _syncThreadSubmitted;
  private volatile Future<?> _syncThreadFuture;
  private final ScheduledExecutorService _executor;

  private final CachedDatastreamReader _datastreamReader;

  // Authorizer is set ad-hoc by Coordinator during startup
  private Optional<Authorizer> _authorizer = Optional.empty();

  public ACLManager(CachedDatastreamReader datastreamReader) {
    Validate.notNull(datastreamReader, "null datastreamReader");

    _datastreamReader = datastreamReader;

    _srcAclAwareMap = new HashMap<>();
    _dstAclAwareMap = new HashMap<>();
    _namespaceProvider = new HashMap<>();

    _executor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r);
      t.setDaemon(true);
      t.setName("ACLManager");
      t.setUncaughtExceptionHandler((thread, e) -> {
        LOG.error("ACLManager thread has died due to uncaught exception", e);
      });
      return t;
    });

    _syncStopped = new AtomicBoolean(true);
    _syncThreadSubmitted = new AtomicBoolean(false);
    _syncThreadFuture = null;
  }

  public void setAuthorizer(Authorizer authorizer) {
    Validate.notNull(authorizer, "null authorizer");
    _authorizer = Optional.of(authorizer);
  }

  public void addConnector(String name, Connector connector) {
    if (connector instanceof AclAware) {
      LOG.info("Adding connector {} for ACLing.", name);
      _srcAclAwareMap.put(name, (AclAware) connector);
    } else {
      LOG.warn("Connector {} does not support ACLing.", name);
    }
  }

  private static class DefaultAclNamespaceProvider implements AclNamespaceProvider {
    @Override
    public String getNamespace(Datastream datastream) {
      // Use transportProviderName as namespace
      return datastream.getTransportProviderName();
    }
  }

  public void addTransportProviderAdmin(String name, TransportProviderAdmin tpAdmin) {
    if (tpAdmin instanceof AclAware) {
      LOG.info("Adding transport provider {} for ACLing.", name);
      _dstAclAwareMap.put(name, (AclAware) tpAdmin);
    }

    // NOTE: expect transport provider be namespace provider
    if (tpAdmin instanceof AclNamespaceProvider) {
      LOG.info("Transport provider {} supports ACL namespace.", name);
      _namespaceProvider.put(name, (AclNamespaceProvider) tpAdmin);
    } else {
      LOG.info("Transport provider {} uses default namespace provider.", name);
      _namespaceProvider.put(name, DEFAULT_NAMESPACE_AWARE);
    }
  }

  // Store Authorizer mandated information directly in the datastream metadata
  // such that datastream is self-sufficient for all ACL operations.
  private void initializeAclMetadata(Datastream datastream) {
    String connectorName = datastream.getConnectorName();
    AclAware srcAclAware = _srcAclAwareMap.get(connectorName);

    String tpName = datastream.getTransportProviderName();
    AclNamespaceProvider nsProvider = _namespaceProvider.getOrDefault(tpName, null);
    if (nsProvider == null) {
      String errMsg = String.format("Unknown transport provider %s in datastream %s", tpName, datastream);
      LOG.error(errMsg);
      throw new DatastreamRuntimeException(errMsg);
    }

    // Set up source ACL key and namespace for authorizer
    datastream.getMetadata().put(SOURCE_ACL_KEY, srcAclAware.getAclKey(datastream));
    datastream.getMetadata().put(ACL_NAMESPACE, nsProvider.getNamespace(datastream));
  }

  public boolean hasSourceAccess(Datastream datastream) {
    Validate.notNull(datastream, "null datastream");

    if (!_authorizer.isPresent()) {
      LOG.debug("No authorizer configured and access is allowed.");
      return true;
    }

    String connectorName = datastream.getConnectorName();

    boolean accepted = true;
    if (_srcAclAwareMap.containsKey(connectorName)) {
      initializeAclMetadata(datastream);
      accepted = _authorizer.get().hasSourceAccess(datastream);
    }

    LOG.debug("hasSourceAccess: datastream={}, accepted={}", datastream, accepted);

    return accepted;
  }

  public void setDestinationAcl(Datastream datastream) {
    Validate.notNull(datastream, "null datastream");

    if (!_authorizer.isPresent()) {
      LOG.debug("No authorizer configured skipping setting destination ACL.");
      return;
    }

    String tpName = datastream.getTransportProviderName();
    Validate.notEmpty(tpName, "invalid TransportProviderName");

    if (_dstAclAwareMap.containsKey(tpName)) {
      AclAware dstAclAware = _dstAclAwareMap.get(tpName);
      datastream.getMetadata().put(DESTINATION_ACL_KEY, dstAclAware.getAclKey(datastream));

      _authorizer.get().setDestinationAcl(datastream);
    }
  }

  /**
   * Start ACL sync thread, this should be called when Coordinator becomes leader.
   */
  public void startAclSync() {
    if (!_authorizer.isPresent() || _authorizer.get().getSyncInterval().isPresent()) {
      LOG.info("No authorizer or sync interval is configured, skipping ACL sync.");
      return;
    }

    _syncStopped.set(false);

    if (_syncThreadSubmitted.compareAndSet(false, true)) {
      Duration interval = _authorizer.get().getSyncInterval().get();
      // Set initial delay to minimize race (benign) with the previous leader
      _syncThreadFuture = _executor.scheduleAtFixedRate(this, ACL_SYNC_INIT_DELAY_MS,
          interval.toMillis(), TimeUnit.MILLISECONDS);
      LOG.info("ACLManager sync thread is scheduled to run with interval " + interval);
    }
  }

  /**
   * Stop ACL sync thread, this should be called when Coordinator becomes follower.
   */
  public void stopAclSync() {
    _syncStopped.set(true);

    if (_syncThreadSubmitted.compareAndSet(true, false)) {
      _syncThreadFuture.cancel(false);
      _syncThreadFuture = null;
      LOG.info("ACLManager sync thread is stopped");
    }
  }

  @Override
  public void run() {
    if (_syncStopped.get()) {
      return;
    }

    LOG.info("Updating ACLs for all READY datastreams");
    for (Datastream stream : _datastreamReader.getAllDatastreams()) {
      try {
        if (_syncStopped.get()) {
          break;
        }

        // Only process fully initialized datastreams
        if (stream.getStatus() != READY) {
          continue;
        }

        LOG.debug("Updating ACL for datastream {}", stream);
        _authorizer.get().setDestinationAcl(stream);
      } catch (Exception e) {
        LOG.error("Updating ACL failed for datastream: " + stream, e);
      }
    }
  }
}
