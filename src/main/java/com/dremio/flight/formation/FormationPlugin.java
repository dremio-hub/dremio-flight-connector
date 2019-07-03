/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.flight.formation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.dfs.GenericCreateTableEntry;
import com.dremio.flight.AuthValidator;
import com.dremio.flight.PropertyHelper;
import com.dremio.flight.SslHelper;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import io.grpc.Status;

public class FormationPlugin implements StoragePlugin, MutablePlugin {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormationPlugin.class);

  public static final int FLIGHT_PORT = 47471;
  private final Location thisLocation;
  private final BufferAllocator allocator;
  private final SabotContext context;
  private final FlightServer server;
  //  private final KVStore<FlightStoreCreator.NodeKey, FlightStoreCreator.NodeKey> kvStore;
  private volatile List<FlightClient> clients = new ArrayList<>();
  private final Provider<StoragePluginId> pluginIdProvider;
  private final FormationFlightProducer producer;
  private final AuthValidator validator;


  public FormationPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    this.context = context;
    this.pluginIdProvider = pluginIdProvider;
    if (!Boolean.parseBoolean(PropertyHelper.getFromEnvProperty("dremio.flight.parallel.enabled", Boolean.toString(false)))) { //todo add this and others to DremioConfig
      allocator = null;
      thisLocation = null;
      server = null;
      producer = null;
      validator = null;
      logger.info("Parallel flight plugin is not enabled, skipping initialization");
      return;
    }

    this.allocator = context.getAllocator().newChildAllocator("formation-" + name, 0, Long.MAX_VALUE);
    String hostname = PropertyHelper.getFromEnvProperty("dremio.flight.host", context.getEndpoint().getAddress());
    int port = Integer.parseInt(PropertyHelper.getFromEnvProperty("dremio.flight.port", Integer.toString(FLIGHT_PORT)));
    FlightServer.Builder serverBuilder = FlightServer.builder().allocator(this.allocator);
    Pair<Location, FlightServer.Builder> pair = SslHelper.sslHelper(
      serverBuilder,
      context.getDremioConfig(),
      Boolean.parseBoolean(PropertyHelper.getFromEnvProperty("dremio.formation.use-ssl", "false")),
      context.getEndpoint().getAddress(),
      port,
      hostname);
    thisLocation = pair.getKey();
    this.producer = new FormationFlightProducer(thisLocation, allocator);
    this.validator = new AuthValidator(context.isUserAuthenticationEnabled() ? context.getUserService() : null, context);
    this.server = pair.getRight().producer(producer).authHandler(new BasicServerAuthHandler(validator)).build();
    logger.info("set up formation plugin on port {} and host {}", thisLocation.getUri().getPort(), thisLocation.getUri().getHost());
  }


  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) throws ConnectorException {
    if (datasetPath.size() != 2) {
      return Optional.empty();
    }
    try {
      return Optional.of(new DatasetBuilder(clients, datasetPath));
    } catch (Exception ex) {
      return Optional.empty();
    }
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options) throws ConnectorException {
    return null;
  }

  @Override
  public DatasetMetadata getDatasetMetadata(DatasetHandle datasetHandle, PartitionChunkListing chunkListing, GetMetadataOption... options) throws ConnectorException {
    return null;
  }

  @Override
  public boolean containerExists(EntityPath containerPath) {
    return true;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  @Override
  public SourceState getState() {
    return SourceState.GOOD;
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return FormationRulesFactory.class;
  }

  @Override
  public void start() throws IOException {
    if (server == null) {
      return;
    }
    server.start();
    context.getClusterCoordinator().getServiceSet(Role.EXECUTOR).addNodeStatusListener(new NodeStatusListener() {

      @Override
      public void nodesUnregistered(Set<NodeEndpoint> arg0) {
        refreshClients();
      }

      @Override
      public void nodesRegistered(Set<NodeEndpoint> arg0) {
        refreshClients();
      }
    });
    refreshClients();
  }

  private synchronized void refreshClients() {
    List<FlightClient> oldClients = clients;
    clients = context.getExecutors().stream()
      .map(e -> FlightClient.builder().allocator(allocator).location(Location.forGrpcInsecure(e.getAddress(), FLIGHT_PORT)).build()).collect(Collectors.toList());
    try {
      AutoCloseables.close(oldClients);
    } catch (Exception ex) {
      logger.error("Failure while refreshing clients.", ex);
    }
  }

  @Override
  public void close() throws Exception {
    if (server == null) {
      return;
    }
    AutoCloseables.close(clients, ImmutableList.of(server, allocator));
//    kvStore.delete(FlightStoreCreator.NodeKey.fromNodeEndpoint(context.getEndpoint()));
  }

  @Override
  public CreateTableEntry createNewTable(SchemaConfig schemaConfig, NamespaceKey namespaceKey, WriterOptions writerOptions, Map<String, Object> map) {
    if (namespaceKey.size() != 2) {
      throw UserException.unsupportedError().message("Formation plugin currently only supports single part names.").build(logger);
    }
    return new GenericCreateTableEntry(SystemUser.SYSTEM_USERNAME, this, namespaceKey.getLeaf(), writerOptions);
  }

  @Override
  public StoragePluginId getId() {
    return pluginIdProvider.get();
  }

  @Override
  public Writer getWriter(PhysicalOperator child, String location, WriterOptions options, OpProps props) throws IOException {
    if (server == null) {
      throw new UnsupportedOperationException();
    }
    return new FormationWriter(child, props, location, options, this);
  }

  @Override
  public void dropTable(List<String> list, SchemaConfig schemaConfig) {

  }

  @Override
  public boolean createOrUpdateView(NamespaceKey namespaceKey, View view, SchemaConfig schemaConfig) throws IOException {
    return false;
  }

  @Override
  public void dropView(SchemaConfig schemaConfig, List<String> list) throws IOException {

  }


  public FormationFlightProducer getProducer() {
    return producer;
  }

  public static class FormationFlightProducer implements FlightProducer {
    private final Location port;
    private final ConcurrentMap<FlightDescriptor, Stream> holders = new ConcurrentHashMap<>();
    private final BufferAllocator allocator;
    //    private final ExecutorService executor = Executors.newFixedThreadPool(5);
    private Map<Ticket, Future<?>> futures = Maps.newHashMap();

    public FormationFlightProducer(Location port, BufferAllocator allocator) {
      this.port = port;
      this.allocator = allocator;
    }

    private static FlightDescriptor getDescriptor(Ticket ticket) {
      String path = new String(ticket.getBytes());
      String[] pathParts = path.split(":");
      return FlightDescriptor.path(pathParts[0], pathParts[1], pathParts[2]);
    }

    @Override
    public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
      String ticketId = new String(ticket.getBytes());
      logger.debug("formation getStream for ticket {}", ticketId);
      FlightDescriptor descriptor = getDescriptor(ticket);
      boolean isIn = holders.containsKey(descriptor);
      Stream holder = holders.computeIfAbsent(descriptor, (t) -> new Stream(allocator, descriptor));
      logger.debug("generated holder for {}, was it created: {}. Now submitting job", ticketId, isIn);
//      Future<?> f = executor.submit(new DrainRunnable(holder.getConsumer(), ticketId, serverStreamListener));
      new DrainRunnable(holder.getConsumer(), ticketId, serverStreamListener).run();
      logger.debug("job for {} was submitted", ticketId);
//      futures.put(ticket, f);
    }

    @Override
    public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
      throw Status.UNIMPLEMENTED.asRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
      throw Status.UNIMPLEMENTED.asRuntimeException();
    }

    @Override
    public Runnable acceptPut(CallContext callContext, FlightStream flightStream, StreamListener<PutResult> streamListener) {
      throw Status.UNIMPLEMENTED.asRuntimeException();
    }

    @Override
    public void doAction(CallContext callContext, Action action, StreamListener<Result> resultStreamListener) {
      try {
        logger.debug("do action is called with action type {} and body {}", action.getType(), new String(action.getBody()));
        if ("create".equals(action.getType())) {
          logger.debug("create stream called, creasting ticket with {}", new String(action.getBody()));
          putStream(getDescriptor(new Ticket(action.getBody())), null);
          logger.debug("put stream succeeded for {}, returning result 0", new String(action.getBody()));
          resultStreamListener.onNext(new Result(new byte[]{0}));
        } else if ("delete".equals(action.getType())) {
          logger.debug("delete stream called, deleting ticket with {}", new String(action.getBody()));
          deleteStream(new Ticket(action.getBody()));
          logger.debug("delete stream succeeded for {}, returning result 0", new String(action.getBody()));
          resultStreamListener.onNext(new Result(new byte[]{0}));
        }
        resultStreamListener.onCompleted();
      } catch (Throwable e) {
        resultStreamListener.onError(e);
      }
    }

    private void deleteStream(Ticket ticket) {
      logger.debug("deleting stream {}", new String(ticket.getBytes()));
//      Future<?> f = futures.remove(ticket);
      Stream h = holders.get(getDescriptor(ticket));
      if (h != null) {
        logger.debug("sending cancel to stream {}", new String(ticket.getBytes()));
        if (h.cancel()) {
          futures.remove(ticket).cancel(true);
        }
      } else {
        logger.debug("stream does not exist! {}", new String(ticket.getBytes()));
      }
    }

    @Override
    public void listActions(CallContext callContext, StreamListener<ActionType> streamListener) {
      throw Status.UNIMPLEMENTED.asRuntimeException();
    }

    public Stream.Producer putStream(FlightDescriptor descriptor, Schema schema) {
      logger.debug("putting stream for descriptor {}", descriptor);
      boolean isIn = holders.containsKey(descriptor);
      Stream h = holders.computeIfAbsent(descriptor, (t) -> new Stream(allocator, descriptor));
      logger.debug("generated holder for {}, was it created: {}. Now setting schema", descriptor, isIn);
      if (schema != null) {
        logger.debug("schema is now set for {}", descriptor);
        h.setSchema(schema);
      } else {
        logger.debug("schema was not set for {}", descriptor);
      }
      return h.getProducer();
    }
  }

  private static class DrainRunnable implements Runnable {

    private final Stream.Consumer holder;
    private final String ticketId;
    private final FlightProducer.ServerStreamListener serverStreamListener;

    public DrainRunnable(Stream.Consumer holder, String ticketId, FlightProducer.ServerStreamListener serverStreamListener) {

      this.holder = holder;
      this.ticketId = ticketId;
      this.serverStreamListener = serverStreamListener;
    }

    @Override
    public void run() {
      logger.debug("running getstream runnable for ticket {}", ticketId);
      try {
        logger.debug("trying to start stream for {}", ticketId);
        holder.start(serverStreamListener);
        logger.debug("started stream for {}. Will try and drain", ticketId);
        while (!holder.drain()) {
          if (Thread.currentThread().isInterrupted()) {
            break;
          }
        }
      } catch (InterruptedException e) {
        logger.warn("interrupt, break and close stream for {}", ticketId);
      } catch (Throwable t) {
        logger.error("WTF!", t);
      } finally {
        logger.debug("closing stream and marking complete for {}", ticketId);
        if (holder != null) {
          holder.close();
        }
      }
      logger.debug("ending future for {}", ticketId);
    }
  }

}
