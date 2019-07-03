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
package com.dremio.flight;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Provider;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.fragment.Wrapper;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.proto.UserBitShared.RecordBatchDef;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementReq;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementResp;
import com.dremio.exec.proto.UserProtos.PreparedStatement;
import com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.dremio.exec.proto.UserProtos.ResultColumnMetadata;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.exec.proto.beans.CoreOperatorType;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.work.foreman.TerminationListenerRegistry;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.flight.formation.FormationPlugin;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;
import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBufUtil;

class Producer implements FlightProducer, AutoCloseable {
  private static final Joiner JOINER = Joiner.on(":");
  private static final Logger logger = LoggerFactory.getLogger(Producer.class);
  private final Location location;
  private final Provider<UserWorker> worker;
  private final Provider<SabotContext> context;
  private final BufferAllocator allocator;
  private final AuthValidator validator;
//  private final KVStore<FlightStoreCreator.NodeKey, FlightStoreCreator.NodeKey> kvStore;

  Producer(Location location, Provider<UserWorker> worker, Provider<SabotContext> context, BufferAllocator allocator, AuthValidator validator) {
    super();
    this.location = location;
    this.worker = worker;
    this.context = context;
    this.allocator = allocator;
    this.validator = validator;
//    kvStore = context.get().getKVStoreProvider().getStore(FlightStoreCreator.class);
  }

  @Override
  public void doAction(CallContext context, Action action, StreamListener<Result> resultStreamListener) {
    if (action.getType().equals("PARALLEL")) {
      AuthValidator.FlightSessionOptions options = validator.getSessionOptions(context);
      options.setParallel(true);
      resultStreamListener.onNext(new Result("ok".getBytes()));
      resultStreamListener.onCompleted();
      return;
    }
    if (action.getType().equals("NO_PARALLEL")) {
      AuthValidator.FlightSessionOptions options = validator.getSessionOptions(context);
      options.setParallel(false);
      resultStreamListener.onNext(new Result("ok".getBytes()));
      resultStreamListener.onCompleted();
      return;
    }
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  private FlightInfo getCoalesce(CallContext callContext, FlightDescriptor descriptor, Ticket cmd) {
    PrepareParallel d = new PrepareParallel();
    logger.debug("coalescing query {}", new String(cmd.getBytes()));
    RunQuery query;
    try {
      PreparedStatementHandle handle = PreparedStatementHandle.parseFrom(cmd.getBytes());

      query = RunQuery.newBuilder()
        .setType(QueryType.PREPARED_STATEMENT)
        .setSource(UserProtos.SubmissionSource.UNKNOWN_SOURCE)
        .setPreparedStatementHandle(handle)
        .setPriority(UserProtos.QueryPriority
          .newBuilder()
          .setWorkloadClass(UserBitShared.WorkloadClass.GENERAL)
          .setWorkloadType(UserBitShared.WorkloadType.UNKNOWN)
          .build())
        .build();
    } catch (InvalidProtocolBufferException e) {
      throw Status.UNKNOWN.withCause(e).asRuntimeException();
    }

    UserRequest request = new UserRequest(RpcType.RUN_QUERY, query);
    UserBitShared.ExternalId externalId = submitWork(callContext, request, d);
    String queryId = QueryIdHelper.getQueryId(ExternalIdHelper.toQueryId(externalId));
    logger.debug("submitted query for {} and got query id {}. Will now wait for parallel planner to return.", new String(cmd.getBytes()), queryId);
    return d.getInfo(descriptor, queryId);
  }

  private FlightInfo getInfoParallel(CallContext callContext, FlightDescriptor descriptor, String sql) {
    String queryId = QueryIdHelper.getQueryId(ExternalIdHelper.toQueryId(ExternalIdHelper.generateExternalId()));
    sql = String.format("create table flight.\"%s\" as (%s)", queryId, sql);
    logger.debug("Submitting ctas {}", sql);
    FlightInfo ticket = getInfoImpl(callContext, descriptor, sql);
    return getCoalesce(callContext, descriptor, ticket.getEndpoints().get(0).getTicket());
  }


  private FlightInfo getInfoImpl(CallContext callContext, FlightDescriptor descriptor, String sql) {
    try {
      final CreatePreparedStatementReq req =
        CreatePreparedStatementReq.newBuilder()
          .setSqlQuery(sql)
          .build();

      UserRequest request = new UserRequest(RpcType.CREATE_PREPARED_STATEMENT, req);
      Prepare prepare = new Prepare();

      UserBitShared.ExternalId externalId = submitWork(callContext, request, prepare);
      return prepare.getInfo(descriptor, externalId);
    } catch (Exception e) {
      throw Status.ABORTED.asRuntimeException();
    }
  }

  @Override
  public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
    FlightInfo info = getInfoImpl(context, descriptor, new String(descriptor.getCommand()));
    return new SchemaResult(info.getSchema());
  }

  @Override
  public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor descriptor) {
    logger.debug("checking if parallel query");
    boolean isParallel = validator.getSessionOptions(callContext).isParallel();
    logger.debug("checking if parallel query: result {}", isParallel);
    if (isParallel) {
      return getInfoParallel(callContext, descriptor, new String(descriptor.getCommand()));
    }
    return getInfoImpl(callContext, descriptor, new String(descriptor.getCommand()));
  }

  @Override
  public Runnable acceptPut(CallContext callContext, FlightStream flightStream, StreamListener<org.apache.arrow.flight.PutResult> streamListener) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  private UserBitShared.ExternalId submitWork(CallContext callContext, UserRequest request, UserResponseHandler handler) {
    UserBitShared.ExternalId externalId = ExternalIdHelper.generateExternalId();
    worker.get().submitWork(
      externalId,
      validator.getUserSession(callContext),
      handler,
      request,
      TerminationListenerRegistry.NOOP);
    logger.debug("Submitted job {} from flight for request with type {}", ExternalIdHelper.toQueryId(externalId), request.getType());
    return externalId;
  }

  @Override
  public void close() throws Exception {
    allocator.close();
  }

  private class Prepare implements UserResponseHandler {

    private final CompletableFuture<CreatePreparedStatementResp> future = new CompletableFuture<>();

    public Prepare() {
    }

    public FlightInfo getInfo(FlightDescriptor descriptor, UserBitShared.ExternalId externalId) {
      try {
        logger.debug("Waiting for prepared statement handle to return for job id {}", ExternalIdHelper.toQueryId(externalId));
        CreatePreparedStatementResp handle = future.get();
        logger.debug("prepared statement handle for job id {} has returned", ExternalIdHelper.toQueryId(externalId));
        if (handle.getStatus() == RequestStatus.FAILED) {
          logger.warn("prepared statement handle for job id " + ExternalIdHelper.toQueryId(externalId) + " has failed", UserRemoteException.create(handle.getError()));
          throw Status.INTERNAL.withDescription(handle.getError().getMessage()).withCause(UserRemoteException.create(handle.getError())).asRuntimeException();
        }
        logger.debug("prepared statement handle for job id {} has succeeded", ExternalIdHelper.toQueryId(externalId));
        PreparedStatement statement = handle.getPreparedStatement();
        Ticket ticket = new Ticket(statement.getServerHandle().toByteArray());
        FlightEndpoint endpoint = new FlightEndpoint(ticket, location);
        logger.debug("flight endpoint for job id {} has been created with ticket {}", ExternalIdHelper.toQueryId(externalId), new String(ticket.getBytes()));
        Schema schema = fromMetadata(statement.getColumnsList());
        FlightInfo info = new FlightInfo(schema, descriptor, Lists.newArrayList(endpoint), -1L, -1L);
        logger.debug("flight info for job id {} has been created with schema {}", ExternalIdHelper.toQueryId(externalId), schema.toJson());
        return info;
      } catch (Exception e) {
        logger.warn("prepared statement handle for job id " + ExternalIdHelper.toQueryId(externalId) + " has failed", UserException.parseError(e).buildSilently());
        throw Status.UNKNOWN.withCause(UserException.parseError(e).buildSilently()).asRuntimeException();
      }
    }

    private Schema fromMetadata(List<ResultColumnMetadata> rcmd) {

      Schema schema = new Schema(rcmd.stream().map(md -> {
        ArrowType arrowType = SqlTypeNameToArrowType.toArrowType(md);
        FieldType fieldType = new FieldType(md.getIsNullable(), arrowType, null, null);
        return new Field(md.getColumnName(), fieldType, null);
      }).collect(Collectors.toList()));
      return schema;
    }

    @Override
    public void sendData(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
      throw Status.UNIMPLEMENTED.asRuntimeException();
    }

    @Override
    public void completed(UserResult result) {
      if (result.getState() == QueryState.FAILED) {
        future.completeExceptionally(result.getException());
      } else if (result.getState() == QueryState.CANCELED) {
        future.completeExceptionally(result.getException());
      } else {
        future.complete(result.unwrap(CreatePreparedStatementResp.class));
      }
    }

  }

  private class PrepareParallel implements UserResponseHandler {

    private final CompletableFuture<List<FlightEndpoint>> future = new CompletableFuture<>();
    private List<FlightEndpoint> endpoints;
    private String queryId = "unknown";


    public PrepareParallel() {
    }

    public FlightInfo getInfo(FlightDescriptor descriptor, String queryId) {
      this.queryId = queryId;
      try {
        List<FlightEndpoint> endpointsFromFuture = future.get();
        logger.debug("got endpoints future back for queryid {} with {} endpoints", queryId, endpointsFromFuture.size());
        endpoints = endpointsFromFuture.stream()
          .map(e -> {
            String ticketId = JOINER.join(queryId, new String(e.getTicket().getBytes()));
            Ticket ticket = new Ticket(ticketId.getBytes());
            return new FlightEndpoint(ticket, e.getLocations().toArray(new Location[0]));
          }).collect(Collectors.toList());
        FlightInfo info = new FlightInfo(new Schema(Lists.newArrayList()), descriptor, endpoints, -1L, -1L);
        logger.debug("returning new flight info with a fake schema and {} data endpoints", endpoints.size());
        return info;
      } catch (InterruptedException | ExecutionException e) {
        throw Status.INTERNAL.asRuntimeException();
      }
    }

    @Override
    public void sendData(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
      logger.debug("send data is called on parallel prepare for query id {}", queryId);
//      if (!future.isDone()) {
//        future.complete(ImmutableList.of());
//      }
      outcomeListener.success(Acks.OK, null);
    }

    @Override
    public void completed(UserResult result) {
      logger.debug("running completed method for queryid {}. Will try and run delete action now.", queryId);
      if (result.getState() == QueryState.FAILED) {
        throw Status.ABORTED.withCause(result.getException()).asRuntimeException();
      }
      if (result.getState() == QueryState.CANCELED) {
        throw Status.CANCELLED.withDescription(result.getCancelReason()).asRuntimeException();
      }
    }

    @Override
    public void planParallelized(PlanningSet planningSet) {
      logger.debug("plan parallel called, collecting endpoints");
      List<FlightEndpoint> endpoints = Lists.newArrayList();
      FlightEndpoint screenEndpoint = null;
      for (Wrapper wrapper : planningSet.getFragmentWrapperMap().values()) {
        String majorId = String.valueOf(wrapper.getMajorFragmentId());
        try {
          Boolean isWriter = wrapper.getNode().getRoot().accept(new WriterVisitor(), false);
          if (!isWriter) {
            continue;
          }
        } catch (Throwable throwable) {
          logger.warn("unable to complete visitor ", throwable);
        }


        CoreOperatorType op = CoreOperatorType.valueOf(wrapper.getNode().getRoot().getOperatorType());
        boolean isScreen = CoreOperatorType.SCREEN.equals(op) && planningSet.getFragmentWrapperMap().size() > 1;

        logger.info("Creating tickets for {}. MajorId {}", wrapper.getNode().getRoot(), majorId);
        for (int i = 0; i < wrapper.getAssignedEndpoints().size(); i++) {
          CoordinationProtos.NodeEndpoint endpoint = wrapper.getAssignedEndpoint(i);
          logger.warn("Creating ticket for {} . MajorId {} and index {}", wrapper.getNode().getRoot(), majorId, i);
          Ticket ticket = new Ticket(JOINER.join(
            majorId,
            String.valueOf(i),
            endpoint.getAddress(),
            endpoint.getUserPort()
          ).getBytes());
          int port = Integer.parseInt(PropertyHelper.getFromEnvProperty("dremio.formation.port", Integer.toString(FormationPlugin.FLIGHT_PORT)));
          String host = PropertyHelper.getFromEnvProperty("dremio.formation.host", endpoint.getAddress());
          Location location = Location.forGrpcInsecure(host, port);

          if (isScreen) {
            screenEndpoint = new FlightEndpoint(ticket, location);
          } else {
            endpoints.add(new FlightEndpoint(ticket, location));
          }
        }
      }
      if (screenEndpoint != null && endpoints.isEmpty()) {
        logger.info("Adding a screen endpoint as its the only possible endpoint!");
        endpoints.add(screenEndpoint);
      } else {
        logger.warn("Skipping a screen as it lies above the writer.");
      }
      logger.debug("built {} parallel endpoints", endpoints.size());
      future.complete(endpoints);
    }
  }

  private class RetrieveData implements UserResponseHandler {

    private final ServerStreamListener listener;
    private RecordBatchLoader loader;
    private volatile VectorSchemaRoot root;

    RetrieveData(ServerStreamListener listener) {
      this.listener = listener;
    }

    @Override
    public void sendData(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
      try {
        RecordBatchDef def = result.getHeader().getDef();
        if (loader == null) {
          loader = new RecordBatchLoader(allocator);
        }

        // consolidate
        try (ArrowBuf buf = allocator.buffer((int) result.getByteCount())) {
          Stream.of(result.getBuffers()).forEach(b -> {
            buf.writeBytes(ByteBufUtil.getBytes(b));
            b.release();
          });
          loader.load(def, buf);
        }

        if (root == null) {
          List<FieldVector> vectors = StreamSupport.stream(loader.spliterator(), false).map(v -> (FieldVector) v.getValueVector()).collect(Collectors.toList());
          root = new VectorSchemaRoot(vectors);
          listener.start(root);
        }

        root.setRowCount(result.getHeader().getRowCount());
        listener.putNext();
        outcomeListener.success(Acks.OK, null);
      } catch (Exception ex) {
        listener.error(Status.UNKNOWN.withCause(ex).withDescription(ex.getMessage()).asException());
      }
    }

    @Override
    public void completed(UserResult result) {
      if (result.getState() == QueryState.FAILED) {
        listener.error(Status.ABORTED.withCause(result.getException()).asRuntimeException());
      } else if (result.getState() == QueryState.CANCELED) {
        listener.error(Status.CANCELLED.withDescription(result.getCancelReason()).asException());
      } else {
        listener.completed();
      }
      loader.close();
      root.close();
    }
  }

  @Override
  public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener listener) {
    RetrieveData d = new RetrieveData(listener);
    RunQuery query;
    try {
      query = RunQuery.newBuilder()
        .setType(QueryType.PREPARED_STATEMENT)
        .setSource(UserProtos.SubmissionSource.UNKNOWN_SOURCE)
        .setPriority(UserProtos.QueryPriority
          .newBuilder()
          .setWorkloadClass(UserBitShared.WorkloadClass.GENERAL)
          .setWorkloadType(UserBitShared.WorkloadType.UNKNOWN)
          .build())
        .setPreparedStatementHandle(PreparedStatementHandle.parseFrom(ticket.getBytes()))
        .build();
    } catch (InvalidProtocolBufferException e) {
      throw Status.UNKNOWN.withCause(e).asRuntimeException();
    }

    UserRequest request = new UserRequest(RpcType.RUN_QUERY, query);
    submitWork(callContext, request, d);
  }

  @Override
  public void listActions(CallContext callContext, StreamListener<ActionType> listener) {
    listener.onCompleted();
  }

  @Override
  public void listFlights(CallContext callContext, Criteria arg0, StreamListener<FlightInfo> list) {
    list.onCompleted();
  }

  private static class WriterVisitor extends AbstractPhysicalVisitor<Boolean, Boolean, Throwable> {

    @Override
    public Boolean visitWriter(Writer writer, Boolean value) throws Throwable {
      return true;
    }

    @Override
    public Boolean visitOp(PhysicalOperator op, Boolean value) throws Throwable {
      return visitChildren(op, value);
    }

    @Override
    public Boolean visitChildren(PhysicalOperator op, Boolean value) throws Throwable {
      boolean isWriter = (value == null) ? false : value;
      for (PhysicalOperator child : op) {
        Boolean result = child.accept(this, value);
        isWriter |= (result == null) ? false : result;
      }
      return isWriter;
    }
  }
}
