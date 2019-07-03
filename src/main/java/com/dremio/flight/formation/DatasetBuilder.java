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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

public class DatasetBuilder implements DatasetHandle {

  private final List<FlightClient> clients;
  private EntityPath key;

  private DatasetConfig config;
  private List<PartitionChunk> splits;
  private List<FlightInfo> infos;

  public DatasetBuilder(List<FlightClient> clients, EntityPath key) {
    super();
    this.clients = clients;
    this.key = key;
    buildIfNecessary();
  }

  public DatasetBuilder(List<FlightClient> clients, EntityPath key, List<FlightInfo> infos) {
    super();
    this.clients = clients;
    this.key = key;
    this.infos = infos;
  }

  private void buildIfNecessary() {
    if (config != null) {
      return;
    }

    if (infos == null) {
      infos = clients.stream()
        .map(c -> c.getInfo(FlightDescriptor.path(key.getName())))
        .collect(Collectors.toList());
    }

    Preconditions.checkArgument(!infos.isEmpty());
    Schema schema = null;
    long records = 0;
    List<FlightEndpoint> endpoints = new ArrayList<>();
    for (FlightInfo info : infos) {
      schema = info.getSchema();
      records += info.getRecords();
      endpoints.addAll(info.getEndpoints());
    }

    config = new DatasetConfig()
      .setFullPathList(key.getComponents())
      .setName(key.getName())
      .setType(DatasetType.PHYSICAL_DATASET)
      .setId(new EntityId().setId(UUID.randomUUID().toString()))
      .setReadDefinition(new ReadDefinition()
        .setScanStats(new ScanStats().setRecordCount(records)
          .setScanFactor(ScanCostFactor.PARQUET.getFactor())))
      .setOwner(SystemUser.SYSTEM_USERNAME)
      .setPhysicalDataset(new PhysicalDataset())
      .setRecordSchema(new BatchSchema(schema.getFields()).toByteString())
      .setSchemaVersion(DatasetHelper.CURRENT_VERSION);

    splits = new ArrayList<>();
    List<DatasetSplit> dSplits = Lists.newArrayList();
//     int i =0;
    for (FlightEndpoint ep : endpoints) {

      List<Location> locations = ep.getLocations();
      if (locations.size() > 1) {
        throw new UnsupportedOperationException("I dont know what more than one location means, not handling it");
      }
      DatasetSplitAffinity a = DatasetSplitAffinity.of(locations.get(0).getUri().getHost(), 100d);

//       split.setSplitKey(Integer.toString(i));
      Flight.Ticket ticket = Flight.Ticket.newBuilder().setTicket(ByteString.copyFrom(ep.getTicket().getBytes())).build();
      dSplits.add(DatasetSplit.of(ImmutableList.of(a), records / endpoints.size(), records, ticket::writeTo));
    }
    splits.add(PartitionChunk.of(dSplits));
  }

  @Override
  public EntityPath getDatasetPath() {
    return key;
  }

}
