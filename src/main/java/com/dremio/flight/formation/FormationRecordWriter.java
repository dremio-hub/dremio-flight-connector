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
import java.util.stream.Collectors;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.WritePartition;
import com.google.common.collect.ImmutableList;

public class FormationRecordWriter implements RecordWriter {
  private static final Logger logger = LoggerFactory.getLogger(FormationRecordWriter.class);
  private final FlightDescriptor descriptor;

  private VectorSchemaRoot root;
  private FormationPlugin.FormationFlightProducer store;
  private VectorUnloader unloader;
  private Stream.Producer creator;

  public FormationRecordWriter(String path, FormationPlugin.FormationFlightProducer store, ExecProtos.FragmentHandle fragmentHandle) {
    super();
    this.store = store;
    this.descriptor = FlightDescriptor.path(
      QueryIdHelper.getQueryId(fragmentHandle.getQueryId()),
      String.valueOf(fragmentHandle.getMajorFragmentId()),
      String.valueOf(fragmentHandle.getMinorFragmentId())
    );
  }

  @Override
  public void close() throws Exception {
    if (creator != null) {
      creator.close();
    }
  }


  @Override
  public void setup(VectorAccessible incoming, OutputEntryListener listener, WriteStatsListener statsListener) throws IOException {
    root = new VectorSchemaRoot(ImmutableList.copyOf(incoming)
      .stream()
      .map(vw -> ((FieldVector) vw.getValueVector()))
      .collect(Collectors.toList()));
    unloader = new VectorUnloader(root);
    creator = store.putStream(
      descriptor, root.getSchema());
  }

  @Override
  public void startPartition(WritePartition partition) throws Exception {
    logger.warn("start partition for {}", descriptor);
  }

  @Override
  public int writeBatch(int offset, int length) throws IOException {
    root.setRowCount(length);
    try (ArrowRecordBatch arb = unloader.getRecordBatch()) {
      long size = arb.computeBodyLength();
      creator.add(arb);
      return (int)size;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void abort() throws IOException {
    creator.abort();
  }


}
