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
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.store.CatalogService;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("formation-writer")
public class FormationWriter extends AbstractWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormationWriter.class);

  private final String location;
  private final FormationPlugin plugin;

  @JsonCreator
  public FormationWriter(
    @JsonProperty("child") PhysicalOperator child,
    @JsonProperty("props") OpProps props,
    @JsonProperty("location") String location,
    @JsonProperty("options") WriterOptions options,
    @JsonProperty("sortColumns") List<String> sortColumns,
    @JsonProperty("pluginId") StoragePluginId pluginId,
    @JacksonInject CatalogService catalogService
  ) throws IOException, ExecutionSetupException {
    super(props, child, options);
    //CatalogService catalogService = null;
    this.plugin = catalogService.getSource(pluginId);
    this.location = location;
  }

  public FormationWriter(
    PhysicalOperator child,
    OpProps props,
    String location,
    WriterOptions options,
    FormationPlugin plugin) {
    super(props, child, options);
    this.plugin = plugin;
    this.location = location;
  }

  @JsonProperty("location")
  public String getLocation() {
    return location;
  }

  public StoragePluginId getPluginId() {
    return plugin.getId();
  }

  @JsonIgnore
  FormationPlugin getPlugin() {
    return plugin;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new FormationWriter(child, props, location, getOptions(), plugin);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.ARROW_WRITER_VALUE;
  }
}
