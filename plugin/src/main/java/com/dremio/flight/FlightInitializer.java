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

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.service.BindingProvider;
import com.dremio.service.Initializer;
import com.dremio.service.users.UserService;

/**
 * Intialize a basic Flight endpoint
 */
public class FlightInitializer implements Initializer<Void>, AutoCloseable {

  private static final int PORT = 47470;

  private FlightServer server;
  private Producer producer;
  private BufferAllocator allocator;

  @Override
  public Void initialize(BindingProvider provider) throws Exception {
    this.allocator = provider.provider(BootStrapContext.class).get().getAllocator().newChildAllocator("arrow-flight", 0, Long.MAX_VALUE);
    AuthValidator validator = new AuthValidator(provider.provider(UserService.class), provider.provider(SabotContext.class));
    Location location = Location.forGrpcInsecure("localhost", PORT);
    producer = new Producer(
      location,
      provider.provider(UserWorker.class),
      provider.provider(SabotContext.class),
      allocator,
      validator);
    server = FlightServer.builder().allocator(allocator).producer(producer).location(location).authHandler(new DremioServerAuthHandler(validator)).build();
    server.start();
    return null;
  }

  public void close() throws Exception {
    AutoCloseables.close(producer, server, allocator);
  }
}
