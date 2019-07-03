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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.config.DremioConfig;
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
  private static final Logger logger = LoggerFactory.getLogger(FlightInitializer.class);

  private static final int DEFAULT_PORT = 47470;
  private static final boolean USE_SSL = false;

  private final int port;
  private final String host;
  private final boolean useSsl;

  private FlightServer server;
  private Producer producer;
  private BufferAllocator allocator;

  public FlightInitializer() {
    port = Integer.parseInt(PropertyHelper.getFromEnvProperty("dremio.flight.port", Integer.toString(DEFAULT_PORT))); //todo add this and others to DremioConfig
    String hostname = "localhost";
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      //pass
    }
    host = PropertyHelper.getFromEnvProperty("dremio.flight.host", hostname);
    useSsl = Boolean.parseBoolean(PropertyHelper.getFromEnvProperty("dremio.flight.use-ssl", Boolean.toString(USE_SSL)));
  }

  @Override
  public Void initialize(BindingProvider provider) throws Exception {
    if (!Boolean.parseBoolean(PropertyHelper.getFromEnvProperty("dremio.flight.enabled", Boolean.toString(false)))) {
      logger.info("Flight plugin is not enabled, skipping initialization");
      return null;
    }
    this.allocator = provider.provider(BootStrapContext.class).get().getAllocator().newChildAllocator("arrow-flight", 0, Long.MAX_VALUE);


    AuthValidator validator = new AuthValidator(provider.provider(UserService.class), provider.provider(SabotContext.class));
    FlightServer.Builder serverBuilder = FlightServer.builder().allocator(allocator).authHandler(new BasicServerAuthHandler(validator));
    DremioConfig config = null;
    try {
      config = provider.lookup(DremioConfig.class);
    } catch (Throwable t) {
    }
    Pair<Location, FlightServer.Builder> pair = SslHelper.sslHelper(
      serverBuilder,
      config,
      useSsl,
      InetAddress.getLocalHost().getHostName(),
      port,
      host);
    producer = new Producer(
      pair.getKey(),
      provider.provider(UserWorker.class),
      provider.provider(SabotContext.class),
      allocator,
      validator);
    pair.getRight().producer(producer);
    server = pair.getRight().build();
    server.start();
    logger.info("set up flight plugin on port {} and host {}", pair.getKey().getUri().getPort(), pair.getKey().getUri().getHost());
    return null;
  }

  public void close() throws Exception {
    AutoCloseables.close(producer, server, allocator);
  }

}
