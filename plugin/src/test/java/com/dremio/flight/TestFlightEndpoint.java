/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecTest;
import com.dremio.proto.flight.commands.Command;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.users.SystemUser;

import io.protostuff.ByteString;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;

/**
 * Basic flight endpoint test
 */
public class TestFlightEndpoint extends BaseTestQuery {

  private static InitializerRegistry registry;
  private static final LinkedBuffer buffer = LinkedBuffer.allocate();
  private static final ExecutorService tp = Executors.newFixedThreadPool(4);
  private static final Logger logger = LoggerFactory.getLogger(TestFlightEndpoint.class);

  @BeforeClass
  public static void init() throws Exception {
    registry = new InitializerRegistry(ExecTest.CLASSPATH_SCAN_RESULT, getBindingProvider());
    registry.start();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    registry.close();
  }

  @Test
  public void connect() throws Exception {
    try (FlightClient c = FlightClient.builder().allocator(getAllocator()).location(Location.forGrpcInsecure("localhost", 47470)).build()) {
      c.authenticate(new DremioClientAuthHandler(SystemUser.SYSTEM_USERNAME, null));
      String sql = "select * from sys.options";
      byte[] message = ProtostuffIOUtil.toByteArray(new Command(sql, false, false, ByteString.EMPTY), Command.getSchema(), buffer);
      buffer.clear();
      FlightInfo info = c.getInfo(FlightDescriptor.command(message));
      long total = info.getEndpoints().stream()
        .map(this::submit)
        .map(TestFlightEndpoint::get)
        .mapToLong(Long::longValue)
        .sum();

      Assert.assertTrue(total > 1);
      System.out.println(total);
    }
  }

  private static AtomicInteger endpointsSubmitted = new AtomicInteger();
  private static AtomicInteger endpointsWaitingOn = new AtomicInteger();
  private static AtomicInteger endpointsReceived = new AtomicInteger();

  private Future<Long> submit(FlightEndpoint e) {
    int thisEndpoint = endpointsSubmitted.incrementAndGet();
    logger.debug("submitting flight endpoint {} with ticket {} to {}", thisEndpoint, new String(e.getTicket().getBytes()), e.getLocations().get(0).getUri());
    RunnableReader reader = new RunnableReader(allocator, e);
    Future<Long> f = tp.submit(reader);
    logger.debug("submitted flight endpoint {} with ticket {} to {}", thisEndpoint, new String(e.getTicket().getBytes()), e.getLocations().get(0).getUri());
    return f;
  }

  private static Long get(Future<Long> r) {
    try {
      logger.debug("starting wait on future {} of {}", endpointsWaitingOn.incrementAndGet(), endpointsSubmitted.get());
      Long f = r.get();
      logger.debug("returned future {} of {} with value {}", endpointsReceived.incrementAndGet(), endpointsSubmitted.get(), f);
      return f;
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }


  private static final class RunnableReader implements Callable<Long> {
    private final BufferAllocator allocator;
    private FlightEndpoint endpoint;

    private RunnableReader(BufferAllocator allocator, FlightEndpoint endpoint) {
      this.allocator = allocator;
      this.endpoint = endpoint;
    }

    @Override
    public Long call() {
      long count = 0;
      int readIndex = 0;
      logger.debug("starting work on flight endpoint with ticket {} to {}", new String(endpoint.getTicket().getBytes()), endpoint.getLocations().get(0).getUri());
      try (FlightClient c = FlightClient.builder().allocator(allocator).location(endpoint.getLocations().get(0)).build()) {
        c.authenticate(new DremioClientAuthHandler(SystemUser.SYSTEM_USERNAME, null));
        logger.debug("trying to get stream for flight endpoint with ticket {} to {}", new String(endpoint.getTicket().getBytes()), endpoint.getLocations().get(0).getUri());
        FlightStream fs = c.getStream(endpoint.getTicket());
        logger.debug("got stream for flight endpoint with ticket {} to {}. Will now try and read", new String(endpoint.getTicket().getBytes()), endpoint.getLocations().get(0).getUri());
        while (fs.next()) {
          long thisCount = fs.getRoot().getRowCount();
          count += thisCount;
          logger.debug("got results from stream for flight endpoint with ticket {} to {}. This is read {} and we got {} rows back for a total of {}", new String(endpoint.getTicket().getBytes()), endpoint.getLocations().get(0).getUri(), ++readIndex, thisCount, count);
          fs.getRoot().clear();
        }
      } catch (InterruptedException e) {

      } catch (Throwable t) {
        logger.error("Error in stream fetch", t);
      }
      logger.debug("got all results from stream for flight endpoint with ticket {} to {}. We read {} batches and we got {} rows back", new String(endpoint.getTicket().getBytes()), endpoint.getLocations().get(0).getUri(), ++readIndex, count);
      return count;
    }
  }
}
