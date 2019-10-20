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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
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
import org.apache.arrow.flight.auth.BasicClientAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.BaseTestQuery;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecTest;
import com.dremio.exec.rpc.ssl.SSLConfig;
import com.dremio.exec.rpc.ssl.SSLConfigurator;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.protostuff.LinkedBuffer;

/**
 * Basic flight endpoint test
 */
public class TestSslFlightEndpoint extends BaseTestQuery {

  private static InitializerRegistry registry;
  private static final LinkedBuffer buffer = LinkedBuffer.allocate();
  private static final ExecutorService tp = Executors.newFixedThreadPool(4);
  private static final Logger logger = LoggerFactory.getLogger(TestSslFlightEndpoint.class);
  private static DremioConfig dremioConfig;

  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    System.setProperty("dremio.flight.use-ssl", "true");
    dremioConfig = DremioConfig.create()
      .withValue(
        DremioConfig.WEB_SSL_PREFIX + DremioConfig.SSL_ENABLED,
        true)
      .withValue(
        DremioConfig.WEB_SSL_PREFIX + DremioConfig.SSL_AUTO_GENERATED_CERTIFICATE,
        true)
      .withValue(
        DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.getRoot().getAbsolutePath());
    getBindingCreator().bind(DremioConfig.class, dremioConfig);
    registry = new InitializerRegistry(ExecTest.CLASSPATH_SCAN_RESULT, getBindingProvider());
    registry.start();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    registry.close();
  }

  private static InputStream certs() throws GeneralSecurityException, IOException {
    final SSLConfigurator configurator = new SSLConfigurator(dremioConfig, DremioConfig.WEB_SSL_PREFIX, "web");
    final Optional<SSLConfig> sslConfigOption = configurator.getSSLConfig(true, "localhost");
    Preconditions.checkState(sslConfigOption.isPresent()); // caller's responsibility
    final SSLConfig sslConfig = sslConfigOption.get();
    KeyStore trustStore = null;
    //noinspection StringEquality
    if (sslConfig.getTrustStorePath() != SSLConfig.UNSPECIFIED) {
      trustStore = KeyStore.getInstance(sslConfig.getTrustStoreType());
      try (InputStream stream = Files.newInputStream(Paths.get(sslConfig.getTrustStorePath()))) {
        trustStore.load(stream, sslConfig.getTrustStorePassword().toCharArray());
      }
    }
    Enumeration<String> es = trustStore.aliases();
    String alias = "";
    List<Certificate> certs = Lists.newArrayList();
    while (es.hasMoreElements()) {
      alias = (String) es.nextElement();
      // if alias refers to a private key break at that point
      // as we want to use that certificate
      certs.add(trustStore.getCertificate(alias));
    }
    return certsToStream(certs);
  }

  private static InputStream certsToStream(List<Certificate> certs) throws IOException {

    final StringWriter writer = new StringWriter();
    final JcaPEMWriter pemWriter = new JcaPEMWriter(writer);
    for (Certificate cert : certs) {
      pemWriter.writeObject(cert);
    }
    pemWriter.flush();
    pemWriter.close();
    String pemString = writer.toString();
    return new ByteArrayInputStream(pemString.getBytes());
  }

  private static FlightClient flightClient(BufferAllocator allocator, Location location) {
    try {
      InputStream certStream = certs();
      return FlightClient.builder()
        .allocator(allocator)
        .location(location)
        .useTls()
        .trustedCertificates(certStream)
        .build();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void connect() throws Exception {
    Location location = Location.forGrpcTls("localhost", 47470);
    try (FlightClient c = flightClient(getAllocator(), location)) {
      c.authenticate(new BasicClientAuthHandler(SystemUser.SYSTEM_USERNAME, null));
      String sql = "select * from sys.options";
      FlightInfo info = c.getInfo(FlightDescriptor.command(sql.getBytes()));
      long total = info.getEndpoints().stream()
        .map(this::submit)
        .map(TestSslFlightEndpoint::get)
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
      try (FlightClient c = flightClient(allocator, endpoint.getLocations().get(0))) {
        c.authenticate(new BasicClientAuthHandler(SystemUser.SYSTEM_USERNAME, null));
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
