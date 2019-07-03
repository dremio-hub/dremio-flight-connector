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
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Enumeration;
import java.util.Optional;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.commons.lang3.tuple.Pair;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.util.io.pem.PemObject;

import com.dremio.config.DremioConfig;
import com.dremio.exec.rpc.ssl.SSLConfig;
import com.dremio.exec.rpc.ssl.SSLConfigurator;
import com.dremio.exec.server.SabotContext;
import com.google.common.base.Preconditions;

public final class SslHelper {

  private SslHelper() {
  }

  public static Pair<Location, FlightServer.Builder> sslHelper(FlightServer.Builder serverBuilder, DremioConfig config, boolean useSsl, String hostname, int port, String sslHostname) {
    Location location;
    Location exLocation;
    try {
      if (!useSsl) {
        throw new UnsupportedOperationException("Don't use ssl");
      }

      Pair<InputStream, InputStream> pair = ssl(config, sslHostname);
      location = Location.forGrpcTls(hostname, port);
      exLocation = Location.forGrpcTls(sslHostname, port);
      serverBuilder.useTls(pair.getRight(), pair.getLeft()).location(location);
    } catch (Exception e) {
      location = Location.forGrpcInsecure(hostname, port);
      exLocation = Location.forGrpcInsecure(sslHostname, port);
      serverBuilder.location(location);
    }
    return Pair.of(exLocation, serverBuilder);
  }

  static Pair<InputStream, InputStream> ssl(DremioConfig config, String hostName) throws Exception {
    final SSLConfigurator configurator = new SSLConfigurator(config, DremioConfig.WEB_SSL_PREFIX, "web");
    final Optional<SSLConfig> sslConfigOption = configurator.getSSLConfig(true, hostName);
    Preconditions.checkState(sslConfigOption.isPresent()); // caller's responsibility
    final SSLConfig sslConfig = sslConfigOption.get();

    final KeyStore keyStore = KeyStore.getInstance(sslConfig.getKeyStoreType());
    try (InputStream stream = Files.newInputStream(Paths.get(sslConfig.getKeyStorePath()))) {
      keyStore.load(stream, sslConfig.getKeyStorePassword().toCharArray());
    }

    boolean isAliasWithPrivateKey = false;
    Enumeration<String> es = keyStore.aliases();
    String alias = "";
    while (es.hasMoreElements()) {
      alias = (String) es.nextElement();
      // if alias refers to a private key break at that point
      // as we want to use that certificate
      if (isAliasWithPrivateKey = keyStore.isKeyEntry(alias)) {
        break;
      }
    }

    if (isAliasWithPrivateKey) {

      KeyStore.PrivateKeyEntry pkEntry = (KeyStore.PrivateKeyEntry) keyStore.getEntry(alias,
        new KeyStore.PasswordProtection(sslConfig.getKeyPassword().toCharArray()));

      PrivateKey myPrivateKey = pkEntry.getPrivateKey();


      // Load certificate chain
      Certificate[] chain = keyStore.getCertificateChain(alias);
      return Pair.of(keyToStream(myPrivateKey), certsToStream(chain));
    }

    return null;
  }

  private static InputStream keyToStream(PrivateKey key) throws IOException {
    final StringWriter writer = new StringWriter();
    final JcaPEMWriter pemWriter = new JcaPEMWriter(writer);
    pemWriter.writeObject(new PemObject("PRIVATE KEY", key.getEncoded()));
    pemWriter.flush();
    pemWriter.close();
    String pemString = writer.toString();
    return new ByteArrayInputStream(pemString.getBytes());
  }

  private static InputStream certsToStream(Certificate[] certs) throws IOException {

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

}
