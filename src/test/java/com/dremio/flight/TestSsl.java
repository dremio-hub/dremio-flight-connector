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

import java.io.InputStream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.config.DremioConfig;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

public class TestSsl {
  private static DremioConfig dremioConfig;

  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    dremioConfig = DremioConfig.create()
      .withValue(
        DremioConfig.WEB_SSL_PREFIX + DremioConfig.SSL_ENABLED,
        true)
      .withValue(
        DremioConfig.WEB_SSL_PREFIX + DremioConfig.SSL_AUTO_GENERATED_CERTIFICATE,
        true)
      .withValue(
        DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.getRoot().getAbsolutePath());
  }

  @Test
  public void sslTest() throws Exception {
    Pair<InputStream, InputStream> pair = SslHelper.ssl(dremioConfig, "localhost");

    SslContextBuilder builder = SslContextBuilder.forServer(pair.getRight(), pair.getLeft());
    SslContext context = builder.build();
    Assert.assertNotNull(context);
  }


}
