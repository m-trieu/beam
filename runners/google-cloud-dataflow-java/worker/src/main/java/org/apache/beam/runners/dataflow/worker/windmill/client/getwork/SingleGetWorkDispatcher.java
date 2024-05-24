/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.dataflow.worker.windmill.client.getwork;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gets and schedules work from a single client. */
abstract class SingleGetWorkDispatcher implements GetWorkDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(SingleGetWorkDispatcher.class);
  private static final String DISPATCH_THREAD_NAME = "GetWorkDispatchThread";

  private final ExecutorService getWorkDispatchExecutor;

  protected SingleGetWorkDispatcher() {
    this.getWorkDispatchExecutor =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(DISPATCH_THREAD_NAME)
                .setDaemon(true)
                .setPriority(Thread.MIN_PRIORITY)
                .build());
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public final void start() {
    getWorkDispatchExecutor.submit(
        () -> {
          LOG.info("Dispatch starting");
          pollGetWork();
          LOG.info("Dispatch done");
        });
  }

  @Override
  public final void stop() {
    getWorkDispatchExecutor.shutdown();
    boolean isShutdown;
    try {
      isShutdown = getWorkDispatchExecutor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (!isShutdown) {
      getWorkDispatchExecutor.shutdownNow();
    }
  }

  protected abstract void pollGetWork();
}
