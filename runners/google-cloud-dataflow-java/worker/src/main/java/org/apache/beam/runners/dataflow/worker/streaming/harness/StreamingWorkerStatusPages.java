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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.dataflow.worker.status.BaseStatusServlet;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.LastExceptionDataProvider;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.ChannelzServlet;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow status pages for debugging current worker and processing state.
 *
 * @implNote Class member state should only be accessed, not modified.
 */
public class StreamingWorkerStatusPages implements StreamingStatusPages {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingWorkerStatusPages.class);
  private static final String DUMP_STATUS_PAGES_EXECUTOR = "DumpStatusPages";

  private final Supplier<Instant> clock;
  private final long clientId;
  private final AtomicBoolean isRunning;
  private final WorkerStatusPages statusPages;
  private final WindmillStateCache stateCache;
  private final ComputationStateCache computationStateCache;
  private final Supplier<Long> currentActiveCommitBytes;
  private final Consumer<PrintWriter> getDataStatusProvider;
  private final BoundedQueueExecutor workUnitExecutor;
  private final ScheduledExecutorService statusPageDumper;

  // StreamingEngine status providers.
  private final @Nullable GrpcWindmillStreamFactory windmillStreamFactory;
  private final DebugCapture.@Nullable Manager debugCapture;
  private final @Nullable ChannelzServlet channelzServlet;

  private StreamingWorkerStatusPages(
      Supplier<Instant> clock,
      long clientId,
      AtomicBoolean isRunning,
      WorkerStatusPages statusPages,
      DebugCapture.@Nullable Manager debugCapture,
      @Nullable ChannelzServlet channelzServlet,
      WindmillStateCache stateCache,
      ComputationStateCache computationStateCache,
      Supplier<Long> currentActiveCommitBytes,
      @Nullable GrpcWindmillStreamFactory windmillStreamFactory,
      Consumer<PrintWriter> getDataStatusProvider,
      BoundedQueueExecutor workUnitExecutor,
      ScheduledExecutorService statusPageDumper) {
    this.clock = clock;
    this.clientId = clientId;
    this.isRunning = isRunning;
    this.statusPages = statusPages;
    this.debugCapture = debugCapture;
    this.channelzServlet = channelzServlet;
    this.stateCache = stateCache;
    this.computationStateCache = computationStateCache;
    this.currentActiveCommitBytes = currentActiveCommitBytes;
    this.windmillStreamFactory = windmillStreamFactory;
    this.getDataStatusProvider = getDataStatusProvider;
    this.workUnitExecutor = workUnitExecutor;
    this.statusPageDumper = statusPageDumper;
  }

  static StreamingWorkerStatusPages create(
      Supplier<Instant> clock,
      long clientId,
      AtomicBoolean isRunning,
      WorkerStatusPages statusPages,
      DebugCapture.Manager debugCapture,
      ChannelzServlet channelzServlet,
      WindmillStateCache stateCache,
      ComputationStateCache computationStateCache,
      Supplier<Long> currentActiveCommitBytes,
      GrpcWindmillStreamFactory windmillStreamFactory,
      Consumer<PrintWriter> getDataStatusProvider,
      BoundedQueueExecutor workUnitExecutor) {
    return new StreamingWorkerStatusPages(
        clock,
        clientId,
        isRunning,
        statusPages,
        debugCapture,
        channelzServlet,
        stateCache,
        computationStateCache,
        currentActiveCommitBytes,
        windmillStreamFactory,
        getDataStatusProvider,
        workUnitExecutor,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat(DUMP_STATUS_PAGES_EXECUTOR).build()));
  }

  static StreamingWorkerStatusPages forAppliance(
      Supplier<Instant> clock,
      long clientId,
      AtomicBoolean isRunning,
      WorkerStatusPages statusPages,
      WindmillStateCache stateCache,
      ComputationStateCache computationStateCache,
      Supplier<Long> currentActiveCommitBytes,
      Consumer<PrintWriter> getDataStatusProvider,
      BoundedQueueExecutor workUnitExecutor) {
    return new StreamingWorkerStatusPages(
        clock,
        clientId,
        isRunning,
        statusPages,
        null,
        null,
        stateCache,
        computationStateCache,
        currentActiveCommitBytes,
        null,
        getDataStatusProvider,
        workUnitExecutor,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat(DUMP_STATUS_PAGES_EXECUTOR).build()));
  }

  @Override
  public void start() {
    statusPages.addServlet(stateCache.statusServlet());
    statusPages.addServlet(newSpecServlet());
    statusPages.addStatusDataProvider(
        "harness",
        "Harness",
        writer -> {
          writer.println("Running: " + isRunning.get() + "<br>");
          writer.println("ID: " + clientId + "<br>");
        });

    statusPages.addStatusDataProvider(
        "metrics",
        "Metrics",
        new MetricsDataProvider(
            workUnitExecutor,
            currentActiveCommitBytes,
            getDataStatusProvider,
            computationStateCache::getAllComputations));
    statusPages.addStatusDataProvider(
        "exception", "Last Exception", new LastExceptionDataProvider());
    statusPages.addStatusDataProvider("cache", "State Cache", stateCache);

    if (isStreamingEngine()) {
      addStreamingEngineStatusPages();
    }

    statusPages.start();
  }

  private void addStreamingEngineStatusPages() {
    Preconditions.checkNotNull(debugCapture).start();
    statusPages.addServlet(Preconditions.checkNotNull(channelzServlet));
    statusPages.addCapturePage(Preconditions.checkNotNull(channelzServlet));
    statusPages.addStatusDataProvider(
        "streaming", "Streaming RPCs", Preconditions.checkNotNull(windmillStreamFactory));
  }

  private boolean isStreamingEngine() {
    return debugCapture != null && channelzServlet != null && windmillStreamFactory != null;
  }

  @Override
  public void stop() {
    statusPages.stop();
    if (debugCapture != null) {
      debugCapture.stop();
    }
    statusPageDumper.shutdown();
    try {
      statusPageDumper.awaitTermination(300, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Error occurred shutting down periodic status page dumper", e);
    }
    statusPageDumper.shutdownNow();
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void scheduleStatusPageDump(
      String getPeriodicStatusPageOutputDirectory, String workerId, long delay) {
    statusPageDumper.scheduleWithFixedDelay(
        () -> dumpStatusPages(getPeriodicStatusPageOutputDirectory, workerId),
        60,
        delay,
        TimeUnit.SECONDS);
  }

  private void dumpStatusPages(String getPeriodicStatusPageOutputDirectory, String workerId) {
    Collection<DebugCapture.Capturable> pages = statusPages.getDebugCapturePages();
    if (pages.isEmpty()) {
      LOG.warn("No captured status pages.");
    }
    long timestamp = clock.get().getMillis();
    for (DebugCapture.Capturable page : pages) {
      PrintWriter writer = null;
      try {
        File outputFile =
            new File(
                getPeriodicStatusPageOutputDirectory,
                ("StreamingDataflowWorker" + workerId + "_" + page.pageName() + timestamp + ".html")
                    .replaceAll("/", "_"));
        writer = new PrintWriter(outputFile, UTF_8.name());
        page.captureData(writer);
      } catch (IOException e) {
        LOG.warn("Error dumping status page.", e);
      } finally {
        if (writer != null) {
          writer.close();
        }
      }
    }
  }

  private BaseStatusServlet newSpecServlet() {
    return new BaseStatusServlet("/spec") {
      @Override
      protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        PrintWriter writer = response.getWriter();
        computationStateCache.appendSummaryHtml(writer);
      }
    };
  }
}
