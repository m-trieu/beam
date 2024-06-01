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
package org.apache.beam.runners.dataflow.worker;

import com.google.auto.value.AutoBuilder;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamCancelledException;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.SettableFuture;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around a {@link WindmillServerStub} that tracks metrics for the number of in-flight
 * requests and throttles requests when memory pressure is high.
 *
 * <p>External API: individual worker threads request state for their computation via {@link
 * #getStateData}. However, requests are either issued using a pool of streaming rpcs or possibly
 * batched requests.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class MetricTrackingWindmillServerStub {
  private static final Logger LOG = LoggerFactory.getLogger(MetricTrackingWindmillServerStub.class);
  private static final String FAN_OUT_REFRESH_WORK_EXECUTOR = "FanOutActiveWorkRefreshExecutor";

  private static final int MAX_READS_PER_BATCH = 60;
  private static final int MAX_ACTIVE_READS = 10;
  private static final Duration STREAM_TIMEOUT = Duration.standardSeconds(30);
  private final AtomicInteger activeSideInputs = new AtomicInteger();
  private final AtomicInteger activeStateReads = new AtomicInteger();
  private final AtomicInteger activeHeartbeats = new AtomicInteger();
  private final WindmillServerStub server;
  private final MemoryMonitor gcThrashingMonitor;
  private final boolean useStreamingRequests;
  private final WindmillStreamPool<GetDataStream> getDataStreamPool;
  private final ExecutorService fanOutActiveWorkRefreshExecutor;

  // This may be the same instance as getDataStreamPool based upon options.
  private final WindmillStreamPool<GetDataStream> heartbeatStreamPool;

  @GuardedBy("this")
  private final List<ReadBatch> pendingReadBatches;

  @GuardedBy("this")
  private int activeReadThreads = 0;

  @Internal
  @AutoBuilder(ofClass = MetricTrackingWindmillServerStub.class)
  public abstract static class Builder {

    abstract Builder setServer(WindmillServerStub server);

    abstract Builder setGcThrashingMonitor(MemoryMonitor gcThrashingMonitor);

    abstract Builder setUseStreamingRequests(boolean useStreamingRequests);

    abstract Builder setUseSeparateHeartbeatStreams(boolean useSeparateHeartbeatStreams);

    abstract Builder setNumGetDataStreams(int numGetDataStreams);

    abstract MetricTrackingWindmillServerStub build();
  }

  public static Builder builder(WindmillServerStub server, MemoryMonitor gcThrashingMonitor) {
    return new AutoBuilder_MetricTrackingWindmillServerStub_Builder()
        .setServer(server)
        .setGcThrashingMonitor(gcThrashingMonitor)
        .setUseStreamingRequests(false)
        .setUseSeparateHeartbeatStreams(false)
        .setNumGetDataStreams(1);
  }

  MetricTrackingWindmillServerStub(
      WindmillServerStub server,
      MemoryMonitor gcThrashingMonitor,
      boolean useStreamingRequests,
      boolean useSeparateHeartbeatStreams,
      int numGetDataStreams) {
    this.server = server;
    this.gcThrashingMonitor = gcThrashingMonitor;
    this.useStreamingRequests = useStreamingRequests;
    this.fanOutActiveWorkRefreshExecutor =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat(FAN_OUT_REFRESH_WORK_EXECUTOR).build());
    if (useStreamingRequests) {
      getDataStreamPool =
          WindmillStreamPool.create(
              Math.max(1, numGetDataStreams), STREAM_TIMEOUT, this.server::getDataStream);
      if (useSeparateHeartbeatStreams) {
        heartbeatStreamPool =
            WindmillStreamPool.create(1, STREAM_TIMEOUT, this.server::getDataStream);
      } else {
        heartbeatStreamPool = getDataStreamPool;
      }
    } else {
      getDataStreamPool = heartbeatStreamPool = null;
    }
    // This is used as a queue but is expected to be less than 10 batches.
    this.pendingReadBatches = new ArrayList<>();
  }

  // Adds the entry to a read batch for sending to the windmill server. If a non-null batch is
  // returned, this thread will be responsible for sending the batch and should wait for the batch
  // startRead to be notified.
  // If null is returned, the entry was added to a read batch that will be issued by another thread.
  private @Nullable ReadBatch addToReadBatch(QueueEntry entry) {
    synchronized (this) {
      ReadBatch batch;
      if (activeReadThreads < MAX_ACTIVE_READS) {
        assert (pendingReadBatches.isEmpty());
        activeReadThreads += 1;
        // fall through to below synchronized block
      } else if (pendingReadBatches.isEmpty()
          || pendingReadBatches.get(pendingReadBatches.size() - 1).reads.size()
              >= MAX_READS_PER_BATCH) {
        // This is the first read of a batch, it will be responsible for sending the batch.
        batch = new ReadBatch();
        pendingReadBatches.add(batch);
        batch.reads.add(entry);
        return batch;
      } else {
        // This fits within an existing batch, it will be sent by the first blocking thread in the
        // batch.
        pendingReadBatches.get(pendingReadBatches.size() - 1).reads.add(entry);
        return null;
      }
    }
    ReadBatch batch = new ReadBatch();
    batch.reads.add(entry);
    batch.startRead.set(true);
    return batch;
  }

  private void issueReadBatch(ReadBatch batch) {
    try {
      boolean read = batch.startRead.get();
      assert (read);
    } catch (InterruptedException e) {
      // We don't expect this thread to be interrupted. To simplify handling, we just fall through
      // to issuing
      // the call.
      assert (false);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      // startRead is a SettableFuture so this should never occur.
      throw new AssertionError("Should not have exception on startRead", e);
    }
    Map<WindmillComputationKey, SettableFuture<Windmill.KeyedGetDataResponse>> pendingResponses =
        new HashMap<>(batch.reads.size());
    Map<String, Windmill.ComputationGetDataRequest.Builder> computationBuilders = new HashMap<>();
    for (QueueEntry entry : batch.reads) {
      Windmill.ComputationGetDataRequest.Builder computationBuilder =
          computationBuilders.computeIfAbsent(
              entry.computation,
              k -> Windmill.ComputationGetDataRequest.newBuilder().setComputationId(k));

      computationBuilder.addRequests(entry.request);
      pendingResponses.put(
          WindmillComputationKey.create(
              entry.computation, entry.request.getKey(), entry.request.getShardingKey()),
          entry.response);
    }

    // Build the full GetDataRequest from the KeyedGetDataRequests pulled from the queue.
    Windmill.GetDataRequest.Builder builder = Windmill.GetDataRequest.newBuilder();
    for (Windmill.ComputationGetDataRequest.Builder computationBuilder :
        computationBuilders.values()) {
      builder.addRequests(computationBuilder);
    }

    try {
      Windmill.GetDataResponse response = server.getData(builder.build());

      // Dispatch the per-key responses back to the waiting threads.
      for (Windmill.ComputationGetDataResponse computationResponse : response.getDataList()) {
        for (Windmill.KeyedGetDataResponse keyResponse : computationResponse.getDataList()) {
          pendingResponses
              .get(
                  WindmillComputationKey.create(
                      computationResponse.getComputationId(),
                      keyResponse.getKey(),
                      keyResponse.getShardingKey()))
              .set(keyResponse);
        }
      }
    } catch (RuntimeException e) {
      // Fan the exception out to the reads.
      for (QueueEntry entry : batch.reads) {
        entry.response.setException(e);
      }
    } finally {
      synchronized (this) {
        assert (activeReadThreads >= 1);
        if (pendingReadBatches.isEmpty()) {
          activeReadThreads--;
        } else {
          // Notify the thread responsible for issuing the next batch read.
          ReadBatch startBatch = pendingReadBatches.remove(0);
          startBatch.startRead.set(true);
        }
      }
    }
  }

  public Windmill.KeyedGetDataResponse getStateData(
      String computation, Windmill.KeyedGetDataRequest request) {
    gcThrashingMonitor.waitForResources("GetStateData");
    activeStateReads.getAndIncrement();

    try {
      if (useStreamingRequests) {
        GetDataStream stream = getDataStreamPool.getStream();
        try {
          return stream.requestKeyedData(computation, request);
        } finally {
          getDataStreamPool.releaseStream(stream);
        }
      } else {
        SettableFuture<Windmill.KeyedGetDataResponse> response = SettableFuture.create();
        ReadBatch batch = addToReadBatch(new QueueEntry(computation, request, response));
        if (batch != null) {
          issueReadBatch(batch);
        }
        return response.get();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      activeStateReads.getAndDecrement();
    }
  }

  public Windmill.KeyedGetDataResponse getStateData(
      GetDataStream getDataStream, String computation, Windmill.KeyedGetDataRequest request) {
    gcThrashingMonitor.waitForResources("GetStateData");
    activeStateReads.getAndIncrement();
    if (getDataStream.isClosed()) {
      throw new WorkItemCancelledException(request.getShardingKey());
    }

    try {
      return getDataStream.requestKeyedData(computation, request);
    } catch (Exception e) {
      if (WindmillStreamCancelledException.isWindmillStreamCancelledException(e)) {
        LOG.error("Tried to fetch keyed data from a closed stream. Work has been cancelled", e);
        throw new WorkItemCancelledException(request.getShardingKey());
      }
      throw new RuntimeException(e);
    } finally {
      activeStateReads.getAndDecrement();
    }
  }

  public Windmill.GlobalData getSideInputData(Windmill.GlobalDataRequest request) {
    gcThrashingMonitor.waitForResources("GetSideInputData");
    activeSideInputs.getAndIncrement();
    try {
      if (useStreamingRequests) {
        GetDataStream stream = getDataStreamPool.getStream();
        try {
          return stream.requestGlobalData(request);
        } finally {
          getDataStreamPool.releaseStream(stream);
        }
      } else {
        return server
            .getData(
                Windmill.GetDataRequest.newBuilder().addGlobalDataFetchRequests(request).build())
            .getGlobalData(0);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to get side input: ", e);
    } finally {
      activeSideInputs.getAndDecrement();
    }
  }

  public Windmill.GlobalData getSideInputData(
      GetDataStream getDataStream, Windmill.GlobalDataRequest request) {
    gcThrashingMonitor.waitForResources("GetSideInputData");
    activeSideInputs.getAndIncrement();
    try {
      return getDataStream.requestGlobalData(request);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get side input: ", e);
    } finally {
      activeSideInputs.getAndDecrement();
    }
  }

  /** Tells windmill processing is ongoing for the given keys. */
  public void refreshActiveWork(Map<String, List<HeartbeatRequest>> heartbeats) {
    if (heartbeats.isEmpty()) {
      return;
    }
    activeHeartbeats.set(heartbeats.size());
    try {
      if (useStreamingRequests) {
        GetDataStream stream = heartbeatStreamPool.getStream();
        try {
          stream.refreshActiveWork(heartbeats);
        } finally {
          heartbeatStreamPool.releaseStream(stream);
        }
      } else {
        // This code path is only used by appliance which sends heartbeats (used to refresh active
        // work) as KeyedGetDataRequests. So we must translate the HeartbeatRequest to a
        // KeyedGetDataRequest here regardless of the value of sendKeyedGetDataRequests.
        Windmill.GetDataRequest.Builder builder = Windmill.GetDataRequest.newBuilder();
        for (Map.Entry<String, List<HeartbeatRequest>> entry : heartbeats.entrySet()) {
          Windmill.ComputationGetDataRequest.Builder perComputationBuilder =
              Windmill.ComputationGetDataRequest.newBuilder();
          perComputationBuilder.setComputationId(entry.getKey());
          for (HeartbeatRequest request : entry.getValue()) {
            perComputationBuilder.addRequests(
                Windmill.KeyedGetDataRequest.newBuilder()
                    .setShardingKey(request.getShardingKey())
                    .setWorkToken(request.getWorkToken())
                    .setCacheToken(request.getCacheToken())
                    .addAllLatencyAttribution(request.getLatencyAttributionList())
                    .build());
          }
          builder.addRequests(perComputationBuilder.build());
        }
        server.getData(builder.build());
      }
    } finally {
      activeHeartbeats.set(0);
    }
  }

  /**
   * Attempts to refresh active work, fanning out to each {@link GetDataStream} in parallel.
   *
   * @implNote Skips closed {@link GetDataStream}(s).
   */
  public void refreshActiveWorkWithFanOut(
      Map<GetDataStream, Map<String, List<HeartbeatRequest>>> heartbeats) {
    if (heartbeats.isEmpty()) {
      return;
    }
    try {
      List<CompletableFuture<Void>> fanOutRefreshActiveWork = new ArrayList<>();
      for (Map.Entry<GetDataStream, Map<String, List<HeartbeatRequest>>> heartbeat :
          heartbeats.entrySet()) {
        GetDataStream stream = heartbeat.getKey();
        Map<String, List<HeartbeatRequest>> heartbeatRequests = heartbeat.getValue();
        if (stream.isClosed()) {
          LOG.warn(
              "Trying to refresh work on stream={} after work has moved off of worker."
                  + " heartbeats={}",
              stream,
              heartbeatRequests);
        } else {
          fanOutRefreshActiveWork.add(sendHeartbeatOnStreamFuture(heartbeat));
        }
      }

      // Don't block until we kick off all the refresh active work RPCs.
      @SuppressWarnings("rawtypes")
      CompletableFuture<Void> parallelFanOutRefreshActiveWork =
          CompletableFuture.allOf(fanOutRefreshActiveWork.toArray(new CompletableFuture[0]));
      parallelFanOutRefreshActiveWork.join();
    } finally {
      activeHeartbeats.set(0);
    }
  }

  private CompletableFuture<Void> sendHeartbeatOnStreamFuture(
      Map.Entry<GetDataStream, Map<String, List<HeartbeatRequest>>> heartbeat) {
    return CompletableFuture.runAsync(
        () -> {
          GetDataStream stream = heartbeat.getKey();
          Map<String, List<HeartbeatRequest>> heartbeatRequests = heartbeat.getValue();
          activeHeartbeats.getAndUpdate(existing -> existing + heartbeat.getValue().size());
          stream.refreshActiveWork(heartbeatRequests);
          // Active heartbeats should never drop below 0.
          activeHeartbeats.getAndUpdate(
              existing -> Math.max(existing - heartbeat.getValue().size(), 0));
        },
        fanOutActiveWorkRefreshExecutor);
  }

  public void printHtml(PrintWriter writer) {
    writer.println("Active Fetches:");
    writer.println("  Side Inputs: " + activeSideInputs.get());
    writer.println("  State Reads: " + activeStateReads.get());
    if (!useStreamingRequests) {
      synchronized (this) {
        writer.println("  Read threads: " + activeReadThreads);
        writer.println("  Pending read batches: " + pendingReadBatches.size());
      }
    }
    writer.println("Heartbeat Keys Active: " + activeHeartbeats.get());
  }

  private static final class ReadBatch {
    ArrayList<QueueEntry> reads = new ArrayList<>();
    SettableFuture<Boolean> startRead = SettableFuture.create();
  }

  private static final class QueueEntry {

    final String computation;
    final Windmill.KeyedGetDataRequest request;
    final SettableFuture<Windmill.KeyedGetDataResponse> response;

    QueueEntry(
        String computation,
        Windmill.KeyedGetDataRequest request,
        SettableFuture<Windmill.KeyedGetDataResponse> response) {
      this.computation = computation;
      this.request = request;
      this.response = response;
    }
  }
}
