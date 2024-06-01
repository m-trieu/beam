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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import static org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream.DEFAULT_STREAM_RPC_DEADLINE_SECONDS;

import com.google.auto.value.AutoBuilder;
import java.io.PrintWriter;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc.CloudWindmillMetadataServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationHeartbeatResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkerMetadataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.AbstractStub;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Creates gRPC streaming connections to Windmill Service. Maintains a set of all currently opened
 * RPC streams for health check/heartbeat requests to keep the streams alive.
 */
@ThreadSafe
@Internal
public class GrpcWindmillStreamFactory implements StatusDataProvider {
  private static final Duration MIN_BACKOFF = Duration.millis(1);
  private static final Duration DEFAULT_MAX_BACKOFF = Duration.standardSeconds(30);
  private static final int DEFAULT_LOG_EVERY_N_STREAM_FAILURES = 1;
  private static final int DEFAULT_STREAMING_RPC_BATCH_LIMIT = Integer.MAX_VALUE;
  private static final int DEFAULT_WINDMILL_MESSAGES_BETWEEN_IS_READY_CHECKS = 1;
  private static final int NO_HEALTH_CHECKS = -1;

  private final JobHeader jobHeader;
  private final int logEveryNStreamFailures;
  private final int streamingRpcBatchLimit;
  private final int windmillMessagesBetweenIsReadyChecks;
  private final Supplier<BackOff> grpcBackOff;
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry;
  private final AtomicLong streamIdGenerator;
  // If true, then active work refreshes will be sent as KeyedGetDataRequests. Otherwise, use the
  // newer ComputationHeartbeatRequests.
  private final boolean sendKeyedGetDataRequests;
  private final Consumer<List<ComputationHeartbeatResponse>> processHeartbeatResponses;

  private GrpcWindmillStreamFactory(
      JobHeader jobHeader,
      int logEveryNStreamFailures,
      int streamingRpcBatchLimit,
      int windmillMessagesBetweenIsReadyChecks,
      boolean sendKeyedGetDataRequests,
      Consumer<List<ComputationHeartbeatResponse>> processHeartbeatResponses,
      Supplier<Duration> maxBackOffSupplier) {
    this.jobHeader = jobHeader;
    this.logEveryNStreamFailures = logEveryNStreamFailures;
    this.streamingRpcBatchLimit = streamingRpcBatchLimit;
    this.windmillMessagesBetweenIsReadyChecks = windmillMessagesBetweenIsReadyChecks;
    // Configure backoff to retry calls forever, with a maximum sane retry interval.
    this.grpcBackOff =
        Suppliers.memoize(
            () ->
                FluentBackoff.DEFAULT
                    .withInitialBackoff(MIN_BACKOFF)
                    .withMaxBackoff(maxBackOffSupplier.get())
                    .backoff());
    this.streamRegistry = ConcurrentHashMap.newKeySet();
    this.sendKeyedGetDataRequests = sendKeyedGetDataRequests;
    this.processHeartbeatResponses = processHeartbeatResponses;
    this.streamIdGenerator = new AtomicLong();
  }

  /** @implNote Used for {@link AutoBuilder} {@link Builder} class, do not call directly. */
  static GrpcWindmillStreamFactory create(
      JobHeader jobHeader,
      int logEveryNStreamFailures,
      int streamingRpcBatchLimit,
      int windmillMessagesBetweenIsReadyChecks,
      boolean sendKeyedGetDataRequests,
      Consumer<List<ComputationHeartbeatResponse>> processHeartbeatResponses,
      Supplier<Duration> maxBackOffSupplier,
      int healthCheckIntervalMillis) {
    GrpcWindmillStreamFactory streamFactory =
        new GrpcWindmillStreamFactory(
            jobHeader,
            logEveryNStreamFailures,
            streamingRpcBatchLimit,
            windmillMessagesBetweenIsReadyChecks,
            sendKeyedGetDataRequests,
            processHeartbeatResponses,
            maxBackOffSupplier);

    if (healthCheckIntervalMillis >= 0) {
      // Health checks are run on background daemon thread, which will only be cleaned up on JVM
      // shutdown.
      new Timer("WindmillHealthCheckTimer")
          .schedule(
              new TimerTask() {
                @Override
                public void run() {
                  Instant reportThreshold =
                      Instant.now().minus(Duration.millis(healthCheckIntervalMillis));
                  for (AbstractWindmillStream<?, ?> stream : streamFactory.streamRegistry) {
                    stream.maybeSendHealthCheck(reportThreshold);
                  }
                }
              },
              0,
              healthCheckIntervalMillis);
    }

    return streamFactory;
  }

  /**
   * Returns a new {@link Builder} for {@link GrpcWindmillStreamFactory} with default values set for
   * the given {@link JobHeader}.
   */
  public static GrpcWindmillStreamFactory.Builder of(JobHeader jobHeader) {
    return new AutoBuilder_GrpcWindmillStreamFactory_Builder()
        .setJobHeader(jobHeader)
        .setWindmillMessagesBetweenIsReadyChecks(DEFAULT_WINDMILL_MESSAGES_BETWEEN_IS_READY_CHECKS)
        .setMaxBackOffSupplier(() -> DEFAULT_MAX_BACKOFF)
        .setLogEveryNStreamFailures(DEFAULT_LOG_EVERY_N_STREAM_FAILURES)
        .setStreamingRpcBatchLimit(DEFAULT_STREAMING_RPC_BATCH_LIMIT)
        .setHealthCheckIntervalMillis(NO_HEALTH_CHECKS)
        .setSendKeyedGetDataRequests(true)
        .setProcessHeartbeatResponses(ignored -> {});
  }

  private static <T extends AbstractStub<T>> T withDefaultDeadline(T stub) {
    // Deadlines are absolute points in time, so generate a new one everytime this function is
    // called.
    return stub.withDeadlineAfter(
        AbstractWindmillStream.DEFAULT_STREAM_RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);
  }

  public GetWorkStream createGetWorkStream(
      CloudWindmillServiceV1Alpha1Stub stub,
      GetWorkRequest request,
      ThrottleTimer getWorkThrottleTimer,
      WorkItemReceiver processWorkItem) {
    return GrpcGetWorkStream.create(
        responseObserver -> withDefaultDeadline(stub).getWorkStream(responseObserver),
        request,
        grpcBackOff.get(),
        newBufferringStreamObserverFactory(),
        streamRegistry,
        logEveryNStreamFailures,
        getWorkThrottleTimer,
        processWorkItem);
  }

  public GetWorkStream createDirectGetWorkStream(
      CloudWindmillServiceV1Alpha1Stub stub,
      GetWorkRequest request,
      ThrottleTimer getWorkThrottleTimer,
      Supplier<GetDataStream> getDataStream,
      Supplier<WorkCommitter> workCommitter,
      Function<
              GetDataStream,
              BiFunction<String, Windmill.KeyedGetDataRequest, Windmill.KeyedGetDataResponse>>
          keyedGetDataFn,
      WorkItemScheduler workItemScheduler) {
    return GrpcDirectGetWorkStream.create(
        stub::getWorkStream,
        request,
        grpcBackOff.get(),
        newSimpleStreamObserverFactory(),
        streamRegistry,
        logEveryNStreamFailures,
        getWorkThrottleTimer,
        getDataStream,
        workCommitter,
        keyedGetDataFn,
        workItemScheduler);
  }

  public GetDataStream createDirectGetDataStream(
      CloudWindmillServiceV1Alpha1Stub stub,
      ThrottleTimer getDataThrottleTimer,
      boolean sendKeyedGetDataRequests,
      Consumer<List<ComputationHeartbeatResponse>> processHeartbeatResponses) {
    return createGetDataStream(
        () -> stub,
        getDataThrottleTimer,
        sendKeyedGetDataRequests,
        processHeartbeatResponses,
        newSimpleStreamObserverFactory());
  }

  public GetDataStream createGetDataStream(
      CloudWindmillServiceV1Alpha1Stub stub, ThrottleTimer getDataThrottleTimer) {
    return GrpcGetDataStream.create(
        responseObserver -> stub.get().getDataStream(responseObserver),
        grpcBackOff.get(),
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        getDataThrottleTimer,
        jobHeader,
        streamIdGenerator,
        streamingRpcBatchLimit,
        sendKeyedGetDataRequests,
        processHeartbeatResponses);
  }

  public CommitWorkStream createCommitWorkStream(
      CloudWindmillServiceV1Alpha1Stub stub, ThrottleTimer commitWorkThrottleTimer) {
    return createCommitWorkStream(
        () -> withDefaultDeadline(stub),
        commitWorkThrottleTimer,
        newBufferringStreamObserverFactory());
  }

  public CommitWorkStream createDirectCommitWorkStream(
      CloudWindmillServiceV1Alpha1Stub stub, ThrottleTimer commitWorkThrottleTimer) {
    return createCommitWorkStream(
        () -> stub, commitWorkThrottleTimer, newSimpleStreamObserverFactory());
  }

  private CommitWorkStream createCommitWorkStream(
      Supplier<CloudWindmillServiceV1Alpha1Stub> stub,
      ThrottleTimer commitWorkThrottleTimer,
      StreamObserverFactory streamObserverFactory) {
    return GrpcCommitWorkStream.create(
        responseObserver -> stub.get().commitWorkStream(responseObserver),
        grpcBackOff.get(),
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        commitWorkThrottleTimer,
        jobHeader,
        streamIdGenerator,
        streamingRpcBatchLimit);
  }

  public GetWorkerMetadataStream createGetWorkerMetadataStream(
      CloudWindmillMetadataServiceV1Alpha1Stub stub,
      ThrottleTimer getWorkerMetadataThrottleTimer,
      Consumer<WindmillEndpoints> onNewWindmillEndpoints) {
    return GrpcGetWorkerMetadataStream.create(
        responseObserver -> withDefaultDeadline(stub).getWorkerMetadata(responseObserver),
        grpcBackOff.get(),
        newBufferringStreamObserverFactory(),
        streamRegistry,
        logEveryNStreamFailures,
        jobHeader,
        0,
        getWorkerMetadataThrottleTimer,
        onNewWindmillEndpoints);
  }

  private StreamObserverFactory newBufferringStreamObserverFactory() {
    return StreamObserverFactory.direct(
        DEFAULT_STREAM_RPC_DEADLINE_SECONDS * 2, windmillMessagesBetweenIsReadyChecks);
  }

  /**
   * Simple {@link StreamObserverFactory} that does not buffer or provide extra functionality for
   * request observers.
   *
   * @implNote Used to create stream observers for direct path streams that do not share any
   *     underlying resources between threads.
   */
  private StreamObserverFactory newSimpleStreamObserverFactory() {
    return new StreamObserverFactory() {
      @Override
      public <ResponseT, RequestT> StreamObserver<RequestT> from(
          Function<StreamObserver<ResponseT>, StreamObserver<RequestT>> clientFactory,
          StreamObserver<ResponseT> responseObserver) {
        return clientFactory.apply(responseObserver);
      }
    };
  }

  /**
   * Schedules streaming RPC health checks to run on a background daemon thread, which will be
   * cleaned up when the JVM shutdown.
   */
  public void scheduleHealthChecks(int healthCheckInterval) {
    if (healthCheckInterval < 0) {
      return;
    }

    new Timer("WindmillHealthCheckTimer")
        .schedule(
            new TimerTask() {
              @Override
              public void run() {
                Instant reportThreshold = Instant.now().minus(Duration.millis(healthCheckInterval));
                for (AbstractWindmillStream<?, ?> stream : streamRegistry) {
                  stream.maybeSendHealthCheck(reportThreshold);
                }
              }
            },
            0,
            healthCheckInterval);
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    writer.write("Active Streams:<br>");
    for (AbstractWindmillStream<?, ?> stream : streamRegistry) {
      stream.appendSummaryHtml(writer);
      writer.write("<br>");
    }
  }

  @Internal
  @AutoBuilder(callMethod = "create")
  public interface Builder {
    Builder setJobHeader(JobHeader jobHeader);

    Builder setLogEveryNStreamFailures(int logEveryNStreamFailures);

    Builder setStreamingRpcBatchLimit(int streamingRpcBatchLimit);

    Builder setWindmillMessagesBetweenIsReadyChecks(int windmillMessagesBetweenIsReadyChecks);

    Builder setMaxBackOffSupplier(Supplier<Duration> maxBackOff);

    Builder setSendKeyedGetDataRequests(boolean sendKeyedGetDataRequests);

    Builder setProcessHeartbeatResponses(
        Consumer<List<ComputationHeartbeatResponse>> processHeartbeatResponses);

    Builder setHealthCheckIntervalMillis(int healthCheckIntervalMillis);

    GrpcWindmillStreamFactory build();
  }
}
