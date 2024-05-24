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

import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.remoteChannel;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.MapTask;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.core.metrics.MetricsLogger;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.WorkHeartbeatResponseProcessor;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingApplianceComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEngineComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEnginePipelineConfig;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusReporter;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.appliance.JniWindmillApplianceServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.CompleteCommit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingApplianceWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingEngineWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getwork.ApplianceGetWorkDispatcher;
import org.apache.beam.runners.dataflow.worker.windmill.client.getwork.GetWorkDispatcher;
import org.apache.beam.runners.dataflow.worker.windmill.client.getwork.StreamingEngineGetWorkDispatcher;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.ChannelzServlet;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCache;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingRemoteStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.IsolationChannel;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingApplianceFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingEngineFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefresher;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefreshers;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.JvmInitializers;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySinkMetrics;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.*;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements a Streaming Dataflow worker. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StreamingDataflowWorker {

  // TODO(https://github.com/apache/beam/issues/19632): Update throttling counters to use generic
  // throttling-msecs metric.
  public static final MetricName BIGQUERY_STREAMING_INSERT_THROTTLE_TIME =
      MetricName.named(
          "org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl$DatasetServiceImpl",
          "throttling-msecs");
  // Maximum number of threads for processing. Currently each thread processes one key at a time.
  static final int MAX_PROCESSING_THREADS = 300;
  static final long THREAD_EXPIRATION_TIME_SEC = 60;
  static final int NUM_COMMIT_STREAMS = 1;
  static final Duration COMMIT_STREAM_TIMEOUT = Duration.standardMinutes(1);

  /**
   * Sinks are marked 'full' in {@link StreamingModeExecutionContext} once the amount of data sinked
   * (across all the sinks, if there are more than one) reaches this limit. This serves as hint for
   * readers to stop producing more. This can be disabled with 'disable_limiting_bundle_sink_bytes'
   * experiment.
   */
  public static final int MAX_SINK_BYTES = 10_000_000;

  private static final Logger LOG = LoggerFactory.getLogger(StreamingDataflowWorker.class);
  /** The idGenerator to generate unique id globally. */
  private static final IdGenerator ID_GENERATOR = IdGenerators.decrementingLongs();

  private static final int DEFAULT_STATUS_PORT = 8081;
  // Maximum size of the result of a GetWork request.
  private static final long MAX_GET_WORK_FETCH_BYTES = 64L << 20; // 64m
  /** Maximum number of failure stacktraces to report in each update sent to backend. */
  private static final int MAX_FAILURES_TO_REPORT_IN_UPDATE = 1000;

  private static final Random clientIdGenerator = new Random();
  private static final String CHANNELZ_PATH = "/channelz";

  @VisibleForTesting final WindmillStateCache stateCache;
  private final StreamingWorkerStatusPages statusPages;
  private final ComputationConfig.Fetcher configFetcher;
  private final ComputationStateCache computationStateCache;
  private final BoundedQueueExecutor workUnitExecutor;
  private final AtomicBoolean running = new AtomicBoolean();
  private final DataflowWorkerHarnessOptions options;
  private final MetricTrackingWindmillServerStub metricTrackingWindmillServer;
  private final MemoryMonitor memoryMonitor;
  private final ReaderCache readerCache;
  private final DataflowExecutionStateSampler sampler = DataflowExecutionStateSampler.instance();
  private final ActiveWorkRefresher activeWorkRefresher;
  private final WorkCommitter workCommitter;
  private final StreamingWorkerStatusReporter workerStatusReporter;
  private final StreamingCounters streamingCounters;
  private final GetWorkDispatcher getWorkDispatcher;
  private final ExecutorService memoryMonitorExecutor;

  private StreamingDataflowWorker(
      WindmillServerStub windmillServer,
      long clientId,
      ComputationConfig.Fetcher configFetcher,
      ComputationStateCache computationStateCache,
      WindmillStateCache windmillStateCache,
      BoundedQueueExecutor workUnitExecutor,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      DataflowWorkerHarnessOptions options,
      HotKeyLogger hotKeyLogger,
      Supplier<Instant> clock,
      StreamingWorkerStatusReporter workerStatusReporter,
      FailureTracker failureTracker,
      WorkFailureProcessor workFailureProcessor,
      StreamingCounters streamingCounters,
      MemoryMonitor memoryMonitor,
      AtomicInteger maxWorkItemCommitBytes,
      GrpcWindmillStreamFactory windmillStreamFactory,
      Function<String, ScheduledExecutorService> executorSupplier,
      ConcurrentMap<String, StageInfo> stageInfoMap) {
    this.configFetcher = configFetcher;
    this.computationStateCache = computationStateCache;
    this.stateCache = windmillStateCache;
    this.readerCache =
        new ReaderCache(
            Duration.standardSeconds(options.getReaderCacheTimeoutSec()),
            Executors.newCachedThreadPool());
    this.options = options;
    boolean windmillServiceEnabled = options.isEnableStreamingEngine();
    int numCommitThreads = 1;
    if (windmillServiceEnabled && options.getWindmillServiceCommitThreads() > 0) {
      numCommitThreads = options.getWindmillServiceCommitThreads();
    }

    this.workCommitter =
        windmillServiceEnabled
            ? StreamingEngineWorkCommitter.create(
                WindmillStreamPool.create(
                        NUM_COMMIT_STREAMS, COMMIT_STREAM_TIMEOUT, windmillServer::commitWorkStream)
                    ::getCloseableStream,
                numCommitThreads,
                this::onCompleteCommit)
            : StreamingApplianceWorkCommitter.create(
                windmillServer::commitWork, this::onCompleteCommit);

    this.workUnitExecutor = workUnitExecutor;

    this.memoryMonitorExecutor =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setPriority(Thread.MIN_PRIORITY)
                .setNameFormat("MemoryMonitor")
                .build());

    this.metricTrackingWindmillServer =
        MetricTrackingWindmillServerStub.builder(windmillServer, memoryMonitor)
            .setUseStreamingRequests(windmillServiceEnabled)
            .setUseSeparateHeartbeatStreams(options.getUseSeparateWindmillHeartbeatStreams())
            .setNumGetDataStreams(options.getWindmillGetDataStreamCount())
            .build();

    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);

    int stuckCommitDurationMillis =
        windmillServiceEnabled && options.getStuckCommitDurationMillis() > 0
            ? options.getStuckCommitDurationMillis()
            : 0;
    this.activeWorkRefresher =
        ActiveWorkRefreshers.createDispatchedActiveWorkRefresher(
            clock,
            options.getActiveWorkRefreshPeriodMillis(),
            stuckCommitDurationMillis,
            computationStateCache::getAllPresentComputations,
            sampler,
            metricTrackingWindmillServer::refreshActiveWork,
            executorSupplier.apply("RefreshWork"));

    WorkerStatusPages workerStatusPages =
        WorkerStatusPages.create(DEFAULT_STATUS_PORT, memoryMonitor);
    StreamingWorkerStatusPages.Builder statusPagesBuilder =
        StreamingWorkerStatusPages.builder()
            .setClock(clock)
            .setClientId(clientId)
            .setIsRunning(running)
            .setStatusPages(workerStatusPages)
            .setStateCache(stateCache)
            .setComputationStateCache(computationStateCache)
            .setCurrentActiveCommitBytes(workCommitter::currentActiveCommitBytes)
            .setGetDataStatusProvider(metricTrackingWindmillServer::printHtml)
            .setWorkUnitExecutor(workUnitExecutor);

    this.statusPages =
        windmillServiceEnabled
            ? statusPagesBuilder
                .setDebugCapture(
                    new DebugCapture.Manager(options, workerStatusPages.getDebugCapturePages()))
                .setChannelzServlet(new ChannelzServlet(CHANNELZ_PATH, options, windmillServer))
                .setWindmillStreamFactory(windmillStreamFactory)
                .build()
            : statusPagesBuilder.build();

    this.workerStatusReporter = workerStatusReporter;
    this.streamingCounters = streamingCounters;
    this.memoryMonitor = memoryMonitor;

    StreamingWorkScheduler streamingWorkScheduler =
        StreamingWorkScheduler.create(
            options,
            clock,
            readerCache,
            mapTaskExecutorFactory,
            workUnitExecutor,
            stateCache::forComputation,
            metricTrackingWindmillServer::getSideInputData,
            failureTracker,
            workFailureProcessor,
            streamingCounters,
            hotKeyLogger,
            sampler,
            maxWorkItemCommitBytes,
            ID_GENERATOR,
            stageInfoMap);

    Supplier<GetWorkRequest> getWorkRequest =
        () ->
            GetWorkRequest.newBuilder()
                .setClientId(clientId)
                .setMaxItems(chooseMaximumBundlesOutstanding())
                .setMaxBytes(MAX_GET_WORK_FETCH_BYTES)
                .build();
    Function<String, Work.ProcessingContext.WithProcessWorkFn> processingContextFactory =
        computationId ->
            Work.createProcessingContext(computationId, metricTrackingWindmillServer::getStateData)
                .setWorkCommitter(workCommitter::commit);
    this.getWorkDispatcher =
        options.isEnableStreamingEngine()
            ? new StreamingEngineGetWorkDispatcher(
                workItemReceiver ->
                    windmillServer.getWorkStream(getWorkRequest.get(), workItemReceiver),
                memoryMonitor,
                computationStateCache::get,
                processingContextFactory,
                streamingWorkScheduler)
            : new ApplianceGetWorkDispatcher(
                memoryMonitor,
                computationStateCache::get,
                processingContextFactory,
                streamingWorkScheduler,
                () -> windmillServer.getWork(getWorkRequest.get()));

    LOG.debug("windmillServiceEnabled: {}", windmillServiceEnabled);
    LOG.debug("WindmillServiceEndpoint: {}", options.getWindmillServiceEndpoint());
    LOG.debug("WindmillServicePort: {}", options.getWindmillServicePort());
    LOG.debug("LocalWindmillHostport: {}", options.getLocalWindmillHostport());
    LOG.debug("maxWorkItemCommitBytes: {}", maxWorkItemCommitBytes.get());
  }

  public static StreamingDataflowWorker fromOptions(DataflowWorkerHarnessOptions options) {
    long clientId = clientIdGenerator.nextLong();
    MemoryMonitor memoryMonitor = MemoryMonitor.fromOptions(options);
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    StreamingCounters streamingCounters = StreamingCounters.create();
    WorkUnitClient dataflowServiceClient = new DataflowWorkUnitClient(options, LOG);
    BoundedQueueExecutor workExecutor = createWorkUnitExecutor(options);
    AtomicInteger maxWorkItemCommitBytes = new AtomicInteger(Integer.MAX_VALUE);
    WindmillStateCache windmillStateCache =
        WindmillStateCache.ofSizeMbs(options.getWorkerCacheMb());
    Function<String, ScheduledExecutorService> executorSupplier =
        threadName ->
            Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(threadName).build());
    GrpcWindmillStreamFactory windmillStreamFactory =
        createWindmillStreamFactory(options, clientId);
    GrpcDispatcherClient dispatcherClient = GrpcDispatcherClient.create(createStubFactory(options));

    // If ComputationConfig.Fetcher is the Streaming Appliance implementation, WindmillServerStub
    // can be created without a heartbeat response processor, as appliance does not send heartbeats.
    Pair<ComputationConfig.Fetcher, Optional<WindmillServerStub>> configFetcherAndWindmillClient =
        createConfigFetcherAndWindmillClient(
            options,
            dataflowServiceClient,
            dispatcherClient,
            maxWorkItemCommitBytes,
            windmillStreamFactory);

    ComputationStateCache computationStateCache =
        ComputationStateCache.create(
            configFetcherAndWindmillClient.getLeft(),
            workExecutor,
            windmillStateCache::forComputation,
            ID_GENERATOR);

    // If WindmillServerStub is not present, it is a Streaming Engine job. We now have all the
    // components created to initialize the GrpcWindmillServer.
    WindmillServerStub windmillServer =
        configFetcherAndWindmillClient
            .getRight()
            .orElseGet(
                () ->
                    GrpcWindmillServer.create(
                        options,
                        windmillStreamFactory,
                        dispatcherClient,
                        new WorkHeartbeatResponseProcessor(computationStateCache::get)));

    FailureTracker failureTracker =
        options.isEnableStreamingEngine()
            ? StreamingEngineFailureTracker.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE, options.getMaxStackTraceDepthToReport())
            : StreamingApplianceFailureTracker.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE,
                options.getMaxStackTraceDepthToReport(),
                windmillServer::reportStats);

    Supplier<Instant> clock = Instant::now;
    WorkFailureProcessor workFailureProcessor =
        WorkFailureProcessor.create(
            workExecutor,
            failureTracker,
            () -> Optional.ofNullable(memoryMonitor.tryToDumpHeap()),
            clock);
    StreamingWorkerStatusReporter workerStatusReporter =
        StreamingWorkerStatusReporter.create(
            dataflowServiceClient,
            windmillServer::getAndResetThrottleTime,
            stageInfo::values,
            failureTracker,
            streamingCounters,
            memoryMonitor,
            workExecutor,
            options.getWindmillHarnessUpdateReportingPeriod().getMillis(),
            options.getPerWorkerMetricsUpdateReportingPeriodMillis());

    return new StreamingDataflowWorker(
        windmillServer,
        clientId,
        configFetcherAndWindmillClient.getLeft(),
        computationStateCache,
        WindmillStateCache.ofSizeMbs(options.getWorkerCacheMb()),
        workExecutor,
        IntrinsicMapTaskExecutorFactory.defaultFactory(),
        options,
        new HotKeyLogger(),
        clock,
        workerStatusReporter,
        failureTracker,
        workFailureProcessor,
        streamingCounters,
        memoryMonitor,
        maxWorkItemCommitBytes,
        windmillStreamFactory,
        executorSupplier,
        stageInfo);
  }

  private static Pair<ComputationConfig.Fetcher, Optional<WindmillServerStub>>
      createConfigFetcherAndWindmillClient(
          DataflowWorkerHarnessOptions options,
          WorkUnitClient dataflowServiceClient,
          GrpcDispatcherClient dispatcherClient,
          AtomicInteger maxWorkItemCommitBytes,
          GrpcWindmillStreamFactory windmillStreamFactory) {
    ComputationConfig.Fetcher configFetcher;
    @Nullable WindmillServerStub windmillServer = null;
    if (options.isEnableStreamingEngine()) {
      configFetcher =
          StreamingEngineComputationConfigFetcher.create(
              options.getGlobalConfigRefreshPeriod().getMillis(),
              dataflowServiceClient,
              config ->
                  onPipelineConfig(
                      config,
                      dispatcherClient::consumeWindmillDispatcherEndpoints,
                      maxWorkItemCommitBytes));
    } else {
      windmillServer =
          createWindmillServerStub(options, windmillStreamFactory, dispatcherClient, ignored -> {});
      configFetcher = new StreamingApplianceComputationConfigFetcher(windmillServer::getConfig);
    }

    return Pair.of(configFetcher, Optional.ofNullable(windmillServer));
  }

  @VisibleForTesting
  static StreamingDataflowWorker forTesting(
      Map<String, String> prePopulatedStateNameMappings,
      WindmillServerStub windmillServer,
      List<MapTask> mapTasks,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      WorkUnitClient workUnitClient,
      DataflowWorkerHarnessOptions options,
      boolean publishCounters,
      HotKeyLogger hotKeyLogger,
      Supplier<Instant> clock,
      Function<String, ScheduledExecutorService> executorSupplier,
      int localRetryTimeoutMs,
      int maxWorkItemCommitBytesOverrides) {
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    AtomicInteger maxWorkItemCommitBytes = new AtomicInteger(maxWorkItemCommitBytesOverrides);
    BoundedQueueExecutor workExecutor = createWorkUnitExecutor(options);
    WindmillStateCache stateCache = WindmillStateCache.ofSizeMbs(options.getWorkerCacheMb());
    ComputationConfig.Fetcher configFetcher =
        options.isEnableStreamingEngine()
            ? StreamingEngineComputationConfigFetcher.forTesting(
                /* hasReceivedGlobalConfig= */ true,
                options.getGlobalConfigRefreshPeriod().getMillis(),
                workUnitClient,
                executorSupplier,
                config ->
                    onPipelineConfig(
                        config,
                        windmillServer::setWindmillServiceEndpoints,
                        maxWorkItemCommitBytes))
            : new StreamingApplianceComputationConfigFetcher(windmillServer::getConfig);
    ConcurrentMap<String, String> stateNameMap =
        new ConcurrentHashMap<>(prePopulatedStateNameMappings);
    ComputationStateCache computationStateCache =
        ComputationStateCache.forTesting(
            configFetcher, workExecutor, stateCache::forComputation, ID_GENERATOR, stateNameMap);
    computationStateCache.loadCacheForTesting(
        mapTasks,
        mapTask ->
            new ComputationState(
                mapTask.getStageName(),
                mapTask,
                workExecutor,
                stateNameMap,
                stateCache.forComputation(mapTask.getStageName())));
    MemoryMonitor memoryMonitor = MemoryMonitor.fromOptions(options);
    StreamingCounters streamingCounters = StreamingCounters.create();
    FailureTracker failureTracker =
        options.isEnableStreamingEngine()
            ? StreamingEngineFailureTracker.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE, options.getMaxStackTraceDepthToReport())
            : StreamingApplianceFailureTracker.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE,
                options.getMaxStackTraceDepthToReport(),
                windmillServer::reportStats);
    WorkFailureProcessor workFailureProcessor =
        WorkFailureProcessor.forTesting(
            workExecutor,
            failureTracker,
            () -> Optional.ofNullable(memoryMonitor.tryToDumpHeap()),
            clock,
            localRetryTimeoutMs);
    StreamingWorkerStatusReporter workerStatusReporter =
        StreamingWorkerStatusReporter.forTesting(
            publishCounters,
            workUnitClient,
            windmillServer::getAndResetThrottleTime,
            stageInfo::values,
            failureTracker,
            streamingCounters,
            memoryMonitor,
            workExecutor,
            executorSupplier,
            options.getWindmillHarnessUpdateReportingPeriod().getMillis(),
            options.getPerWorkerMetricsUpdateReportingPeriodMillis());

    return new StreamingDataflowWorker(
        windmillServer,
        1L,
        configFetcher,
        computationStateCache,
        stateCache,
        workExecutor,
        mapTaskExecutorFactory,
        options,
        hotKeyLogger,
        clock,
        workerStatusReporter,
        failureTracker,
        workFailureProcessor,
        streamingCounters,
        memoryMonitor,
        maxWorkItemCommitBytes,
        createWindmillStreamFactory(options, 1),
        executorSupplier,
        stageInfo);
  }

  private static void onPipelineConfig(
      StreamingEnginePipelineConfig config,
      Consumer<ImmutableSet<HostAndPort>> consumeWindmillServiceEndpoints,
      AtomicInteger maxWorkItemCommitBytes) {
    if (config.maxWorkItemCommitBytes() != maxWorkItemCommitBytes.get()) {
      LOG.info("Setting maxWorkItemCommitBytes to {}", maxWorkItemCommitBytes);
      maxWorkItemCommitBytes.set((int) config.maxWorkItemCommitBytes());
    }

    if (!config.windmillServiceEndpoints().isEmpty()) {
      consumeWindmillServiceEndpoints.accept(config.windmillServiceEndpoints());
    }
  }

  private static GrpcWindmillStreamFactory createWindmillStreamFactory(
      DataflowWorkerHarnessOptions options, long clientId) {
    Duration maxBackoff =
        !options.isEnableStreamingEngine() && options.getLocalWindmillHostport() != null
            ? GrpcWindmillServer.LOCALHOST_MAX_BACKOFF
            : Duration.millis(options.getWindmillServiceStreamMaxBackoffMillis());
    return GrpcWindmillStreamFactory.of(
            JobHeader.newBuilder()
                .setJobId(options.getJobId())
                .setProjectId(options.getProject())
                .setWorkerId(options.getWorkerId())
                .setClientId(clientId)
                .build())
        .setWindmillMessagesBetweenIsReadyChecks(options.getWindmillMessagesBetweenIsReadyChecks())
        .setMaxBackOffSupplier(() -> maxBackoff)
        .setLogEveryNStreamFailures(options.getWindmillServiceStreamingLogEveryNStreamFailures())
        .setStreamingRpcBatchLimit(options.getWindmillServiceStreamingRpcBatchLimit())
        .build();
  }

  private static BoundedQueueExecutor createWorkUnitExecutor(DataflowWorkerHarnessOptions options) {
    return new BoundedQueueExecutor(
        chooseMaxThreads(options),
        THREAD_EXPIRATION_TIME_SEC,
        TimeUnit.SECONDS,
        chooseMaxBundlesOutstanding(options),
        chooseMaxBytesOutstanding(options),
        new ThreadFactoryBuilder().setNameFormat("DataflowWorkUnits-%d").setDaemon(true).build());
  }

  public static void main(String[] args) throws Exception {
    JvmInitializers.runOnStartup();

    DataflowWorkerHarnessHelper.initializeLogging(StreamingDataflowWorker.class);
    DataflowWorkerHarnessOptions options =
        DataflowWorkerHarnessHelper.initializeGlobalStateAndPipelineOptions(
            StreamingDataflowWorker.class, DataflowWorkerHarnessOptions.class);
    DataflowWorkerHarnessHelper.configureLogging(options);
    checkArgument(
        options.isStreaming(),
        "%s instantiated with options indicating batch use",
        StreamingDataflowWorker.class.getName());

    checkArgument(
        !DataflowRunner.hasExperiment(options, "beam_fn_api"),
        "%s cannot be main() class with beam_fn_api enabled",
        StreamingDataflowWorker.class.getSimpleName());

    CoderTranslation.verifyModelCodersRegistered();

    LOG.debug("Creating StreamingDataflowWorker from options: {}", options);
    StreamingDataflowWorker worker = StreamingDataflowWorker.fromOptions(options);

    // Use the MetricsLogger container which is used by BigQueryIO to periodically log process-wide
    // metrics.
    MetricsEnvironment.setProcessWideContainer(new MetricsLogger(null));

    if (options.isEnableStreamingEngine()
        && !DataflowRunner.hasExperiment(options, "disable_per_worker_metrics")) {
      enableBigQueryMetrics();
    }

    JvmInitializers.runBeforeProcessing(options);
    worker.startStatusPages();
    worker.start();
  }

  private static WindmillServerStub createWindmillServerStub(
      DataflowWorkerHarnessOptions options,
      GrpcWindmillStreamFactory windmillStreamFactory,
      GrpcDispatcherClient dispatcherClient,
      Consumer<List<Windmill.ComputationHeartbeatResponse>> processHeartbeatResponses) {
    if (options.getWindmillServiceEndpoint() != null
        || options.isEnableStreamingEngine()
        || options.getLocalWindmillHostport().startsWith("grpc:")) {
      windmillStreamFactory.scheduleHealthChecks(
          options.getWindmillServiceStreamingRpcHealthCheckPeriodMs());
      return GrpcWindmillServer.create(
          options, windmillStreamFactory, dispatcherClient, processHeartbeatResponses);
    } else {
      return new JniWindmillApplianceServer(options.getLocalWindmillHostport());
    }
  }

  private static ChannelCachingStubFactory createStubFactory(
      DataflowWorkerHarnessOptions workerOptions) {
    Function<WindmillServiceAddress, ManagedChannel> channelFactory =
        serviceAddress ->
            remoteChannel(
                serviceAddress, workerOptions.getWindmillServiceRpcChannelAliveTimeoutSec());
    ChannelCache channelCache =
        ChannelCache.create(
            serviceAddress ->
                // IsolationChannel will create and manage separate RPC channels to the same
                // serviceAddress via calling the channelFactory, else just directly return the
                // RPC channel.
                workerOptions.getUseWindmillIsolatedChannels()
                    ? IsolationChannel.create(() -> channelFactory.apply(serviceAddress))
                    : channelFactory.apply(serviceAddress));
    return ChannelCachingRemoteStubFactory.create(workerOptions.getGcpCredential(), channelCache);
  }

  private static int chooseMaxThreads(DataflowWorkerHarnessOptions options) {
    if (options.getNumberOfWorkerHarnessThreads() != 0) {
      return options.getNumberOfWorkerHarnessThreads();
    }
    return MAX_PROCESSING_THREADS;
  }

  private static int chooseMaxBundlesOutstanding(DataflowWorkerHarnessOptions options) {
    int maxBundles = options.getMaxBundlesFromWindmillOutstanding();
    return maxBundles > 0 ? maxBundles : chooseMaxThreads(options) + 100;
  }

  private static long chooseMaxBytesOutstanding(DataflowWorkerHarnessOptions options) {
    long maxMem = options.getMaxBytesFromWindmillOutstanding();
    return maxMem > 0 ? maxMem : (Runtime.getRuntime().maxMemory() / 2);
  }

  private static void enableBigQueryMetrics() {
    // When enabled, the Pipeline will record Per-Worker metrics that will be piped to DFE.
    StreamingStepMetricsContainer.setEnablePerWorkerMetrics(true);
    // StreamingStepMetricsContainer automatically deletes perWorkerCounters if they are zero-valued
    // for longer than 5 minutes.
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    // Support metrics for BigQuery's Streaming Inserts write method.
    BigQuerySinkMetrics.setSupportStreamingInsertsMetrics(true);
  }

  @VisibleForTesting
  final void reportPeriodicWorkerUpdatesForTest() {
    workerStatusReporter.reportPeriodicWorkerUpdates();
  }

  private int chooseMaximumNumberOfThreads() {
    if (options.getNumberOfWorkerHarnessThreads() != 0) {
      return options.getNumberOfWorkerHarnessThreads();
    }
    return MAX_PROCESSING_THREADS;
  }

  private int chooseMaximumBundlesOutstanding() {
    int maxBundles = options.getMaxBundlesFromWindmillOutstanding();
    if (maxBundles > 0) {
      return maxBundles;
    }
    return chooseMaximumNumberOfThreads() + 100;
  }

  @VisibleForTesting
  public boolean workExecutorIsEmpty() {
    return workUnitExecutor.executorQueueIsEmpty();
  }

  @VisibleForTesting
  int numCommitThreads() {
    return workCommitter.parallelism();
  }

  @VisibleForTesting
  ComputationStateCache getComputationStateCache() {
    return computationStateCache;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    running.set(true);

    configFetcher.start();

    memoryMonitorExecutor.submit(memoryMonitor);
    getWorkDispatcher.start();
    sampler.start();

    workCommitter.start();
    workerStatusReporter.start();
    activeWorkRefresher.start();
  }

  /** Starts the status page server for debugging. May be omitted for lighter weight testing. */
  private void startStatusPages() {
    statusPages.start(options);
  }

  @VisibleForTesting
  void stop() {
    try {
      configFetcher.stop();

      activeWorkRefresher.stop();
      statusPages.stop();
      running.set(false);
      getWorkDispatcher.stop();

      workCommitter.stop();
      memoryMonitor.stop();
      memoryMonitorExecutor.shutdown();
      workUnitExecutor.shutdown();

      computationStateCache.closeAndInvalidateAll();

      workerStatusReporter.stop();
    } catch (Exception e) {
      LOG.warn("Exception while shutting down: ", e);
    }
  }

  private void onCompleteCommit(CompleteCommit completeCommit) {
    if (completeCommit.status() != Windmill.CommitStatus.OK) {
      readerCache.invalidateReader(
          WindmillComputationKey.create(
              completeCommit.computationId(), completeCommit.shardedKey()));
      stateCache
          .forComputation(completeCommit.computationId())
          .invalidate(completeCommit.shardedKey());
    }

    computationStateCache
        .getIfPresent(completeCommit.computationId())
        .ifPresent(
            state ->
                state.completeWorkAndScheduleNextWorkForKey(
                    completeCommit.shardedKey(), completeCommit.workId()));
  }

  @VisibleForTesting
  public Iterable<CounterUpdate> buildCounters() {
    return Iterables.concat(
        streamingCounters
            .pendingDeltaCounters()
            .extractModifiedDeltaUpdates(DataflowCounterUpdateExtractor.INSTANCE),
        streamingCounters
            .pendingCumulativeCounters()
            .extractUpdates(false, DataflowCounterUpdateExtractor.INSTANCE));
  }
}
