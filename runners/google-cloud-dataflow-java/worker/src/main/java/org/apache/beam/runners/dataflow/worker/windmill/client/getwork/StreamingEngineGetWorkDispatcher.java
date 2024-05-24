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

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Instant;

/** Gets and schedules work from Streaming Engine. */
@Internal
@ThreadSafe
public final class StreamingEngineGetWorkDispatcher extends SingleGetWorkDispatcher {
  private static final int GET_WORK_STREAM_TIMEOUT_MINUTES = 3;

  private final Function<WorkItemReceiver, WindmillStream.GetWorkStream> getWorkStreamFactory;
  private final MemoryMonitor memoryMonitor;
  private final Function<String, Optional<ComputationState>> computationStateFetcher;
  private final Function<String, Work.ProcessingContext.WithProcessWorkFn> processingContextFactory;
  private final StreamingWorkScheduler streamingWorkScheduler;

  public StreamingEngineGetWorkDispatcher(
      Function<WorkItemReceiver, WindmillStream.GetWorkStream> getWorkStreamFactory,
      MemoryMonitor memoryMonitor,
      Function<String, Optional<ComputationState>> computationStateFetcher,
      Function<String, Work.ProcessingContext.WithProcessWorkFn> processingContextFactory,
      StreamingWorkScheduler streamingWorkScheduler) {
    this.getWorkStreamFactory = getWorkStreamFactory;
    this.memoryMonitor = memoryMonitor;
    this.computationStateFetcher = computationStateFetcher;
    this.processingContextFactory = processingContextFactory;
    this.streamingWorkScheduler = streamingWorkScheduler;
  }

  @Override
  protected void pollGetWork() {
    while (true) {
      WindmillStream.GetWorkStream stream =
          getWorkStreamFactory.apply(
              (String computationId,
                  Instant inputDataWatermark,
                  Instant synchronizedProcessingTime,
                  Windmill.WorkItem workItem,
                  Collection<Windmill.LatencyAttribution> getWorkStreamLatencies) ->
                  computationStateFetcher
                      .apply(computationId)
                      .ifPresent(
                          computationState -> {
                            memoryMonitor.waitForResources("GetWork");
                            streamingWorkScheduler.scheduleWork(
                                computationState,
                                workItem,
                                Work.createWatermarks()
                                    .setInputDataWatermark(inputDataWatermark)
                                    .setSynchronizedProcessingTime(synchronizedProcessingTime)
                                    .setOutputDataWatermark(workItem.getOutputDataWatermark())
                                    .build(),
                                processingContextFactory.apply(computationState.getComputationId()),
                                getWorkStreamLatencies);
                          }));
      try {
        // Reconnect every now and again to enable better load balancing.
        // If at any point the server closes the stream, we will reconnect immediately; otherwise
        // we half-close the stream after some time and create a new one.
        if (!stream.awaitTermination(GET_WORK_STREAM_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
          stream.close();
        }
      } catch (InterruptedException e) {
        // Continue processing until GetWorkDispatcher.stop() is called.
      }
    }
  }
}
