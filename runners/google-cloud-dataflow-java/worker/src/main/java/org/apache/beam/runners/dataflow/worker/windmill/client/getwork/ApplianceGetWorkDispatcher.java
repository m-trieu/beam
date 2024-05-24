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

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationWorkItems;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gets and schedules work from Streaming Appliance. */
@Internal
@ThreadSafe
public final class ApplianceGetWorkDispatcher extends SingleGetWorkDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(ApplianceGetWorkDispatcher.class);

  private final MemoryMonitor memoryMonitor;
  private final Function<String, Optional<ComputationState>> computationStateFetcher;
  private final Function<String, Work.ProcessingContext.WithProcessWorkFn> processingContextFactory;
  private final StreamingWorkScheduler streamingWorkScheduler;
  private final Supplier<Windmill.GetWorkResponse> workFetcher;

  public ApplianceGetWorkDispatcher(
      MemoryMonitor memoryMonitor,
      Function<String, Optional<ComputationState>> computationStateFetcher,
      Function<String, Work.ProcessingContext.WithProcessWorkFn> processingContextFactory,
      StreamingWorkScheduler streamingWorkScheduler,
      Supplier<Windmill.GetWorkResponse> workFetcher) {
    this.memoryMonitor = memoryMonitor;
    this.computationStateFetcher = computationStateFetcher;
    this.processingContextFactory = processingContextFactory;
    this.streamingWorkScheduler = streamingWorkScheduler;
    this.workFetcher = workFetcher;
  }

  @Override
  protected void pollGetWork() {
    while (true) {
      memoryMonitor.waitForResources("GetWork");
      Windmill.GetWorkResponse workResponse = fetchWork();
      for (ComputationWorkItems computationWork : workResponse.getWorkList()) {
        computationStateFetcher
            .apply(computationWork.getComputationId())
            .ifPresent(computationState -> scheduleWorkItems(computationState, computationWork));
      }
    }
  }

  private void scheduleWorkItems(
      ComputationState computationState, ComputationWorkItems computationWorkItems) {
    Instant inputDataWatermark =
        WindmillTimeUtils.windmillToHarnessWatermark(computationWorkItems.getInputDataWatermark());
    Work.Watermarks.Builder watermarks =
        Work.createWatermarks()
            .setInputDataWatermark(Preconditions.checkNotNull(inputDataWatermark))
            .setSynchronizedProcessingTime(
                WindmillTimeUtils.windmillToHarnessWatermark(
                    computationWorkItems.getDependentRealtimeInputWatermark()));

    for (Windmill.WorkItem workItem : computationWorkItems.getWorkList()) {
      streamingWorkScheduler.scheduleWork(
          computationState,
          workItem,
          watermarks.setOutputDataWatermark(workItem.getOutputDataWatermark()).build(),
          processingContextFactory.apply(computationState.getComputationId()),
          /* getWorkStreamLatencies= */ Collections.emptyList());
    }
  }

  private Windmill.GetWorkResponse fetchWork() {
    int backoff = 1;
    Windmill.GetWorkResponse workResponse;
    do {
      try {
        workResponse = workFetcher.get();
        if (workResponse.getWorkCount() > 0) {
          return workResponse;
        }
      } catch (WindmillServerStub.RpcException e) {
        LOG.warn("GetWork failed, retrying:", e);
      }
      Uninterruptibles.sleepUninterruptibly(backoff, TimeUnit.MILLISECONDS);
      backoff = Math.min(1000, backoff * 2);
    } while (true);
  }
}
