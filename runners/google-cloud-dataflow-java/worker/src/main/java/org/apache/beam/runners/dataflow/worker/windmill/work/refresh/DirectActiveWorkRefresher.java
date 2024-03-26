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
package org.apache.beam.runners.dataflow.worker.windmill.work.refresh;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.computations.ComputationState;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;

public final class DirectActiveWorkRefresher extends ActiveWorkRefresher {
  private final Consumer<Map<GetDataStream, Map<String, List<HeartbeatRequest>>>>
      refreshActiveWorkFn;

  DirectActiveWorkRefresher(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      DataflowExecutionStateSampler sampler,
      Consumer<Map<GetDataStream, Map<String, List<HeartbeatRequest>>>> refreshActiveWorkFn,
      ScheduledExecutorService scheduledExecutorService) {
    super(
        clock,
        activeWorkRefreshPeriodMillis,
        stuckCommitDurationMillis,
        computations,
        sampler,
        scheduledExecutorService);
    this.refreshActiveWorkFn = refreshActiveWorkFn;
  }

  public static ActiveWorkRefresher create(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      DataflowExecutionStateSampler sampler,
      Consumer<Map<GetDataStream, Map<String, List<HeartbeatRequest>>>> refreshActiveWorkFn) {
    return new DirectActiveWorkRefresher(
        clock,
        activeWorkRefreshPeriodMillis,
        stuckCommitDurationMillis,
        computations,
        sampler,
        refreshActiveWorkFn,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("RefreshWork").build()));
  }

  @VisibleForTesting
  public static DirectActiveWorkRefresher forTesting(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      DataflowExecutionStateSampler sampler,
      Consumer<Map<GetDataStream, Map<String, List<HeartbeatRequest>>>> refreshActiveWorkFn,
      ScheduledExecutorService scheduledExecutorService) {
    return new DirectActiveWorkRefresher(
        clock,
        activeWorkRefreshPeriodMillis,
        stuckCommitDurationMillis,
        computations,
        sampler,
        refreshActiveWorkFn,
        scheduledExecutorService);
  }

  @Override
  public void refreshActiveWork() {
    Instant refreshDeadline = clock.get().minus(Duration.millis(activeWorkRefreshPeriodMillis));

    Map<GetDataStream, Map<String, List<HeartbeatRequest>>> heartbeatRequests = new HashMap<>();
    for (ComputationState computationState : computations.get()) {
      String computationId = computationState.getComputationId();
      ImmutableList<DirectHeartbeatRequest> heartbeats =
          computationState.getDirectKeyHeartbeats(refreshDeadline, sampler);
      for (DirectHeartbeatRequest heartbeat : heartbeats) {
        Map<String, List<HeartbeatRequest>> existingHeartbeats =
            heartbeatRequests.computeIfAbsent(heartbeat.stream(), ignored -> new HashMap<>());
        List<HeartbeatRequest> existingHeartbeatsForComputation =
            existingHeartbeats.computeIfAbsent(computationId, ignored -> new ArrayList<>());
        existingHeartbeatsForComputation.add(heartbeat.heartbeatRequest());
      }
    }

    refreshActiveWorkFn.accept(heartbeatRequests);
  }
}
