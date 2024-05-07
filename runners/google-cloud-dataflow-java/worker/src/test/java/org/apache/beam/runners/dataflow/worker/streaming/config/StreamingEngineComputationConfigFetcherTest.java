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
package org.apache.beam.runners.dataflow.worker.streaming.config;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.StreamingComputationConfig;
import com.google.api.services.dataflow.model.StreamingConfigTask;
import com.google.api.services.dataflow.model.WorkItem;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingEngineComputationConfigFetcherTest {
  private final WorkUnitClient mockDataflowServiceClient = mock(WorkUnitClient.class);
  private StreamingEngineComputationConfigFetcher streamingEngineConfigFetcher;

  private StreamingEngineComputationConfigFetcher createConfigLoader(
      boolean waitForInitialConfig,
      long globalConfigRefreshPeriod,
      Consumer<StreamingEnginePipelineConfig> onPipelineConfig) {
    return StreamingEngineComputationConfigFetcher.forTesting(
        !waitForInitialConfig,
        globalConfigRefreshPeriod,
        mockDataflowServiceClient,
        ignored -> Executors.newSingleThreadScheduledExecutor(),
        onPipelineConfig);
  }

  @After
  public void cleanUp() {
    streamingEngineConfigFetcher.stop();
  }

  @Test
  public void testStart_requiresInitialConfig() throws IOException, InterruptedException {
    WorkItem initialConfig =
        new WorkItem()
            .setJobId("job")
            .setStreamingConfigTask(new StreamingConfigTask().setMaxWorkItemCommitBytes(10L));
    CountDownLatch waitForInitialConfig = new CountDownLatch(1);
    Set<StreamingEnginePipelineConfig> receivedPipelineConfig = new HashSet<>();
    when(mockDataflowServiceClient.getGlobalStreamingConfigWorkItem())
        .thenReturn(Optional.of(initialConfig));
    streamingEngineConfigFetcher =
        createConfigLoader(
            /* waitForInitialConfig= */ true,
            0,
            config -> {
              try {
                receivedPipelineConfig.add(config);
                waitForInitialConfig.await();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });
    Thread asyncStartConfigLoader = new Thread(streamingEngineConfigFetcher::start);
    asyncStartConfigLoader.start();
    waitForInitialConfig.countDown();
    asyncStartConfigLoader.join();
    assertThat(receivedPipelineConfig)
        .containsExactly(
            StreamingEnginePipelineConfig.builder()
                .setMaxWorkItemCommitBytes(
                    initialConfig.getStreamingConfigTask().getMaxWorkItemCommitBytes())
                .build());
  }

  @Test
  public void testStart_startsPeriodicConfigRequests() throws IOException, InterruptedException {
    WorkItem firstConfig =
        new WorkItem()
            .setJobId("job")
            .setStreamingConfigTask(new StreamingConfigTask().setMaxWorkItemCommitBytes(10L));
    WorkItem secondConfig =
        new WorkItem()
            .setJobId("job")
            .setStreamingConfigTask(new StreamingConfigTask().setMaxWorkItemCommitBytes(15L));
    WorkItem thirdConfig =
        new WorkItem()
            .setJobId("job")
            .setStreamingConfigTask(new StreamingConfigTask().setMaxWorkItemCommitBytes(100L));
    CountDownLatch numExpectedRefreshes = new CountDownLatch(3);
    Set<StreamingEnginePipelineConfig> receivedPipelineConfig = new HashSet<>();
    when(mockDataflowServiceClient.getGlobalStreamingConfigWorkItem())
        .thenReturn(Optional.of(firstConfig))
        .thenReturn(Optional.of(secondConfig))
        .thenReturn(Optional.of(thirdConfig));

    streamingEngineConfigFetcher =
        createConfigLoader(
            /* waitForInitialConfig= */ true,
            Duration.millis(100).getMillis(),
            config -> {
              receivedPipelineConfig.add(config);
              numExpectedRefreshes.countDown();
            });

    Thread asyncStartConfigLoader = new Thread(streamingEngineConfigFetcher::start);
    asyncStartConfigLoader.start();
    numExpectedRefreshes.await();
    asyncStartConfigLoader.join();
    assertThat(receivedPipelineConfig)
        .containsExactly(
            StreamingEnginePipelineConfig.builder()
                .setMaxWorkItemCommitBytes(
                    firstConfig.getStreamingConfigTask().getMaxWorkItemCommitBytes())
                .build(),
            StreamingEnginePipelineConfig.builder()
                .setMaxWorkItemCommitBytes(
                    secondConfig.getStreamingConfigTask().getMaxWorkItemCommitBytes())
                .build(),
            StreamingEnginePipelineConfig.builder()
                .setMaxWorkItemCommitBytes(
                    thirdConfig.getStreamingConfigTask().getMaxWorkItemCommitBytes())
                .build());
  }

  @Test
  public void testGetComputationConfig() throws IOException {
    streamingEngineConfigFetcher =
        createConfigLoader(/* waitForInitialConfig= */ false, 0, ignored -> {});
    String computationId = "computationId";
    String stageName = "stageName";
    String systemName = "systemName";
    StreamingComputationConfig pipelineConfig =
        new StreamingComputationConfig()
            .setComputationId(computationId)
            .setStageName(stageName)
            .setSystemName(systemName)
            .setInstructions(ImmutableList.of());

    WorkItem workItem =
        new WorkItem()
            .setStreamingConfigTask(
                new StreamingConfigTask()
                    .setStreamingComputationConfigs(ImmutableList.of(pipelineConfig)));

    when(mockDataflowServiceClient.getStreamingConfigWorkItem(anyString()))
        .thenReturn(Optional.of(workItem));
    Optional<ComputationConfig> actualPipelineConfig =
        streamingEngineConfigFetcher.fetchConfig(computationId);

    assertTrue(actualPipelineConfig.isPresent());
    assertThat(actualPipelineConfig.get())
        .isEqualTo(
            ComputationConfig.create(
                StreamingEngineComputationConfigFetcher.createMapTask(pipelineConfig),
                ImmutableMap.of(),
                ImmutableMap.of()));
  }

  @Test
  public void testGetComputationConfig_noComputationPresent() throws IOException {
    Set<StreamingEnginePipelineConfig> receivedPipelineConfig = new HashSet<>();
    streamingEngineConfigFetcher =
        createConfigLoader(/* waitForInitialConfig= */ false, 0, receivedPipelineConfig::add);
    when(mockDataflowServiceClient.getStreamingConfigWorkItem(anyString()))
        .thenReturn(Optional.empty());
    Optional<ComputationConfig> pipelineConfig =
        streamingEngineConfigFetcher.fetchConfig("someComputationId");
    assertFalse(pipelineConfig.isPresent());
    assertThat(receivedPipelineConfig).isEmpty();
  }
}
