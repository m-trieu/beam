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

import org.apache.beam.sdk.annotations.Internal;

/**
 * Tracks time spent in a throttled state due to {@code Status.RESOURCE_EXHAUSTED} errors returned
 * from gRPC calls.
 */
@Internal
@FunctionalInterface
public interface ThrottledTimeTracker {

  /** Returns the combined total of all throttle times and resets those times to 0. */
  long getAndResetThrottleTime();
}
