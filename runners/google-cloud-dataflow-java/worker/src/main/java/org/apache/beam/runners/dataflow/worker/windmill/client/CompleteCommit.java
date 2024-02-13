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

package org.apache.beam.runners.dataflow.worker.windmill.client;

import com.google.auto.value.AutoValue;
import org.apache.beam.runners.dataflow.worker.streaming.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;

@AutoValue
public abstract class CompleteCommit {

  public static CompleteCommit create(Commit commit, CommitStatus commitStatus) {
    return new AutoValue_CompleteCommit(commit, commitStatus);
  }

  public abstract Commit commit();

  public abstract CommitStatus status();
}
