/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions.autotokenize.testing.stubs;


import com.google.api.core.ApiFuture;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public abstract class BaseUnaryApiFuture<T> implements ApiFuture<T>, Serializable {

  @Override
  public final void addListener(Runnable runnable, Executor executor) {
    executor.execute(runnable);
  }

  @Override
  public final boolean cancel(boolean b) {
    return false;
  }

  @Override
  public final boolean isCancelled() {
    return false;
  }

  @Override
  public final boolean isDone() {
    return true;
  }

  @Override
  @SuppressWarnings("NullableProblems")
  public final T get(long l, TimeUnit timeUnit) throws ExecutionException, InterruptedException {
    return get();
  }
}
