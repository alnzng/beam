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
package org.apache.beam.runners.samza.portable;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.portability.testing.TestPortablePipelineOptions;
import org.apache.beam.runners.portability.testing.TestPortableRunner;
import org.apache.beam.runners.samza.SamzaJobServerDriver;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "unused" // TODO(BEAM-13271): Remove when new version of errorprone is released (2.11.0)
})
public class UDFTimeoutTest {

  @Test
  public void testPortable() {
    buildPipeline(buildOptions()).run().waitUntilFinish();
  }

  private static TestPortablePipelineOptions buildOptions() {
    TestPortablePipelineOptions options =
        PipelineOptionsFactory.as(TestPortablePipelineOptions.class);
    options.setJobServerDriver((Class) SamzaJobServerDriver.class);
    options.setJobServerConfig(
        "--job-host=localhost", "--job-port=0", "--artifact-port=0", "--expansion-port=0");
    options.setRunner(TestPortableRunner.class);
    options.setEnvironmentExpirationMillis(0);
    options.setDefaultEnvironmentType("EMBEDDED");

    SamzaPipelineOptions samzaOptions = options.as(SamzaPipelineOptions.class);
    samzaOptions.setMaxBundleSize(1);
    Map<String, String> configs = new HashMap<>();
    configs.put("task.callback.timeout.ms", "5000");
    samzaOptions.setConfigOverride(configs);

    return options;
  }

  private static Pipeline buildPipeline(PipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Create.of(buildInputData()))
        .apply(
            ParDo.of(
                new DoFn<KV<String, Integer>, KV<String, Integer>>() {
                  @ProcessElement
                  public void process(ProcessContext context) {
                    System.out.println(
                        "----- Received an event: "
                            + context.element()
                            + ", current thread: "
                            + Thread.currentThread().getName()
                            + ", current time: "
                            + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
                    context.output(context.element());
                  }
                }))
        .apply(
            ParDo.of(
                new DoFn<KV<String, Integer>, KV<String, Integer>>() {
                  @ProcessElement
                  public void process(ProcessContext context) {
                    triggerTimeout();
                    context.output(context.element());
                    System.out.println(
                        "===== Handled an event: "
                            + context.element()
                            + ", current thread: "
                            + Thread.currentThread().getName()
                            + ", current time: "
                            + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
                  }
                }));

    return pipeline;
  }

  private static List<KV<String, Integer>> buildInputData() {
    final List<KV<String, Integer>> input = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      input.add(KV.of("" + i, i));
    }
    return input;
  }

  private static void triggerTimeout() {
    try {
      Thread.sleep(1000000L);
    } catch (InterruptedException e) {
    }
  }
}
