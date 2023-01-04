package org.apache.beam.runners.samza.portable;

import java.util.ArrayList;
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


public class UDFTimeoutTest {

  public static void main(String[] args) {
    buildPipeline(buildOptions()).run().waitUntilFinish();
  }

  private static TestPortablePipelineOptions buildOptions() {
    TestPortablePipelineOptions options = PipelineOptionsFactory.as(TestPortablePipelineOptions.class);
    options.setJobServerDriver((Class) SamzaJobServerDriver.class);
    options.setJobServerConfig("--job-host=localhost", "--job-port=0", "--artifact-port=0", "--expansion-port=0");
    options.setRunner(TestPortableRunner.class);
    options.setEnvironmentExpirationMillis(10000);
    options.setDefaultEnvironmentType("EMBEDDED");

    SamzaPipelineOptions samzaOptions = options.as(SamzaPipelineOptions.class);
    samzaOptions.setMaxBundleSize(2);
    Map<String, String> configs = new HashMap<>();
    configs.put("task.callback.timeout.ms", "5000");
    samzaOptions.setConfigOverride(configs);

    return options;
  }

  private static Pipeline buildPipeline(PipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(Create.of(buildInputData())).apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
      @ProcessElement
      public void process(ProcessContext context) {
        triggerTimeout();
        context.output(context.element());
        System.out.println("Handled the event: " + context.element());
      }
    }));

    return pipeline;
  }

  private static List<KV<String, Integer>> buildInputData() {
    final List<KV<String, Integer>> input = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      input.add(KV.of("" + i, i));
    }
    return input;
  }

  private static void triggerTimeout() {
    try {
      Thread.sleep(20000L);
    } catch (InterruptedException e) {
    }
  }
}
