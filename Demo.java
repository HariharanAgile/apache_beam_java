package com.BeamTextIO.TextIO;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

public class Demo {

    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        pipelineOptions.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        pipeline.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/DWSample1-TXT.txt")).apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split(" ")) {
                            if (!word.isEmpty()) {
                                c.output(word);
                                System.out.println("output : "+word);
                            }
                        }
                    }
                })).apply(Count.<String>perElement())
                .apply("FormatResult", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + ": " + input.getValue();
                    }
                })).apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/word_count.txt").withNumShards(1));
        pipeline.run().waitUntilFinish();





    }

}
