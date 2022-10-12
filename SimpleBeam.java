import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class SimpleBeam {
    public static void main(String[] args) {
//        final List<String> LINES = Arrays.asList(
//                "To be, or not to be: that is the question: ",
//                "Whether 'tis nobler in the mind to suffer ",
//                "The slings and arrows of outrageous fortune, ",
//                "Or to take arms against a sea of troubles, ");
//
//        // Create the pipeline.
//        PipelineOptions options =
//                PipelineOptionsFactory.fromArgs(args).create();
//        Pipeline p = Pipeline.create(options);
//
//        // Apply Create, passing the list and the coder, to create the PCollection.
//        PCollection<String> the=p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
//        the.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/outsale.txt").withNumShards(1).withSuffix(".txt")); //PTransform
        PipelineOptions pipelineOptions= PipelineOptionsFactory.create();
        /*
        pipelineOptions.setJobName("LabSession1");
        pipelineOptions.setProject("my-project-25411");
        pipelineOptions.setRegion("Multi-region");
        pipelineOptions.setRunner(DataflowRunner.class);
        pipelineOptions.setGcpTempLocation("gs://my-project-25411.appspot.com/mybucket");

         */

        Pipeline pipeline=Pipeline.create(pipelineOptions);
        PCollection<String> output = pipeline.apply(TextIO.read().from("gs://my-project-25411.appspot.com/mybucket/writer.csv"));
        output.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/table_gcp.csv").withNumShards(1).withSuffix(".csv")); //PTransform

//        final List<String> abc = Arrays.asList("hari","haran","dataflow");
//        pipeline.apply(Create.of(abc)).apply(TextIO.write().to("gs://my-project-25411.appspot.com//mybucket//hari.txt").withSuffix(".txt").withNumShards(1));
       pipeline.run().waitUntilFinish();
    }
}
