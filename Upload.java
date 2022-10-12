import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class Upload {
    public static void main(String[] args) {
        FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
        Pipeline piper = Pipeline.create();
        PCollection<String> ats = piper.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/all_matches.csv"));
        ats.apply(TextIO.write().to("gs://my-project-25411.appspot.com/mybucket/hari").withSuffix(".csv").withNumShards(1));
        piper.run().waitUntilFinish();
    }
}
