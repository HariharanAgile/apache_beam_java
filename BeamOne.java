import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class BeamOne {
    public static void main(String[] args) {
        Pipeline pipe = Pipeline.create();
       PCollection<String> out= pipe.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/dir.csv"));
       out.apply(TextIO.write().to("/Users/hariharanthirumurugan/Desktop/direct.csv").withNumShards(1).withSuffix(".csv"));
       pipe.run();
    }
}
