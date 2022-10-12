import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

class pub extends DoFn<String,String> {
    @ProcessElement
    public void processElement(ProcessContext c){
        String line = c.element();
        System.out.println(line);
    }
}

public class Test {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<String> abs = p.apply(TextIO.read().from("gs://my-project-25411.appspot.com/mybucket/lecture-50-challenge.txt"));
        PCollection<String> k = abs.apply(ParDo.of(new pub()));
        p.run().waitUntilFinish();
    }
}
