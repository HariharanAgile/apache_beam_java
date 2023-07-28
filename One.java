import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class One {
    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        pipelineOptions.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(pipelineOptions);
        pipeline.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/DWSample1-TXT.txt"))
                .apply(ParDo.of(new engineering()))
                .apply(Count.globally())
                .apply(ParDo.of(new DoFn<Long, Void>() {

                    @ProcessElement
                    public void processElement(ProcessContext c){
                        System.out.println(c.element());
                    }


                }));
        pipeline.run().waitUntilFinish();
    }
}


class engineering extends DoFn<String,String> {
    @ProcessElement
    public void processElement(ProcessContext c){
        for (String word : c.element().split(" ")) {
            if (!word.isEmpty()) {
                c.output(word);
            }
        }
        }
    }





