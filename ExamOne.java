import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PDone;

class TaskOne extends DoFn<String,String>{
        @ProcessElement
        public void processElement(ProcessContext c){
                String line = c.element();
                if(line!=null){
                        String[] arr= line.split(",");
                        System.out.println(arr[0].toUpperCase());
                        String abc = arr[0].toUpperCase();
                        c.output(abc);
                }
        }
        }

public class ExamOne {
        public static void main(String[] args) {
                Pipeline p = Pipeline.create();
                PDone obj = p.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/temp_new.csv")).apply(ParDo.of(new TaskOne())).
                        apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/ion.csv").withSuffix(".csv").withNumShards(1));
                p.run().waitUntilFinish();
        }
}