import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class TaskTwo extends DoFn<String,String> {
    @ProcessElement
    public void process(ProcessContext c){
        String line = c.element();
        System.out.println(line);
        c.output(line);

    }
        }

public class ExamTwo {
    public static void main(String[] args) {

      final  List i= new ArrayList<String>();
        i.add("hari");
        i.add("haran");
        i.add("balaji");

        final List<String> L = Arrays.asList("hari","haran","balaji");

        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");
        Pipeline l=Pipeline.create();
        PCollection<String> a =l.apply(Create.of(LINES)).apply(ParDo.of(new TaskTwo())).setCoder(StringUtf8Coder.of());
        a.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/outable.txt").withNumShards(1).withSuffix("txt"));
        l.run().waitUntilFinish();
    }
}
