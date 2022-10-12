import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


class covid extends DoFn<String,String>{
    @ProcessElement
    public void processElement(ProcessContext c){
        String line=c.element();
        System.out.println(line);
        String arr[]=line.split(",");
        if(arr[1].equals("Maanaadu")){
            c.output(line);
            System.out.println(line);
        }
    }
}

public class parDo {
    public static void main(String[] args) {
        Pipeline pipe=Pipeline.create();
        PCollection<String> custList=pipe.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/mov.csv"));
        PCollection<String> pout=custList.apply(ParDo.of(new covid()));
        pout.apply("hariharan",TextIO.write().to("gs://my-project-25411.appspot.com/mybucket").withNumShards(1).withSuffix("csv"));
        pipe.run().waitUntilFinish();
    }
}
