import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

class FilterProcess implements SerializableFunction<String,Boolean>{
    public Boolean apply(String input){
    return input.contains("Master");
    }
}

public class Filt {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<String> pList=p.apply(TextIO.read().from("/Users/hariharanthirumurugan/Downloads/mov.csv"));
        PCollection<String> pOut=pList.apply(Filter.by(new FilterProcess()));
        pOut.apply(TextIO.write().to("/Users/hariharanthirumurugan/Downloads/Master.csv"));
        p.run().waitUntilFinish();
    }
}
